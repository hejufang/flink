/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.bmq;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.bytedance.bmq.client.SmartConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.SegmentMetadataRequest;
import org.apache.kafka.common.requests.SegmentMetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.connector.bmq.BmqOptions.DEFAULT_GROUP_ID;
import static org.apache.flink.connector.bmq.BmqOptions.DEFAULT_OWNER;
import static org.apache.flink.connector.bmq.BmqOptions.DEFAULT_PSM;
import static org.apache.flink.connector.bmq.BmqOptions.DEFAULT_TEAM;

/**
 * BMQ InputFormat for batch reading.
 */
public class BmqInputFormat extends RichInputFormat<RowData, BmqInputSplit> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(BmqInputFormat.class);

	private final String cluster;
	private final String topic;
	private final long startMs;
	private final long endMs;
	private final DeserializationSchema<RowData> deserializationSchema;
	private final boolean ignoreUnhealthySegment;

	private transient SmartConsumer smartConsumer;
	private transient List<ConsumerRecord<byte[], byte[]>> unConsumedData;
	private transient int consumerIndex;

	private BmqInputFormat(
			String cluster,
			String topic,
			long startMs,
			long endMs,
			DeserializationSchema<RowData> deserializationSchema,
			boolean ignoreUnhealthySegment) {
		Preconditions.checkArgument(endMs > startMs,
			String.format("startMs should < endMs, but the given startMs is %d, and the given endMs is %d",
				startMs, endMs));
		this.cluster = Preconditions.checkNotNull(cluster, "cluster cannot be null");
		this.topic = Preconditions.checkNotNull(topic, "topic cannot be null)");
		this.startMs = startMs;
		this.endMs = endMs;
		this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema,
			"deserialization schema cannot be null");
		this.ignoreUnhealthySegment = ignoreUnhealthySegment;
	}

	@Override
	public void configure(Configuration parameters) {
		// nothing, by default.
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		// no statistics available, by default.
		return null;
	}

	@Override
	public BmqInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		try (KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer()) {
			// 1. fetch partition infos
			List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream()
				.map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
				.collect(Collectors.toList());

			// 2. request start/end/latest offsets
			Map<TopicPartition, Long> startTimestampToSearch = topicPartitions.stream()
				.collect(Collectors.toMap(Function.identity(), noUse -> startMs));
			Map<TopicPartition, OffsetAndTimestamp> startOffsetMap = consumer.offsetsForTimes(startTimestampToSearch);

			Map<TopicPartition, Long> endTimestampToSearch = topicPartitions.stream()
				.collect(Collectors.toMap(Function.identity(), noUse -> endMs));
			Map<TopicPartition, OffsetAndTimestamp> endOffsetMap = consumer.offsetsForTimes(endTimestampToSearch);

			Map<TopicPartition, Long> latestOffsets = consumer.endOffsets(topicPartitions);

			// 3. validate and construct segment scope
			Map<TopicPartition, SegmentMetadataRequest.SegmentScope> lookupMap = new HashMap<>();
			for (TopicPartition topicPartition : topicPartitions) {
				if (startOffsetMap.get(topicPartition) == null) {
					throw new IOException(String.format("Cannot get start offset for %s, startTime=%d. " +
							"Maybe something wrong happened in BMQ cluster.", topicPartition, startMs));
				}
				long startOffset = startOffsetMap.get(topicPartition).offset();

				long endOffset;
				// maybe there is no records newer than end time
				if (endOffsetMap.get(topicPartition) == null) {
					if (latestOffsets.get(topicPartition) == null) {
						throw new IOException(String.format("Cannot get latest offset for %s. " +
							"Maybe something wrong happended in BMQ cluster.", topicPartition));
					}
					endOffset = latestOffsets.get(topicPartition);
				} else {
					endOffset = endOffsetMap.get(topicPartition).offset();
				}

				if (startOffset > endOffset) {
					throw new IOException(String.format("start offset should not be bigger than end offset, " +
							"but for %s the startOffset=%d, endOffset=%d.",
						topicPartition, startOffset, endOffset));
				}
				lookupMap.put(topicPartition, new SegmentMetadataRequest.SegmentScope(startOffset, endOffset));
			}

			// 4. fetch segment detail infos and construct splits
			List<BmqInputSplit> splits = new ArrayList<>();
			Map<TopicPartition, SegmentMetadataResponse.PartitionMetadata> segmentMetadata =
				consumer.lookupSegmentMetadata(lookupMap);
			for (Map.Entry<TopicPartition, SegmentMetadataResponse.PartitionMetadata> entry :
					segmentMetadata.entrySet()) {
				TopicPartition topicPartition = entry.getKey();
				SegmentMetadataResponse.PartitionMetadata partitionMetadata = entry.getValue();
				if (partitionMetadata.errorCode == Errors.NONE.code()) {
					SegmentMetadataRequest.SegmentScope segmentScope = lookupMap.get(topicPartition);
					int idx = 0;
					for (SegmentMetadataResponse.Segment segment : partitionMetadata.segments) {
						// This may happen when the file on HDFS is missing or corrupted
						String errMessage = String.format("The health of segment %s is not 'HEALTHY', but is '%s'," +
								" ignoring this segment. The corresponding TopicPartition is %s.",
							segment, segment.health, topicPartition);
						if (!segment.health.equalsIgnoreCase("HEALTHY")) {
							if (ignoreUnhealthySegment) {
								LOG.error(errMessage);
								continue;
							} else {
								throw new FlinkRuntimeException(errMessage);
							}
						}
						splits.add(BmqInputSplit.builder()
							.setSplitNumber(splits.size())
							.setIndexInSameTopicPartition(idx++)
							.setTopicPartition(topicPartition)
							.setExpectedStartOffset(segmentScope.startOffset)
							.setExpectedEndOffset(segmentScope.endOffset)
							.setHealth(segment.health)
							.setFilePath(segment.filePath)
							.setMessageFormat(segment.messageFormat)
							.setCodec(segment.codec)
							.setStartOffset(segment.startOffset)
							.setMessageBytes(segment.messageBytes)
							.build());
					}
				} else {
					throw new FlinkRuntimeException(String.format("the error code of partition %s is %s, " +
							"Maybe something wrong happended in BMQ cluster.",
						partitionMetadata, Errors.forCode(partitionMetadata.errorCode)));
				}
			}

			// sort to distribute the load evenly among partitions.
			splits.sort((o1, o2) -> o2.getIndexInSameTopicPartition() - o1.getIndexInSameTopicPartition());

			String splitInfo = splits.stream().map(Object::toString).collect(Collectors.joining(", "));
			LOG.info("Creating BMQ splits: {}.", splitInfo);

			return splits.toArray(new BmqInputSplit[0]);
		}
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(BmqInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(BmqInputSplit split) throws IOException {
		smartConsumer = new SmartConsumer();
		LOG.info("Opening SmartConsumer with {}.", split);

		// BMQ will read records in [expectedStartOffset, expectedEndOffset)
		// startOffset means the file physical offset
		smartConsumer.open(
			split.getTopicPartition(),
			split.getExpectedStartOffset(),
			split.getExpectedEndOffset(),
			split.getHealth(),
			split.getFilePath(),
			split.getMessageFormat(),
			split.getCodec(),
			split.getStartOffset(),
			split.getMessageBytes());

		try {
			deserializationSchema.open(() -> getRuntimeContext().getMetricGroup().addGroup("user"));
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (unConsumedData != null && unConsumedData.size() > consumerIndex) {
			return false;
		}
		return !smartConsumer.hasNext();
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		try {
			while (unConsumedData == null || unConsumedData.size() == consumerIndex) {
				// BMQ only has the semantic that:
				// hasNext() means nextRecords() will return a list, however, the list may be empty.
				if (!smartConsumer.hasNext()) {
					return null;
				}
				unConsumedData = smartConsumer.nextRecords();
				consumerIndex = 0;
			}
		} catch (Throwable t) {
			LOG.error("Consuming from BMQ error.", t);
		}

		byte[] data = unConsumedData.get(consumerIndex++).value();
		return deserializationSchema.deserialize(data);
	}

	@Override
	public void close() throws IOException {
		if (smartConsumer != null) {
			smartConsumer.close();
		}
	}

	private KafkaConsumer<byte[], byte[]> createKafkaConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.CLUSTER_NAME_CONFIG, cluster);
		props.put(ConsumerConfig.TOPIC_NAME_CONFIG, topic);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_ID);
		props.put(ConsumerConfig.PSM_CONFIG, DEFAULT_PSM);
		props.put(ConsumerConfig.OWNER_CONFIG, DEFAULT_OWNER);
		props.put(ConsumerConfig.TEAM_CONFIG, DEFAULT_TEAM);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
		props.put(CommonClientConfigs.ENABLE_ZTI_TOKEN, "true");
		return new KafkaConsumer<>(props);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link org.apache.flink.connector.bmq.BmqInputFormat}.
	 */
	public static class Builder {
		private String cluster;
		private String topic;
		private long startMs;
		private long endMs;
		private DeserializationSchema<RowData> deserializationSchema;
		private boolean ignoreUnhealthySegment;

		private Builder() {
			// private constructor
		}

		public Builder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public Builder setTopic(String topic) {
			this.topic = topic;
			return this;
		}

		public Builder setStartMs(long startMs) {
			this.startMs = startMs;
			return this;
		}

		public Builder setEndMs(long endMs) {
			this.endMs = endMs;
			return this;
		}

		public Builder setDeserializationSchema(DeserializationSchema<RowData> deserializationSchema) {
			this.deserializationSchema = deserializationSchema;
			return this;
		}

		public Builder setIgnoreUnhealthySegment(boolean ignoreUnhealthySegment) {
			this.ignoreUnhealthySegment = ignoreUnhealthySegment;
			return this;
		}

		public BmqInputFormat build() {
			return new BmqInputFormat(cluster, topic, startMs, endMs, deserializationSchema, ignoreUnhealthySegment);
		}
	}
}
