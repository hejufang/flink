/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.bmq;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.bmq.config.Metadata;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.EngineMetadataRequest;
import org.apache.kafka.common.requests.EngineMetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.connector.bmq.BmqOptions.DEFAULT_GROUP_ID;
import static org.apache.flink.connector.bmq.BmqOptions.DEFAULT_OWNER;
import static org.apache.flink.connector.bmq.BmqOptions.DEFAULT_PSM;
import static org.apache.flink.connector.bmq.BmqOptions.DEFAULT_TEAM;

/**
 * BmqFileInputFormat.
 */
public class BmqFileInputFormat extends RichInputFormat<RowData, BmqFileInputSplit> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(BmqFileInputFormat.class);

	private static final String PARTITION_METADATA_BATCH_TYPE = "BATCH";
	private static final int PARQUET_READ_BLOCK_SIZE = 2048;
	private static final int UNSPECIFIED_VALUE = -1;

	private final String cluster;
	private final String topic;
	private final long scanStartMs;
	private final long scanEndMs;
	private final String[] fullFieldNames;
	private final DataType[] fullFieldTypes;
	private final int[] selectedFields;
	private final Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap;
	private final boolean utcTimestamp;
	private final SerializableConfiguration conf;
	private final long limit;

	private long scanStartOffset;
	private long scanEndOffset;
	private boolean hasOffsetField;
	private final boolean producedOffsetField;
	private int offsetFieldParquetIdx;

	private transient ParquetColumnarRowSplitReader reader;
	private transient long currentReadCount;

	private BmqFileInputFormat(
			String cluster,
			String topic,
			long scanStartMs,
			long scanEndMs,
			String[] fullFieldNames,
			DataType[] fullFieldTypes,
			int[] selectedFields,
			Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap,
			long limit,
			org.apache.hadoop.conf.Configuration conf,
			boolean utcTimestamp) {
		this.cluster = Preconditions.checkNotNull(cluster, "cluster cannot be null");
		this.topic = Preconditions.checkNotNull(topic, "topic cannot be null)");
		this.scanStartMs = scanStartMs;
		this.scanEndMs = scanEndMs;
		this.limit = limit;
		this.fullFieldNames = fullFieldNames;
		this.fullFieldTypes = fullFieldTypes;
		this.selectedFields = selectedFields;
		this.metadataMap = metadataMap;
		this.conf = new SerializableConfiguration(conf);
		this.utcTimestamp = utcTimestamp;
		this.hasOffsetField = false;
		this.offsetFieldParquetIdx = UNSPECIFIED_VALUE;

		int offsetIndexInFullField = UNSPECIFIED_VALUE;
		if (this.metadataMap != null) {
			for (Map.Entry<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> entry :
				this.metadataMap.entrySet()) {
				Metadata metadata = (Metadata) entry.getValue();
				if (metadata == Metadata.OFFSET) {
					this.hasOffsetField = true;
					offsetIndexInFullField = entry.getKey();
					break;
				}
			}
		}

		this.producedOffsetField = ArrayUtils.contains(selectedFields, offsetIndexInFullField);
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
	public BmqFileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}

		try (KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer()) {
			// 1. fetch partition infos
			List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream()
				.map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
				.collect(Collectors.toList());

			// 2. request start/end/latest offset
			Map<TopicPartition, Long> startTimestampToSearch = topicPartitions.stream()
				.collect(Collectors.toMap(Function.identity(), noUse -> scanStartMs));
			Map<TopicPartition, OffsetAndTimestamp> startOffsetMap = consumer.offsetsForTimes(startTimestampToSearch);

			Map<TopicPartition, Long> endTimestampToSearch = topicPartitions.stream()
				.collect(Collectors.toMap(Function.identity(), noUse -> scanEndMs));
			Map<TopicPartition, OffsetAndTimestamp> endOffsetMap = consumer.offsetsForTimes(endTimestampToSearch);

			Map<TopicPartition, Long> latestOffsets = consumer.endOffsets(topicPartitions);

			LOG.debug("Topic partition startOffsetMap: {}, endOffsetMap: {}", startOffsetMap, endOffsetMap);
			// 3. construct QueryScope
			Map<TopicPartition, EngineMetadataRequest.QueryScope> lookupMap = new HashMap<>();
			for (TopicPartition topicPartition : topicPartitions) {
				if (startOffsetMap.get(topicPartition) == null) {
					throw new IOException(String.format("Cannot get start offset for %s, scan start ms is %d. " +
						"Maybe something wrong happened in BMQ cluster.", topicPartition, scanStartMs));
				}
				long startOffset = startOffsetMap.get(topicPartition).offset();

				long endOffset;
				// maybe there is no records newer than end time
				if (endOffsetMap.get(topicPartition) == null) {
					if (latestOffsets.get(topicPartition) == null) {
						throw new IOException(String.format("Cannot get latest offset for %s. " +
							"Maybe something wrong happened in BMQ cluster.", topicPartition));
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
				// If both offset range & timestamp range specified, BMQ only take offset range as the filter.
				// Use -1 for unspecified value for startTimestamp/endTimestamp
				// Assign max_block_num to INT_MAX to avoid pagination.
				lookupMap.put(topicPartition, new EngineMetadataRequest.QueryScope(
					startOffset,
					endOffset,
					UNSPECIFIED_VALUE,
					UNSPECIFIED_VALUE,
					Integer.MAX_VALUE));
			}

			// 4. fetch Block detailed and construct split
			List<BmqFileInputSplit> splits = new ArrayList<>();
			Map<TopicPartition, EngineMetadataResponse.PartitionMetadata> metadata =
				consumer.lookupEngineMetadata(lookupMap);

			LOG.debug("Lookup engine metadata result: {}", metadata);
			for (Map.Entry<TopicPartition, EngineMetadataResponse.PartitionMetadata> entry :
					metadata.entrySet()) {
				TopicPartition topicPartition = entry.getKey();
				EngineMetadataResponse.PartitionMetadata partitionMetadata = entry.getValue();
				if (partitionMetadata.errorCode == Errors.NONE.code()) {
					EngineMetadataRequest.QueryScope queryScope = lookupMap.get(topicPartition);
					int splitNum = 0;
					for (EngineMetadataResponse.Block block: partitionMetadata.blocks) {
						if (block.type.equals(PARTITION_METADATA_BATCH_TYPE)) {
							Path path = new Path(block.filePath);
							FileSystem fs = path.getFileSystem();
							FileStatus pathFile = fs.getFileStatus(path);
							final BlockLocation[] blocks = fs.getFileBlockLocations(pathFile, 0, pathFile.getLen());
							Set<String> hosts = new HashSet<String>();
							for (BlockLocation hdfsBlock : blocks) {
								hosts.addAll(Arrays.asList(hdfsBlock.getHosts()));
							}

							splits.add(BmqFileInputSplit.builder()
								.setSplitNumber(splitNum++)
								.setPath(path)
								.setStart(0)
								.setLength(pathFile.getLen())
								.setHosts(hosts.toArray(new String[0]))
								.setStartOffset(block.startOffset)
								.setExpectedStartOffset(queryScope.startOffset)
								.setExpectedEndOffset(queryScope.endOffset)
								.build());
						}
					}
				} else {
					throw new FlinkRuntimeException(String.format("the error code of partition %s is %s, " +
							"Maybe something wrong happended in BMQ cluster.",
						partitionMetadata, Errors.forCode(partitionMetadata.errorCode)));
				}
			}

			return splits.toArray(new BmqFileInputSplit[0]);
		}
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(BmqFileInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(BmqFileInputSplit fileSplit) throws IOException {
		LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();

		String[] queryFieldNames;
		DataType[] queryFieldTypes;
		int[] querySelectedFields;

		// 1. extend parquet query field if metadata offset is not set.
		if (this.hasOffsetField) {
			queryFieldNames = new String[fullFieldNames.length];
			queryFieldTypes = new DataType[fullFieldTypes.length];
		} else {
			queryFieldNames = new String[fullFieldNames.length + 1];
			queryFieldTypes = new DataType[fullFieldTypes.length + 1];
			queryFieldNames[fullFieldNames.length] = Metadata.OFFSET.getMetadata();
			queryFieldTypes[fullFieldTypes.length] = DataTypes.BIGINT();
		}

		System.arraycopy(fullFieldNames, 0, queryFieldNames, 0, fullFieldNames.length);
		System.arraycopy(fullFieldTypes, 0, queryFieldTypes, 0, fullFieldTypes.length);

		// 2. replace metadata field name
		if (metadataMap != null) {
			assert selectedFields.length <= fullFieldNames.length;
			for (Map.Entry<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> entry:
					this.metadataMap.entrySet()) {
				int idx = entry.getKey();
				Metadata metadata = (Metadata) entry.getValue();
				queryFieldNames[idx] = metadata.getMetadata();
			}
		}

		// 3a. check __offset index in full query fields
		int offsetIndexInFullQueryFields = UNSPECIFIED_VALUE;
		for (int i = 0; i < queryFieldNames.length; ++i) {
			if (queryFieldNames[i].equals(Metadata.OFFSET.getMetadata())) {
				offsetIndexInFullQueryFields = i;
				break;
			}
		}

		// 3b. check and extend selected field
		if (producedOffsetField) {
			querySelectedFields = new int[selectedFields.length];
		} else {
			querySelectedFields = new int[selectedFields.length + 1];
			querySelectedFields[selectedFields.length] = offsetIndexInFullQueryFields;
		}

		System.arraycopy(selectedFields, 0, querySelectedFields, 0, selectedFields.length);

		// 3c. find offset parquet index
		for (int i = 0; i < querySelectedFields.length; ++i) {
			if (querySelectedFields[i] == offsetIndexInFullQueryFields) {
				offsetFieldParquetIdx = i;
				break;
			}
		}

		this.scanStartOffset = fileSplit.getExpectedStartOffset();
		this.scanEndOffset = fileSplit.getExpectedEndOffset();

		this.reader = ParquetSplitReaderUtil.genPartColumnarRowReader(
			utcTimestamp,
			true,
			conf.conf(),
			queryFieldNames,
			queryFieldTypes,
			partObjects,
			querySelectedFields,
			PARQUET_READ_BLOCK_SIZE,
			new Path(fileSplit.getPath().toString()),
			fileSplit.getStart(),
			fileSplit.getLength());
		this.currentReadCount = 0L;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (limit != UNSPECIFIED_VALUE && currentReadCount >= limit) {
			return true;
		} else {
			return reader.reachedEnd();
		}
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		RowData record;
		long offset;
		do {
			record = this.reader.nextRecord();
			offset = record.getLong(offsetFieldParquetIdx);
			LOG.debug("parquet offset: {}", offset);
		} while (offset < scanStartOffset && !this.reader.reachedEnd());

		if (offset < scanStartOffset) {
			LOG.warn("Offset should greater or equal to {}, but {}", scanStartOffset, offset);
			return null;
		} else if (offset >= scanEndOffset) {
			return null;
		}
		currentReadCount++;

		if (producedOffsetField) {
			return record;
		} else {
			return new ColumnarRowDataWrapper(record);
		}
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			this.reader.close();
		}
		this.reader = null;
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
		props.put(CommonClientConfigs.ENABLE_ZTI_TOKEN, "true");

		return new KafkaConsumer<>(props);
	}

	public static BmqFileInputFormat.Builder builder() {
		return new BmqFileInputFormat.Builder();
	}

	@Override
	public String toString() {
		return "BmqFileInputFormat{" +
			"cluster='" + cluster + '\'' +
			", topic='" + topic + '\'' +
			", scanStartMs=" + scanStartMs +
			", scanEndMs=" + scanEndMs +
			", fullFieldNames=" + Arrays.toString(fullFieldNames) +
			", fullFieldTypes=" + Arrays.toString(fullFieldTypes) +
			", selectedFields=" + Arrays.toString(selectedFields) +
			", metadataMap=" + metadataMap +
			", utcTimestamp=" + utcTimestamp +
			", conf=" + conf +
			", limit=" + limit +
			", scanStartOffset=" + scanStartOffset +
			", scanEndOffset=" + scanEndOffset +
			", offsetFieldParquetIdx=" + offsetFieldParquetIdx +
			", reader=" + reader +
			", currentReadCount=" + currentReadCount +
			'}';
	}

	/**
	 * Builder for {@link org.apache.flink.connector.bmq.BmqFileInputFormat}.
	 */
	public static class Builder {
		private String cluster;
		private String topic;
		private long startMs;
		private long endMs;
		private String[] fullFieldNames;
		private DataType[] fullFieldTypes;
		private int[] selectedFields;
		private Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap;
		private long limit;
		private org.apache.hadoop.conf.Configuration conf;
		private boolean utcTimestamp;

		private Builder() {
		}

		public BmqFileInputFormat.Builder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public BmqFileInputFormat.Builder setTopic(String topic) {
			this.topic = topic;
			return this;
		}

		public BmqFileInputFormat.Builder setStartMs(long startMs) {
			this.startMs = startMs;
			return this;
		}

		public BmqFileInputFormat.Builder setEndMs(long endMs) {
			this.endMs = endMs;
			return this;
		}

		public BmqFileInputFormat.Builder setFullFieldNames(String[] fullFieldNames) {
			this.fullFieldNames = fullFieldNames;
			return this;
		}

		public BmqFileInputFormat.Builder setFullFieldTypes(DataType[] fullFieldTypes) {
			this.fullFieldTypes = fullFieldTypes;
			return this;
		}

		public BmqFileInputFormat.Builder setSelectedFields(int[] selectedFields) {
			this.selectedFields = selectedFields;
			return this;
		}

		public BmqFileInputFormat.Builder setMetadataMap(Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap) {
			this.metadataMap = metadataMap;
			return this;
		}

		public BmqFileInputFormat.Builder setLimit(long limit) {
			this.limit = limit;
			return this;
		}

		public BmqFileInputFormat.Builder setConf(org.apache.hadoop.conf.Configuration conf) {
			this.conf = conf;
			return this;
		}

		public BmqFileInputFormat.Builder setUtcTimeStamp(boolean utcTimestamp) {
			this.utcTimestamp = utcTimestamp;
			return this;
		}

		public BmqFileInputFormat build() {
			return new BmqFileInputFormat(
				cluster,
				topic,
				startMs,
				endMs,
				fullFieldNames,
				fullFieldTypes,
				selectedFields,
				metadataMap,
				limit,
				conf,
				utcTimestamp);
		}
	}

	/**
	 * Wrapper class for filtering extra __offset field.
	 */
	public static class ColumnarRowDataWrapper implements RowData {
		private RowData row;

		public ColumnarRowDataWrapper(RowData row) {
			this.row = row;
		}

		@Override
		public int getArity() {
			return row.getArity() - 1;
		}

		@Override
		public RowKind getRowKind() {
			return row.getRowKind();
		}

		@Override
		public void setRowKind(RowKind kind) {
			row.setRowKind(kind);
		}

		@Override
		public boolean isNullAt(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.isNullAt(pos);
		}

		@Override
		public boolean getBoolean(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getBoolean(pos);
		}

		@Override
		public byte getByte(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getByte(pos);
		}

		@Override
		public short getShort(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getShort(pos);
		}

		@Override
		public int getInt(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getInt(pos);
		}

		@Override
		public long getLong(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getLong(pos);
		}

		@Override
		public float getFloat(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getFloat(pos);
		}

		@Override
		public double getDouble(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getDouble(pos);
		}

		@Override
		public StringData getString(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getString(pos);
		}

		@Override
		public DecimalData getDecimal(int pos, int precision, int scale) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getDecimal(pos, precision, scale);
		}

		@Override
		public TimestampData getTimestamp(int pos, int precision) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getTimestamp(pos, precision);
		}

		@Override
		public <T> RawValueData<T> getRawValue(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getRawValue(pos);
		}

		@Override
		public byte[] getBinary(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getBinary(pos);
		}

		@Override
		public ArrayData getArray(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getArray(pos);
		}

		@Override
		public MapData getMap(int pos) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getMap(pos);
		}

		@Override
		public RowData getRow(int pos, int numFields) {
			Preconditions.checkArgument(pos >= 0 && pos < row.getArity() - 1);
			return row.getRow(pos, numFields);
		}
	}
}
