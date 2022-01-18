/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq;

import org.apache.flink.api.java.tuple.Tuple2;

import com.bytedance.mqproxy.proto.Message;
import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.mqproxy.proto.MessageQueuePb;
import com.bytedance.mqproxy.proto.OffsetResultPerQueue;
import com.bytedance.mqproxy.proto.QueryOffsetResponse;
import com.bytedance.mqproxy.proto.QueryTopicQueuesResponse;
import com.bytedance.mqproxy.proto.ResetOffsetResponse;
import com.bytedance.mqproxy.proto.ResponseCode;
import com.bytedance.mqproxy.proto.ResponseHeader;
import com.bytedance.mqproxy.proto.SeekConsumerOffsetReq;
import com.bytedance.mqproxy.proto.SeekConsumerOffsetResp;
import com.bytedance.mqproxy.proto.SeekResultPerQueue;
import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;
import com.bytedance.rocketmq.clientv2.consumer.QueryOffsetResult;
import com.bytedance.rocketmq.clientv2.consumer.QueryTopicQueuesResult;
import com.bytedance.rocketmq.clientv2.consumer.ResetOffsetResult;
import com.bytedance.rocketmq.clientv2.consumer.SeekOffsetResult;
import com.bytedance.schema.registry.common.util.JSON;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_EARLIEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_LATEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_TIMESTAMP;
import static org.apache.flink.connector.rocketmq.TestSqlGenerator.SqlGeneratorConfig;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * MockConsumerReturns.
 */
public class MockConsumer {
	private final AssignAnswer assignAnswer;
	private final DefaultMQPullConsumer consumer;
	private final MockConsumerConfig consumerConfig;
	private final List<List<MessageQueuePb>> messageQueuePbLists;
	private final PollAnswer pollAnswer;

	public MockConsumer(MockConsumerConfig consumerConfig, List<List<MessageQueuePb>> messageQueuePbLists) {
		consumerConfig.validate();
		this.consumerConfig = consumerConfig;
		this.consumer = mock(DefaultMQPullConsumer.class);
		this.messageQueuePbLists = messageQueuePbLists;
		this.pollAnswer = new PollAnswer(consumerConfig, messageQueuePbLists);
		this.assignAnswer = new AssignAnswer(pollAnswer);
	}

	public MockConsumer mockQueryCommittedOffset() throws Exception {
		when(consumer.queryCommitOffset(anyString(), any())).then(
			new Answer<QueryOffsetResult>() {
				@Override
				public QueryOffsetResult answer(InvocationOnMock invocationOnMock) throws Throwable {
					Assert.assertEquals(consumerConfig.getSqlGeneratorConfig().getStartMode(),
						RocketMQOptions.SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS);

					List<MessageQueuePb> queuePbList = invocationOnMock.getArgument(1);
					String topic = invocationOnMock.getArgument(0);
					Assert.assertEquals(queuePbList.size(), 1);
					Assert.assertEquals(topic, consumerConfig.sqlGeneratorConfig.getTopic());

					QueryOffsetResponse queryOffsetResponse = QueryOffsetResponse.newBuilder().addResetResult(
						newOffsetResultPerQueue(queuePbList.get(0), consumerConfig.getStartOffset())).build();
					return new QueryOffsetResult(queryOffsetResponse);
				}
			}
		);
		return this;
	}

	public MockConsumer mockResetToEarliest() throws Exception {
		when(consumer.resetOffsetToEarliest(anyString(), anyString(), anyList(), anyBoolean()))
			.thenAnswer(new ResetOffsetAnswer(consumerConfig));
		return this;
	}

	public MockConsumer mockResetToLatest() throws Exception {
		when(consumer.resetOffsetToLatest(anyString(), anyString(), anyList(), anyBoolean()))
			.thenAnswer(new ResetOffsetAnswer(consumerConfig));
		return this;
	}

	public MockConsumer mockResetToTimestamp() throws Exception {
		when(consumer.resetOffsetByTimestamp(anyString(), anyString(), anyList(), anyLong(), anyBoolean()))
			.thenAnswer(new ResetOffsetAnswer(consumerConfig));
		return this;
	}

	public MockConsumer mockSeekOffsetReturns() throws Exception {
		when(consumer.seekConsumerOffset(anyString(), anyString(), any(), anyLong(), anyList(), anyBoolean()))
			.then(new Answer<SeekOffsetResult>() {
				@Override
				public SeekOffsetResult answer(InvocationOnMock invocationOnMock) throws Throwable {
					List<MessageQueuePb> queuePbList = invocationOnMock.getArgument(4);
					consumerConfig.validateSeekFrom(invocationOnMock);

					List<SeekResultPerQueue> seekResultPerQueues = queuePbList.stream()
						.map(queuePb -> SeekResultPerQueue.newBuilder()
							.setOffset(consumerConfig.getStartOffset()).setMessageQueue(queuePb).build())
						.collect(Collectors.toList());

					SeekConsumerOffsetResp seekConsumerOffsetResp =
						SeekConsumerOffsetResp.newBuilder().addAllSeekResult(seekResultPerQueues).build();
					return new SeekOffsetResult(seekConsumerOffsetResp);
				}
			});
		return this;
	}

	public void mockSplitReader() throws Exception {
		mockSeekOffsetReturns();
		mockQueryCommittedOffset();
		Mockito.doAnswer(assignAnswer).when(consumer).assign(any());
		when(consumer.poll(anyLong())).then(pollAnswer);
		mockResetOffsetToSpecified();
		if (SCAN_STARTUP_MODE_VALUE_EARLIEST.equals(consumerConfig.getStartMode())) {
			mockResetToEarliest();
		} else if (SCAN_STARTUP_MODE_VALUE_LATEST.equals(consumerConfig.getStartMode())) {
			mockResetToLatest();
		} else if (SCAN_STARTUP_MODE_VALUE_TIMESTAMP.equals(consumerConfig.getStartMode())) {
			mockResetToTimestamp();
		}
	}

	public void mockEnumerator() throws Exception {
		mockQueryTopicQueuesReturns();
	}

	public MockConsumer mockQueryTopicQueuesReturns() throws Exception {
		OngoingStubbing<QueryTopicQueuesResult> ongoingStubbing = when(consumer.queryTopicQueues(anyString()));
		for (List<MessageQueuePb> messageQueuePbList: messageQueuePbLists) {
			ongoingStubbing = ongoingStubbing.thenReturn(buildSuccessQueryTopicQueuesResult(messageQueuePbList));
		}
		return this;
	}

	public MockConsumer mockResetOffsetToSpecified() throws Exception {
		when(consumer.resetOffsetToSpecified(anyString(), anyString(), anyList(), anyLong(), anyBoolean())).thenAnswer(
			new Answer<ResetOffsetResult>() {
				@Override
				public ResetOffsetResult answer(InvocationOnMock invocationOnMock) throws Throwable {
					List<MessageQueuePb> queues = invocationOnMock.getArgument(2);
					long offset = invocationOnMock.getArgument(3);
					assert queues.size() == 1;
					MessageQueuePb queuePb = queues.get(0);
					ResetOffsetResponse resetOffsetResponse = ResetOffsetResponse.newBuilder()
						.setResponseHeader(okResponseHeader())
						.addResetResult(newOffsetResultPerQueue(queuePb, offset)).build();
					pollAnswer.setOffset(queuePb, offset);
					return new ResetOffsetResult(resetOffsetResponse);
				}
			}
		);
		return this;
	}

	protected QueryTopicQueuesResult buildSuccessQueryTopicQueuesResult(List<MessageQueuePb> messageQueuePbList) {
		return new QueryTopicQueuesResult(successQueryTopicQueuesResponse(messageQueuePbList));
	}

	protected QueryTopicQueuesResponse successQueryTopicQueuesResponse(
		List<MessageQueuePb> messageQueuePbList) {
		return QueryTopicQueuesResponse.newBuilder()
			.setRespHeader(okResponseHeader()).addAllMqs(messageQueuePbList).build();
	}

	private static OffsetResultPerQueue.Builder newOffsetResultPerQueue(MessageQueuePb queuePb, long offset) {
		return OffsetResultPerQueue.newBuilder().setOffset(offset).setMq(queuePb);
	}

	private static ResponseHeader.Builder okResponseHeader() {
		return ResponseHeader.newBuilder().setErrCode(ResponseCode.OK_VALUE)
			.setErrMsg("");
	}

	public DefaultMQPullConsumer getConsumer() {
		return consumer;
	}

	public static MessageQueuePb createMessageQueuePb(String topic, String broker, int queueId) {
		return MessageQueuePb.newBuilder().setTopic(topic)
			.setBrokerName(broker).setQueueId(queueId).build();
	}

	public List<List<Object>> getMockTestDataList(long maxOffset) {
		return pollAnswer.generatorList.stream()
			.filter(t2 -> t2.f1 < maxOffset).map(t2 -> t2.f0).collect(Collectors.toList());
	}

	public void addAdditionalOffset(int additionalOffset) {
		pollAnswer.addAdditionalOffset(additionalOffset);
	}

	/**
	 * MockConsumerConfig.
	 */
	public static class MockConsumerConfig {
		private final int assignAddStep;
		private final int pollLatency;
		private final int maxPollSize;
		private final long startOffset;
		private final long initMaxOffset;
		private final SqlGeneratorConfig sqlGeneratorConfig;
		private final boolean enableCheckpoint;
		private final CountDownLatch countDownLatch;

		public MockConsumerConfig(
				int assignAddStep,
				int pollLatency,
				int maxPollSize,
				long startOffset,
				long initMaxOffset,
				SqlGeneratorConfig sqlGeneratorConfig,
				boolean enableCheckpoint,
				CountDownLatch countDownLatch) {
			this.assignAddStep = assignAddStep;
			this.pollLatency = pollLatency;
			this.maxPollSize = maxPollSize;
			this.startOffset = startOffset;
			this.initMaxOffset = initMaxOffset;
			this.sqlGeneratorConfig = sqlGeneratorConfig;
			this.enableCheckpoint = enableCheckpoint;
			this.countDownLatch = countDownLatch;
		}

		public void validate() {
			Assert.assertTrue(startOffset <= initMaxOffset);
		}

		public int getAssignAddStep() {
			return assignAddStep;
		}

		public int getPollLatency() {
			return pollLatency;
		}

		public int getMaxPollSize() {
			return maxPollSize;
		}

		public long getInitMaxOffset() {
			return initMaxOffset;
		}

		public long getEndOffset(int assigned) {
			return initMaxOffset + (assigned - 1) * (long) assignAddStep;
		}

		public long getStartOffset() {
			return startOffset;
		}

		public boolean isEnableCheckpoint() {
			return enableCheckpoint;
		}

		public String getCluster() {
			return getSqlGeneratorConfig().getCluster();
		}

		public String getTopic() {
			return getSqlGeneratorConfig().getTopic();
		}

		public String getConsumerGroup() {
			return getSqlGeneratorConfig().getConsumerGroup();
		}

		public SqlGeneratorConfig getSqlGeneratorConfig() {
			return sqlGeneratorConfig;
		}

		public CountDownLatch getCountDownLatch() {
			return countDownLatch;
		}

		public String getStartMode() {
			return sqlGeneratorConfig.getStartMode();
		}

		public void validateSeekFrom(InvocationOnMock invocationOnMock) {
			String topic = invocationOnMock.getArgument(0);
			String consumerGroup = invocationOnMock.getArgument(1);
			SeekConsumerOffsetReq.SeekFromWhere seekFromWhere = invocationOnMock.getArgument(2);
			long timestamp = invocationOnMock.getArgument(3);
			List<MessageQueuePb> messageQueuePbs = invocationOnMock.getArgument(4);
			boolean dryRun = invocationOnMock.getArgument(5);

			Assert.assertFalse(dryRun);
			Assert.assertEquals(messageQueuePbs.size(), 1);
			Assert.assertEquals(topic, sqlGeneratorConfig.getTopic());
			Assert.assertEquals(consumerGroup, sqlGeneratorConfig.getConsumerGroup());
			if (!enableCheckpoint) {
				switch (sqlGeneratorConfig.getStartMode()) {
					case SCAN_STARTUP_MODE_VALUE_EARLIEST:
						Assert.assertEquals(seekFromWhere, SeekConsumerOffsetReq.SeekFromWhere.EARLIEST);
						break;
					case RocketMQOptions.SCAN_STARTUP_MODE_VALUE_LATEST:
						Assert.assertEquals(seekFromWhere, SeekConsumerOffsetReq.SeekFromWhere.LATEST);
						break;
					case RocketMQOptions.SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
						Assert.assertEquals(seekFromWhere, SeekConsumerOffsetReq.SeekFromWhere.TIMESTAMP);
						Assert.assertEquals(timestamp, sqlGeneratorConfig.getStartTimestamp());
						break;
				}
			}
		}
	}

	private static class PollAnswer implements Answer<List<MessageExt>> {
		private static final Logger LOG = LoggerFactory.getLogger(PollAnswer.class);
		private static final String UID = "uid";
		private static final String INT_FIELD = "int_field";
		private static final String BIGINT_FIELD = "bigint_field";
		private static final String VARCHAR_FIELD = "varchar_field";

		private final MockConsumerConfig consumerConfig;
		private final CountDownLatch countDownLatch;
		private int assignTimes = 0;
		private final List<List<MessageQueuePb>> messageQueuePbLists;
		private final List<Tuple2<List<Object>, Long>> generatorList = new ArrayList<>();
		private final Map<MessageQueuePb, Long> queue2OffsetMap = new HashMap<>();
		private final Random random = new Random();
		private final AtomicInteger additionalOffset = new AtomicInteger(0);

		public PollAnswer(
				MockConsumerConfig consumerConfig,
				List<List<MessageQueuePb>> messageQueuePbLists) {
			this.consumerConfig = consumerConfig;
			this.messageQueuePbLists = messageQueuePbLists;
			this.countDownLatch = consumerConfig.getCountDownLatch();
		}

		public void addAssignPbListIfNotExist(List<MessageQueuePb> queuePbList) {
			List<MessageQueuePb> messageQueuePbList = messageQueuePbLists.get(assignTimes++);
			LOG.trace("Assign times {}, queue size {}", assignTimes, queuePbList.size());
			Assert.assertEquals(new HashSet<>(messageQueuePbList), new HashSet<>(queuePbList));
			for (MessageQueuePb queuePb: queuePbList) {
				if (!queue2OffsetMap.containsKey(queuePb)) {
					queue2OffsetMap.put(queuePb, consumerConfig.getStartOffset());
				}
			}
		}

		@Override
		public List<MessageExt> answer(InvocationOnMock invocationOnMock) throws Throwable {
			if (consumerConfig.getPollLatency() > 0) {
				Thread.sleep(consumerConfig.getPollLatency());
			}

			List<MessageExt> messageExtList = new ArrayList<>();
			int maxPollSize = consumerConfig.getMaxPollSize();
			long maxOffset = consumerConfig.getEndOffset(assignTimes) + additionalOffset.get();
			for (MessageQueuePb queuePb: queue2OffsetMap.keySet()) {
				Long offset = queue2OffsetMap.get(queuePb);
				if (offset > maxOffset) {
					continue;
				}
				for (long i = offset; i <= maxOffset && messageExtList.size() < maxPollSize; i++) {
					Map<String, Object> generatorMap = createDefaultTestData();
					String uid = String.format("[group %s, topic: %s, broker %s, queue %s, offset %s] %s",
						consumerConfig.getSqlGeneratorConfig().getConsumerGroup(),
						queuePb.getTopic(),
						queuePb.getBrokerName(), queuePb.getQueueId(), i, UUID.randomUUID());
					generatorMap.put(UID, uid);
					generatorList.add(new Tuple2<>(Arrays.asList(uid,
						generatorMap.get(VARCHAR_FIELD), generatorMap.get(INT_FIELD), generatorMap.get(BIGINT_FIELD)), i));
					messageExtList.add(MessageExt.newBuilder()
						.setMessageQueue(queuePb)
						.setQueueOffset(i)
						.setMsg(
							Message.newBuilder().setBody(ByteString.copyFromUtf8(
								JSON.toJSONString(generatorMap))))
						.setBodyCRC(0)
						.setBornHostBytes(ByteString.copyFromUtf8(""))
						.setBornTimestamp(1).build());
					queue2OffsetMap.put(queuePb, i + 1);
				}
				if (messageExtList.size() >= maxPollSize) {
					break;
				}
			}
			LOG.trace("[cluster: {}, topic: {}, group: {}] poll {} messages", consumerConfig.getCluster(),
				consumerConfig.getTopic(), consumerConfig.getConsumerGroup(), messageExtList.size());
			if (countDownLatch != null) {
				messageExtList.forEach(m -> countDownLatch.countDown());
			}
			return messageExtList;
		}

		public void addAdditionalOffset(int addedOffset) {
			additionalOffset.addAndGet(addedOffset);
		}

		public void setOffset(MessageQueuePb queuePb, long offset) {
			LOG.trace("PollAnswer reset [topic: {}, broker: {}, queue: {}] offset to {}",
				queuePb.getTopic(), queuePb.getBrokerName(), queuePb.getQueueId(), offset);
			queue2OffsetMap.put(queuePb, offset);
		}

		private Map<String, Object> createDefaultTestData() {
			Map<String, Object> map = new HashMap<>();
			map.put(INT_FIELD, random.nextInt());
			map.put(BIGINT_FIELD, random.nextLong());
			map.put(VARCHAR_FIELD, UUID.randomUUID().toString());
			return map;
		}
	}

	private static class AssignAnswer implements Answer<Void> {
		private final PollAnswer pollAnswer;

		public AssignAnswer(PollAnswer pollAnswer) {
			this.pollAnswer = pollAnswer;
		}

		@Override
		public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
			List<MessageQueuePb> queuePbList = invocationOnMock.getArgument(0);
			pollAnswer.addAssignPbListIfNotExist(queuePbList);
			return null;
		}
	}

	private static class ResetOffsetAnswer implements Answer<ResetOffsetResult> {
		private final MockConsumerConfig consumerConfig;

		public ResetOffsetAnswer(MockConsumerConfig consumerConfig) {
			this.consumerConfig = consumerConfig;
		}

		@Override
		public ResetOffsetResult answer(InvocationOnMock invocationOnMock) throws Throwable {
			List<MessageQueuePb> queuePbList = invocationOnMock.getArgument(2);
			ResetOffsetResponse resetOffsetResponse = ResetOffsetResponse.newBuilder()
				.setResponseHeader(okResponseHeader())
				.addResetResult(newOffsetResultPerQueue(queuePbList.get(0), consumerConfig.getStartOffset())).build();

			return new ResetOffsetResult(resetOffsetResponse);
		}
	}
}
