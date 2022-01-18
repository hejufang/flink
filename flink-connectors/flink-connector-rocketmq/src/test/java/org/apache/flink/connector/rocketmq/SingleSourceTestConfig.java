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

import org.apache.flink.rocketmq.source.enumerator.EnumSplitIdManager;
import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;

import com.bytedance.mqproxy.proto.MessageQueuePb;
import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.rocketmq.MockConsumer.MockConsumerConfig;
import static org.apache.flink.connector.rocketmq.MockConsumer.createMessageQueuePb;
import static org.apache.flink.connector.rocketmq.RocketMQTestSplit.RmqSplitType.ENUMERATOR;
import static org.apache.flink.connector.rocketmq.RocketMQTestSplit.RmqSplitType.SPLIT_READER;
import static org.apache.flink.connector.rocketmq.RocketMQTestUtils.TEST_CLUSTER_A;
import static org.apache.flink.connector.rocketmq.RocketMQTestUtils.TEST_CONSUMER_GROUP_A;
import static org.apache.flink.connector.rocketmq.RocketMQTestUtils.TEST_TOPIC_NAME_A;
import static org.apache.flink.connector.rocketmq.TestSqlGenerator.SqlGeneratorConfig;

/**
 * SingleSourceTestConfig.
 */
public class SingleSourceTestConfig {
	private final List<List<RocketMQSplitBase>> splitBaseLists;
	private final MockConsumerConfig mockConsumerConfig;
	private final EnumSplitIdManager splitIdManager;
	private final List<MockConsumer> consumers = new ArrayList<>();
	private final int resultRowNum;

	public SingleSourceTestConfig(
			List<List<RocketMQSplitBase>> splitBaseLists,
			MockConsumerConfig mockConsumerConfig,
			int resultRowNum) {
		this.splitBaseLists = splitBaseLists;
		this.mockConsumerConfig = mockConsumerConfig;
		this.splitIdManager = new EnumSplitIdManager();
		this.resultRowNum = resultRowNum;
	}

	public void init() throws Exception {
		this.consumers.addAll(mockAllSplitsConsumer());
		mockAndRegisterEnumeratorConsumer();
	}

	public List<MockConsumer> mockAllSplitsConsumer() {
		// taskId -> xth
		List<List<List<RocketMQSplitBase>>> readerAssignSplitLists = new ArrayList<>();
		IntStream.range(0, getParallelism()).forEach(x -> readerAssignSplitLists.add(new ArrayList<>()));
		for (int i = 0; i < splitBaseLists.size(); i++) {
			List<RocketMQSplitBase> rocketMQSplitBases = splitBaseLists.get(i);
			List<List<RocketMQSplitBase>> splitBaseList =
				groupingTaskSplitBase(rocketMQSplitBases, getParallelism(), splitIdManager);
			for (int j = 0; j < getParallelism(); j++) {
				readerAssignSplitLists.get(j).add(splitBaseList.get(j));
			}
		}

		List<MockConsumer> consumers = new ArrayList<>();
		for (int i = 0; i < readerAssignSplitLists.size(); i++) {
			try {
				consumers.add(mockAndRegisterSplitConsumer(readerAssignSplitLists.get(i), i));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return consumers;
	}

	public List<List<Object>> getAllMockedData() {
		List<List<Object>> results = new ArrayList<>();
		consumers.forEach(c -> results.addAll(c.getMockTestDataList(getSqlGeneratorConfig().getFinalEndOffset())));
		return results;
	}

	public static List<List<RocketMQSplitBase>> groupingTaskSplitBase(
			List<RocketMQSplitBase> splitBaseList,
			int totalTaskCnt,
			EnumSplitIdManager enumSplitIdManager) {
		enumSplitIdManager.addAll(splitBaseList);
		List<List<RocketMQSplitBase>> splitLists = new ArrayList<>();
		IntStream.range(0, totalTaskCnt).forEach(i -> splitLists.add(new ArrayList<>()));
		for (RocketMQSplitBase splitBase: splitBaseList) {
			int taskId = enumSplitIdManager.getSplitId(splitBase) % totalTaskCnt;
			splitLists.get(taskId).add(splitBase);
		}
		return splitLists;
	}

	private RocketMQTestSplit createASplit(RocketMQTestSplit.RmqSplitType type, int taskId) {
		SqlGeneratorConfig sqlGenConf = getSqlGeneratorConfig();
		return new RocketMQTestSplit(sqlGenConf.getCluster(),
			sqlGenConf.getTopic(), sqlGenConf.getConsumerGroup(), taskId, type);
	}

	private void registerConsumer(RocketMQTestSplit.RmqSplitType type, int taskId, DefaultMQPullConsumer consumer) {
		RocketMQTestSplit testSplit = createASplit(type, taskId);
		TestMQConsumerStaticFactory.registerConsumer(testSplit, consumer);
	}

	private List<MessageQueuePb> convertSplits2Queues(List<RocketMQSplitBase> rocketMQSplitBases) {
		return rocketMQSplitBases.stream()
			.map(split -> createMessageQueuePb(split.getTopic(), split.getBrokerName(), split.getQueueId()))
			.collect(Collectors.toList());
	}

	private MockConsumer mockAndRegisterSplitConsumer(
			List<List<RocketMQSplitBase>> rocketMQSplitBases,
			int taskId) throws Exception {
		MockConsumer mockConsumerReturns = new MockConsumer(mockConsumerConfig,
			rocketMQSplitBases.stream().map(this::convertSplits2Queues).collect(Collectors.toList()));
		DefaultMQPullConsumer consumer = mockConsumerReturns.getConsumer();
		mockConsumerReturns.mockSplitReader();
		registerConsumer(SPLIT_READER, taskId, consumer);
		return mockConsumerReturns;
	}

	protected MockConsumer mockAndRegisterEnumeratorConsumer() throws Exception {
		MockConsumer mockConsumerReturns = new MockConsumer(mockConsumerConfig,
			splitBaseLists.stream().map(this::convertSplits2Queues).collect(Collectors.toList()));
		mockConsumerReturns.mockEnumerator();
		registerConsumer(ENUMERATOR, -1, mockConsumerReturns.getConsumer());
		return mockConsumerReturns;
	}

	public List<List<RocketMQSplitBase>> getSplitBaseLists() {
		return splitBaseLists;
	}

	public MockConsumer.MockConsumerConfig getMockConsumerConfig() {
		return mockConsumerConfig;
	}

	public int getResultRowNum() {
		return resultRowNum;
	}

	public SqlGeneratorConfig getSqlGeneratorConfig() {
		return mockConsumerConfig.getSqlGeneratorConfig();
	}

	public int getParallelism() {
		return mockConsumerConfig.getSqlGeneratorConfig().getParallelism();
	}

	public void addAdditionalOffset(int additionalOffset) {
		consumers.forEach(c -> c.addAdditionalOffset(additionalOffset));
	}

	/**
	 * Builder.
	 */
	public static class Builder {
		private static final int DEFAULT_STEP = 0;
		private static final int DEFAULT_POLL_LATENCY = 0;
		private static final int DEFAULT_POLL_SIZE = 2;
		private static final int DEFAULT_START_OFFSET = 0;
		private static final int DEFAULT_INIT_OFFSET = 10;
		private static final int DEFAULT_FINAL_OFFSET = 10;

		// Mock consumer config.
		private int assignAddStep = DEFAULT_STEP;
		private int pollLatency = DEFAULT_POLL_LATENCY;
		private int maxPollSize = DEFAULT_POLL_SIZE;
		private long startOffset = DEFAULT_START_OFFSET;
		private long initMaxOffset = DEFAULT_INIT_OFFSET;

		// Sql generator config.
		private String topic = TEST_TOPIC_NAME_A;
		private String cluster = TEST_CLUSTER_A;
		private String startMode = RocketMQOptions.SCAN_STARTUP_MODE_VALUE_EARLIEST;
		private String consumerGroup = TEST_CONSUMER_GROUP_A;
		private long finalEndOffset = DEFAULT_FINAL_OFFSET;
		private long discoverMs = 10;
		private long startTimestamp = 0;
		private String tableName;
		private int parallelism = 1;
		private int expectResultRowNum;
		private CountDownLatch countDownLatch;

		// Topic test config
		private boolean enableCheckpoint = false;
		private List<List<RocketMQSplitBase>> splitBaseLists = new ArrayList<>();

		public static Builder newBuilder() {
			return new Builder();
		}

		public Builder setAssignAddStep(int assignAddStep) {
			this.assignAddStep = assignAddStep;
			return this;
		}

		public Builder setPollLatency(int pollLatency) {
			this.pollLatency = pollLatency;
			return this;
		}

		public Builder setMaxPollSize(int maxPollSize) {
			this.maxPollSize = maxPollSize;
			return this;
		}

		public Builder setStartOffset(long startOffset) {
			this.startOffset = startOffset;
			return this;
		}

		public Builder setInitMaxOffset(long initMaxOffset) {
			this.initMaxOffset = initMaxOffset;
			return this;
		}

		public Builder setTopic(String topic) {
			this.topic = topic;
			return this;
		}

		public Builder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public Builder setStartMode(String startMode) {
			this.startMode = startMode;
			return this;
		}

		public Builder setConsumerGroup(String consumerGroup) {
			this.consumerGroup = consumerGroup;
			return this;
		}

		public Builder setExcludedEndOffset(long finalEndOffset) {
			this.finalEndOffset = finalEndOffset;
			return this;
		}

		public Builder setDiscoverMs(long discoverMs) {
			this.discoverMs = discoverMs;
			return this;
		}

		public Builder setStartTimestamp(long startTimestamp) {
			this.startTimestamp = startTimestamp;
			return this;
		}

		public Builder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder setEnableCheckpoint(boolean enableCheckpoint) {
			this.enableCheckpoint = enableCheckpoint;
			return this;
		}

		public Builder setCountDownLatch(CountDownLatch countDownLatch) {
			this.countDownLatch = countDownLatch;
			return this;
		}

		public Builder addNextSplitBaseLists(List<RocketMQSplitBase> splitBaseLists) {
			this.splitBaseLists.add(splitBaseLists);
			return this;
		}

		public Builder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public Builder setExpectResultRowNum(int expectResultRowNum) {
			this.expectResultRowNum = expectResultRowNum;
			return this;
		}

		public SingleSourceTestConfig build() {
			Assert.assertTrue(finalEndOffset > 0);
			Assert.assertTrue(parallelism > 0);
			Assert.assertNotNull(topic);
			Assert.assertTrue(expectResultRowNum > 0);
			if (splitBaseLists.size() == 0) {
				splitBaseLists = Collections.singletonList(RocketMQTestUtils.createSplitListSize4(topic));
			}

			if (tableName == null) {
				tableName = String.format("source_%s_%s_%s", cluster, topic, consumerGroup);
			}

			SqlGeneratorConfig sqlGeneratorConfig = new SqlGeneratorConfig(cluster, topic, consumerGroup,
				startMode, finalEndOffset, discoverMs, startTimestamp, tableName, parallelism);
			MockConsumerConfig consumerConfig = new MockConsumerConfig(assignAddStep, pollLatency, maxPollSize,
				startOffset, initMaxOffset, sqlGeneratorConfig, enableCheckpoint, countDownLatch);
			return new SingleSourceTestConfig(splitBaseLists, consumerConfig, expectResultRowNum);
		}
	}
}
