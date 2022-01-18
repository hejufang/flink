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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rocketmq.RocketMQTestUtils.TEST_BROKER_LIST_SIZE_2;
import static org.apache.flink.connector.rocketmq.RocketMQTestUtils.TEST_TOPIC_NAME_A;
import static org.apache.flink.connector.rocketmq.TestSqlGenerator.SqlGeneratorConfig;

/**
 * RocketMQTestBase.
 */
public class RocketMQFlip27ITTest {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void init() {
		System.setProperty("log4j.configurationFile", "log4j-rmq-junit.properties");
	}

	@After
	public void clear() {
		TestMQConsumerStaticFactory.clear();
	}

	@Test
	public void testStartFromEarliest() throws Exception {
		SingleSourceTestConfig singleSourceTestConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setStartMode(RocketMQOptions.SCAN_STARTUP_MODE_VALUE_EARLIEST)
			.setExpectResultRowNum(40)
			.build();
		commonTest(singleSourceTestConfig);
	}

	@Test
	public void testStartFromLatest() throws Exception {
		SingleSourceTestConfig singleSourceTestConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setStartMode(RocketMQOptions.SCAN_STARTUP_MODE_VALUE_LATEST)
			.setExpectResultRowNum(40)
			.build();
		commonTest(singleSourceTestConfig);
	}

	@Test
	public void testStartFromGroup() throws Exception {
		long offset = 10;
		SingleSourceTestConfig singleSourceTestConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setStartMode(RocketMQOptions.SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS)
			.setStartOffset(1)
			.setExpectResultRowNum(36)
			.setInitMaxOffset(offset)
			.setExcludedEndOffset(offset)
			.build();
		commonTest(singleSourceTestConfig);
	}

	@Test
	public void testStartFromTimestamp() throws Exception {
		SingleSourceTestConfig singleSourceTestConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setStartMode(RocketMQOptions.SCAN_STARTUP_MODE_VALUE_TIMESTAMP)
			.setExpectResultRowNum(40)
			.setStartTimestamp(1)
			.build();
		commonTest(singleSourceTestConfig);
	}

	@Test
	public void testMultiParallelism() throws Exception {
		SingleSourceTestConfig singleSourceTestConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setParallelism(2)
			.setExpectResultRowNum(40)
			.build();
		commonTest(singleSourceTestConfig);
	}

	@Test
	public void testServerReBalance() throws Exception {
		// First time: every broker has 2 queue id
		List<RocketMQSplitBase> firstSplitList =
			buildSplitList(TEST_TOPIC_NAME_A, TEST_BROKER_LIST_SIZE_2, 2);
		// Second time: every broker has 3 queue id
		List<RocketMQSplitBase> secondSplitList =
			buildSplitList(TEST_TOPIC_NAME_A, TEST_BROKER_LIST_SIZE_2, 3);
		SingleSourceTestConfig singleSourceTestConfig = SingleSourceTestConfig.Builder.newBuilder()
				.addNextSplitBaseLists(firstSplitList)
				.addNextSplitBaseLists(secondSplitList)
				.setParallelism(2)
				.setPollLatency(10)
				.setInitMaxOffset(10)
				.setExcludedEndOffset(20)
				.setAssignAddStep(10)
				.setExpectResultRowNum(120)
				.build();
		commonTest(singleSourceTestConfig);
	}

	@Test
	public void testConsumerMultiTopic() throws Exception {
		SingleSourceTestConfig topicAConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic("TopicA")
			.setExpectResultRowNum(40)
			.build();

		SingleSourceTestConfig topicBConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic("TopicB")
			.setExpectResultRowNum(40)
			.build();

		commonTest(1, Arrays.asList(topicAConfig, topicBConfig));
	}

	@Test
	public void queueNotReBalance() throws Exception {
		SingleSourceTestConfig topicAConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic("TopicA")
			.setParallelism(2)
			.setExpectResultRowNum(40)
			.build();
		commonTest(3, Arrays.asList(topicAConfig));
	}

	@Test
	public void testConsumerMultiTopicWithMultiParallelism() throws Exception {
		int jobParallelism = 3;
		SingleSourceTestConfig topicAConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic("TopicA")
			.setParallelism(2)
			.setExpectResultRowNum(40)
			.build();

		SingleSourceTestConfig topicBConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic("TopicB")
			.setParallelism(3)
			.setExpectResultRowNum(40)
			.build();

		commonTest(jobParallelism, Arrays.asList(topicAConfig, topicBConfig));
	}

	@Test
	public void testWithCheckpoint() throws Exception {
		// firstTime config will get 8 messages
		SingleSourceTestConfig firstTime = SingleSourceTestConfig.Builder.newBuilder()
			.setCountDownLatch(new CountDownLatch(8))
			.setParallelism(1)
			.setExcludedEndOffset(2)
			.setInitMaxOffset(1)
			.setExpectResultRowNum(8)
			.build();

		SingleSourceTestConfig secondTime = SingleSourceTestConfig.Builder.newBuilder()
			.setParallelism(1)
			// Recover from checkpoint offset 2, final offset is 3, expect 4 messages
			.setExpectResultRowNum(4)
			.setInitMaxOffset(3)
			.setExcludedEndOffset(3)
			.build();

		commonTestWithCheckpoint(1, firstTime, secondTime, 1);
	}

	@Test
	public void testSomeQueueNotInCheckpoint() throws Exception {
		// firstTime config will get 8 messages
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize4 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 2);
		List<RocketMQSplitBase> queueSize6 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 3);
		SingleSourceTestConfig firstTime = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setCountDownLatch(new CountDownLatch(8))
			.setParallelism(1)
			.setExcludedEndOffset(2)
			.setInitMaxOffset(1)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize4)
			.build();

		SingleSourceTestConfig secondTime = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			// Recover from checkpoint offset 2, final offset is 3, and 2 new queues, total 10 messages
			.setExpectResultRowNum(10)
			.setInitMaxOffset(3)
			.setExcludedEndOffset(3)
			.addNextSplitBaseLists(queueSize6)
			.build();

		commonTestWithCheckpoint(1, firstTime, secondTime, 1);
	}

	private void commonTest(SingleSourceTestConfig singleSourceTestConfig) throws Exception {
		commonTest(singleSourceTestConfig.getParallelism(), Collections.singletonList(singleSourceTestConfig));
	}

	private void commonTest(
			int maxParallelism,
			List<SingleSourceTestConfig> enumeratorConsumerConfigs) throws Exception {
		// execute sql
		TableResult tableResult = executeJob(maxParallelism, enumeratorConsumerConfigs, null, null);
		checkAndReturnResult(tableResult, enumeratorConsumerConfigs,
			enumeratorConsumerConfigs.stream().mapToInt(SingleSourceTestConfig::getResultRowNum).sum());
	}

	private List<Row> checkAndReturnResult(
			TableResult tableResult,
			List<SingleSourceTestConfig> enumeratorConsumerConfigs,
			int totalCnt) {
		List<Row> resultRows = new ArrayList<>();
		tableResult.collect().forEachRemaining(resultRows::add);
		List<List<Object>> resultTestData = new ArrayList<>();
		enumeratorConsumerConfigs.forEach(consumer -> resultTestData.addAll(consumer.getAllMockedData()));
		Assert.assertEquals(totalCnt, resultRows.size());
		verifyResult(resultRows, resultTestData);
		return resultRows;
	}

	private TableResult executeJob(
			int maxParallelism,
			List<SingleSourceTestConfig> enumeratorConsumerConfigs,
			String checkpointDir,
			String savepointDir) throws Exception {
		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(maxParallelism, checkpointDir, savepointDir);
		handleCreateTable(tEnv, enumeratorConsumerConfigs);

		// Generate select test sql
		String selectSql = TestSqlGenerator.getSelectSql(enumeratorConsumerConfigs.stream()
			.map(SingleSourceTestConfig::getSqlGeneratorConfig).collect(Collectors.toList()));

		// execute sql
		return tEnv.executeSql(selectSql);
	}

	private void commonTestWithCheckpoint(
			int maxParallelism,
			SingleSourceTestConfig firstConfig,
			SingleSourceTestConfig secondConfig,
			int additionalOffset) throws Exception {
		temporaryFolder.create();
		String tmpSavepointFolder = "file://" + temporaryFolder.newFolder().toString();
		String checkpointFolder = "file://" + temporaryFolder.newFolder().toString();
		CountDownLatch countDownLatch = firstConfig.getMockConsumerConfig().getCountDownLatch();

		// execute sql
		List<SingleSourceTestConfig> firstConfigs = Collections.singletonList(firstConfig);
		TableResult tableResult =
			executeJob(maxParallelism, firstConfigs, tmpSavepointFolder, checkpointFolder);
		JobClient jobClient = tableResult.getJobClient().get();
		// wait for fetch data
		boolean countDownFinished = countDownLatch.await(10, TimeUnit.SECONDS);
		Assert.assertTrue(countDownFinished);

		// trigger savepoint
		CompletableFuture<String> future = jobClient.triggerSavepoint(tmpSavepointFolder);
		future.get();

		// add additional offset to make job finished
		firstConfig.addAdditionalOffset(additionalOffset);

		// check first stage result
		checkAndReturnResult(tableResult, firstConfigs, firstConfig.getResultRowNum());

		TestMQConsumerStaticFactory.clear();
		// start again
		List<SingleSourceTestConfig> secondConfigs = Collections.singletonList(secondConfig);
		TableResult tableResult2 =
			executeJob(maxParallelism, secondConfigs, tmpSavepointFolder, checkpointFolder);

		// check second time result
		int totalCnt = firstConfig.getResultRowNum() + secondConfig.getResultRowNum();
		List<Row> resultRows = new ArrayList<>();
		tableResult2.collect().forEachRemaining(resultRows::add);
		Assert.assertEquals(totalCnt, resultRows.size());
		List<List<Object>> consumerResult = new ArrayList<>();
		consumerResult.addAll(firstConfig.getAllMockedData());
		consumerResult.addAll(secondConfig.getAllMockedData());
		verifyResult(resultRows, consumerResult);
	}

	private void handleCreateTable(
			StreamTableEnvironment tEnv,
			List<SingleSourceTestConfig> enumeratorConsumerConfigs) throws Exception {
		for (SingleSourceTestConfig enumConfig: enumeratorConsumerConfigs) {
			enumConfig.init();
			SqlGeneratorConfig sqlGeneratorConfig =
				enumConfig.getMockConsumerConfig().getSqlGeneratorConfig();

			// Generate test sql
			String createTableSql = TestSqlGenerator.generateCreateTableSql(sqlGeneratorConfig);
			tEnv.executeSql(createTableSql);
		}
	}

	protected List<RocketMQSplitBase> buildSplitList(String topic, String[] brokerList, int endId) {
		List<RocketMQSplitBase> splitBaseList = new ArrayList<>();
		for (int i = 0; i < endId; i++) {
			for (String broker: brokerList) {
				splitBaseList.add(new RocketMQSplitBase(topic, broker, i));
			}
		}
		return splitBaseList;
	}

	protected void verifyResult(List<Row> rowList, List<List<Object>> testDataList) {
		Assert.assertEquals(rowList.size(), testDataList.size());
		List<List<Object>> actualResult =
			rowList.stream().map(this::convertRowToObjectList).collect(Collectors.toList());
		Assert.assertEquals(new HashSet<>(actualResult), new HashSet<>(testDataList));
	}

	protected List<Object> convertRowToObjectList(Row row) {
		List<Object> objectList = new ArrayList<>();
		for (int i = 0; i < row.getArity(); i++) {
			objectList.add(row.getField(i));
		}
		return objectList;
	}

	protected StreamTableEnvironment createStreamTableEnvironment(
			int parallelism,
			String savePointDir,
			String checkpointDir) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(parallelism);

		if (savePointDir != null && checkpointDir != null) {
			env.getCheckpointConfig().setCheckpointInterval(100000);
			env.getConfiguration().set(CheckpointingOptions.STATE_BACKEND, "filesystem");
			env.getConfiguration().set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savePointDir);
			env.getConfiguration().set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		}

		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner().inStreamingMode().build();
		return StreamTableEnvironment.create(env, settings);
	}
}
