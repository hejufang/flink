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

import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connector.rocketmq.RocketMQFlip27ITTest.createStreamTableEnvironment;
import static org.apache.flink.connector.rocketmq.RocketMQTestUtils.TEST_BROKER_LIST_SIZE_2;

/**
 * RocketMQMetadataITCase.
 */
public class RocketMQMetadataITCase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	public static final String METADATA_TEMPLATE_SQL = "create table test_table (\n" +
		"%s" +
		"    broker varchar,\n" +
		"    queue_id bigint,\n" +
		"    offset_field bigint\n" +
		"%s" +
		") WITH (\n" +
		"\t'connector' = 'rocketmq',\n" +
		"\t'cluster' = 'clusterDefault',\n" +
		"\t'topic' = 'topicA',\n" +
		"\t'group' = 'consumerDefault',\n" +
		"\t'scan.startup-mode' = 'earliest'\n," +
		"%s" +
		"\t'scan.use-flip27-source' = 'true',\n" +
		"\t'scan.end-offset' = '4',\n" +
		// Add discover interval to make test faster
		"\t'scan.discover-queue-interval' = '2 s',\n" +
		"\t'scan.metadata-fields-mapping' = 'offset=offset_field,queue_id=queue_id,broker_name=broker',\n" +
		"\t'scan.consumer-factory-class' = 'org.apache.flink.connector.rocketmq.TestMQConsumerStaticFactory'\n" +
		")";

	public static final String NO_METADATA_TEMPLATE_SQL = "create table test_table \n" +
		"\n%s\n" +
		" WITH (\n" +
		"\t'connector' = 'rocketmq',\n" +
		"\t'cluster' = 'clusterDefault',\n" +
		"\t'topic' = 'topicA',\n" +
		"\t'group' = 'consumerDefault',\n" +
		"\t'scan.startup-mode' = 'earliest'\n," +
		"%s" +
		"\t'scan.use-flip27-source' = 'true',\n" +
		"\t'scan.end-offset' = '4',\n" +
		// Add discover interval to make test faster
		"\t'scan.discover-queue-interval' = '2 s',\n" +
		"\t'scan.consumer-factory-class' = 'org.apache.flink.connector.rocketmq.TestMQConsumerStaticFactory'\n" +
		")";

	private final Set<String> metadataResults = new HashSet<>(Arrays.asList(
		"test_dc1_0,0,0", "test_dc1_0,0,1", "test_dc1_0,0,2", "test_dc1_0,0,3",
		"test_dc2_0,0,0", "test_dc2_0,0,1", "test_dc2_0,0,2", "test_dc2_0,0,3"));

	@Test
	public void testPbWithMetaData() throws Exception {
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize2 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 1);
		SingleSourceTestConfig testConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			.setExcludedEndOffset(3)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize2)
			.setGenDataFormat("pb")
			.build();

		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(1, null, null);
		testConfig.init();

		String createTableSql = String.format(METADATA_TEMPLATE_SQL, "", "",
			"\t'format' = 'pb',\n'pb.pb-class' = 'org.apache.flink.formats.pb.TestPb$InnerMessage',\n");
		tEnv.executeSql(createTableSql);
		TableResult tableResult = tEnv.executeSql("select " +
			"broker, queue_id, offset_field,longTest, boolTest \n" +
			" from test_table");
		validateMetadata(tableResult);
	}

	@Test
	public void testPbWithMetaDataComputeColumnBefore() throws Exception {
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize2 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 1);
		SingleSourceTestConfig testConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			.setExcludedEndOffset(3)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize2)
			.setGenDataFormat("pb")
			.build();

		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(1, null, null);
		testConfig.init();

		String createTableSql = String.format(METADATA_TEMPLATE_SQL, "\tproc_time as PROCTIME(),\n", "",
			"\t'format' = 'pb',\n'pb.pb-class' = 'org.apache.flink.formats.pb.TestPb$InnerMessage',\n");
		tEnv.executeSql(createTableSql);
		TableResult tableResult = tEnv.executeSql("select " +
			"broker, queue_id, offset_field,longTest, boolTest, proc_time \n" +
			" from test_table");
		validateMetadata(tableResult);
	}

	@Test
	public void testPbWithMetaDataComputeColumn() throws Exception {
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize2 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 1);
		SingleSourceTestConfig testConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			.setExcludedEndOffset(3)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize2)
			.setGenDataFormat("pb")
			.build();

		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(1, null, null);
		testConfig.init();

		String createTableSql = String.format(METADATA_TEMPLATE_SQL, "", ",\nproc_time as PROCTIME()\n",
			"\t'format' = 'pb',\n'pb.pb-class' = 'org.apache.flink.formats.pb.TestPb$InnerMessage',\n");
		tEnv.executeSql(createTableSql);
		TableResult tableResult = tEnv.executeSql("select " +
			"broker, queue_id, offset_field,longTest, boolTest, proc_time \n" +
			" from test_table");
		validateMetadata(tableResult);
	}

	@Test
	public void testFastPbWithMetaData() throws Exception {
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize2 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 1);
		SingleSourceTestConfig testConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			.setExcludedEndOffset(3)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize2)
			.setGenDataFormat("fast-pb")
			.build();

		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(1, null, null);
		testConfig.init();

		String createTableSql = String.format(METADATA_TEMPLATE_SQL, "", "",
			"\t'format' = 'fast-pb',\n'fast-pb.pb-class' = 'org.apache.flink.formats.pb.TestPb$InnerMessage',\n");
		tEnv.executeSql(createTableSql);
		TableResult tableResult = tEnv.executeSql("select " +
			"broker, queue_id, offset_field,longTest, boolTest \n" +
			" from test_table");
		validateMetadata(tableResult);
	}

	@Test
	public void testBytesWithMetadata() throws Exception {
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize2 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 1);
		SingleSourceTestConfig testConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			.setExcludedEndOffset(3)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize2)
			.build();

		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(1, null, null);
		testConfig.init();

		String createTableSql = String.format(METADATA_TEMPLATE_SQL,
			"\tbyteString varbinary,", "", "\t'format' = 'bytes',\n");
		tEnv.executeSql(createTableSql);
		TableResult tableResult = tEnv.executeSql("select " +
			"broker, queue_id, offset_field,byteString \n" +
			" from test_table");
		validateMetadata(tableResult);
	}

	@Test
	public void testPbWithoutMetadata() throws Exception {
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize2 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 1);
		SingleSourceTestConfig testConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			.setExcludedEndOffset(3)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize2)
			.setGenDataFormat("pb")
			.build();

		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(1, null, null);
		testConfig.init();

		String createTableSql = String.format(NO_METADATA_TEMPLATE_SQL, "",
			"\t'format' = 'pb',\n'pb.pb-class' = 'org.apache.flink.formats.pb.TestPb$InnerMessage',\n");
		tEnv.executeSql(createTableSql);
		TableResult tableResult = tEnv.executeSql("select " +
			"longTest, boolTest \n" +
			" from test_table");

		Set<String> results = new HashSet<>();
		tableResult.collect().forEachRemaining(r -> results.add(
			r.getField(0) + "," + r.getField(1)));
		Assert.assertEquals(results.size(), 8);
	}

	@Test
	public void testFastPbWithoutMetadata() throws Exception {
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize2 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 1);
		SingleSourceTestConfig testConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			.setExcludedEndOffset(3)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize2)
			.setGenDataFormat("fast-pb")
			.build();

		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(1, null, null);
		testConfig.init();

		String createTableSql = String.format(NO_METADATA_TEMPLATE_SQL, "",
			"\t'format' = 'fast-pb',\n'fast-pb.pb-class' = 'org.apache.flink.formats.pb.TestPb$InnerMessage',\n");
		tEnv.executeSql(createTableSql);
		TableResult tableResult = tEnv.executeSql("select " +
			"longTest, boolTest \n" +
			" from test_table");

		Set<String> results = new HashSet<>();
		tableResult.collect().forEachRemaining(r -> results.add(
			r.getField(0) + "," + r.getField(1)));
		Assert.assertEquals(results.size(), 8);
	}

	@Test
	public void testBytesWithoutMetadata() throws Exception {
		String topic = "topicA";
		List<RocketMQSplitBase> queueSize2 = RocketMQTestUtils.buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 1);
		SingleSourceTestConfig testConfig = SingleSourceTestConfig.Builder.newBuilder()
			.setTopic(topic)
			.setParallelism(1)
			.setExcludedEndOffset(3)
			.setExpectResultRowNum(8)
			.addNextSplitBaseLists(queueSize2)
			.build();

		// Mock test consumer
		StreamTableEnvironment tEnv = createStreamTableEnvironment(1, null, null);
		testConfig.init();

		String createTableSql = String.format(METADATA_TEMPLATE_SQL,
			"\tbyteString varbinary,", "", "\t'format' = 'bytes',\n");
		tEnv.executeSql(createTableSql);
		TableResult tableResult = tEnv.executeSql("select byteString from test_table");

		Set<String> results = new HashSet<>();
		tableResult.collect().forEachRemaining(r -> results.add(
			new String((byte[]) r.getField(0))));
		Assert.assertEquals(results.size(), 8);
	}

	private void validateMetadata(TableResult tableResult) {
		Set<String> results = new HashSet<>();
		tableResult.collect().forEachRemaining(r -> results.add(
			r.getField(0) + "," + r.getField(1) + "," + r.getField(2)));
		Assert.assertEquals(results, metadataResults);
	}
}
