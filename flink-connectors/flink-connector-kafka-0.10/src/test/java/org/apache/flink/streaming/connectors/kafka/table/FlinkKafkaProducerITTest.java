/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.DeleteNormalizer;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.streaming.connectors.kafka.table.MockKafkaProducerConsumerContext.withContextPresent;
import static org.junit.Assert.assertEquals;

/**
 * IT cases for Kafka 0.10 producer.
 */
public class FlinkKafkaProducerITTest {

	protected StreamExecutionEnvironment env;
	protected StreamTableEnvironment tEnv;

	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Before
	public void setup() {
		environmentVariables.set("RUNTIME_IDC_NAME", "BOE");
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tEnv = StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance()
				// Watermark is only supported in blink planner
				.useBlinkPlanner()
				.inStreamingMode()
				.build()
		);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		// we have to use single parallelism,
		// because we will count the messages in sink to terminate the job
		env.setParallelism(1);
	}

	private void testDeleteNormalizerInternal(String normalizer, List<String> expected) {
		withContextPresent(
			ctx -> {
				final String sinkDDL = "CREATE TABLE mysink(\n" +
					"  cnt1 bigint,\n" +
					"  cnt2 bigint,\n" +
					"  primary key(cnt1) not enforced\n" +
					") WITH (\n" +
					"  'connector' = '" + Kafka010DynamicTableFactory.IDENTIFIER + "',\n" +
					"  'topic' = 'test-topic',\n" +
					"  'properties.cluster' = 'test-cluster',\n" +
					"  'properties.group.id' = 'test-group',\n" +
					"  'sink.producer-factory-class' = '" + MockKafkaProducerConsumerContext.KafkaStaticProducerFactoryForMock.class.getName() + "',\n" +
					"  'sink.delete-normalizer' = '" + normalizer + "',\n" +
					"  'format' = 'json'\n" +
					")";

				tEnv.executeSql(sinkDDL);

				//test table sink.
				String initialValues = "INSERT INTO mysink\n" +
					"SELECT cnt as cnt1, count(1) as cnt2\n" +
					"FROM (\n" +
					"  SELECT count(1) as cnt\n" +
					"  FROM (VALUES ('hello'), ('world'))\n" +
					")\n" +
					"GROUP BY cnt";
				TableEnvUtil.execInsertSqlAndWaitResult(tEnv, initialValues);

				List<String> result = ctx.getResults();

				assertEquals(expected, result);
			}
		);
	}

	@Test
	public void testDeleteNormalizer() {
		testDeleteNormalizerInternal(DeleteNormalizer.NormalizeType.NULL_FOR_NON_PRIMARY_FIELDS.name(), Arrays.asList(
			"{\"cnt1\":1,\"cnt2\":1}",
			"{\"cnt1\":1,\"cnt2\":null}",
			"{\"cnt1\":2,\"cnt2\":1}"));
	}

	@Test
	public void testNoDeleteNormalizer() {
		testDeleteNormalizerInternal(DeleteNormalizer.NormalizeType.NONE.name(), Arrays.asList(
			"{\"cnt1\":1,\"cnt2\":1}",
			"{\"cnt1\":2,\"cnt2\":1}"));
	}

	private void testSinkMsgKeyInternal(String sinkMsgKey, List<String> expected) {
		withContextPresent(
			ctx -> {
				final String sinkDDL = "CREATE TABLE mysink(\n" +
					"  boolean_field boolean,\n" +
					"  int_field int,\n" +
					"  bigint_field bigint,\n" +
					"  float_field float,\n" +
					"  double_field double,\n" +
					"  varchar_field varchar,\n" +
					"  binary_field binary,\n" +
					"  decimal_field decimal(18, 9),\n" +
					"  tinyint_field tinyint,\n" +
					"  smallint_field smallint,\n" +
					"  array_int_field array<int>\n" +
					") WITH (\n" +
					"  'connector' = '" + Kafka010DynamicTableFactory.IDENTIFIER + "',\n" +
					"  'topic' = 'test-topic',\n" +
					"  'properties.cluster' = 'test-cluster',\n" +
					"  'properties.group.id' = 'test-group',\n" +
					"  'sink.producer-factory-class' = '" + MockKafkaProducerConsumerContext.KafkaStaticProducerFactoryForMock.class.getName() + "',\n" +
					"  'sink.msg-key' = '" + sinkMsgKey + "',\n" +
					"  'format' = 'json'\n" +
					")";

				tEnv.executeSql(sinkDDL);

				//test table sink.
				String initialValues = "INSERT INTO mysink\n" +
					"SELECT *\n" +
					"FROM (\n" +
					"VALUES (true, 123, 117927469461234, CAST(0.709 AS FLOAT), CAST(0.68354621 AS DOUBLE), 'hello', x'68656c6c6f', 123456789.987654321, CAST(45 AS TINYINT), CAST(123 AS SMALLINT), ARRAY[1, 2, 3, 4, 5]),\n" +
					" (false, 456, 36091809311234, CAST(0.208 AS FLOAT), CAST(0.56293294 AS DOUBLE), 'world', X'68656c6c6f',  678901234.567891234, CAST(-68 AS TINYINT), CAST(456 AS SMALLINT), ARRAY[3, 4, 5, 6, 7]),\n" +
					" (true, 789, 63980359211234, CAST(0.809 AS FLOAT), CAST(0.62947892 AS DOUBLE), 'key', x'68656c6c6f', 987654321.123456789, CAST(121 AS TINYINT), CAST(789 AS SMALLINT), ARRAY[5, 6, 7, 8, 9])\n" +
					")";
				TableEnvUtil.execInsertSqlAndWaitResult(tEnv, initialValues);

				List<String> result = ctx.getSinkMsgKeyResults();

				assertEquals(expected, result);
			}
		);
	}

	@Test
	public void testSinkMsgKeyBoolean() {
		testSinkMsgKeyInternal("boolean_field", Arrays.asList("true", "false", "true"));
	}

	@Test
	public void testSinkMsgKeyInt() {
		testSinkMsgKeyInternal("int_field", Arrays.asList("123", "456", "789"));
	}

	@Test
	public void testSinkMsgKeyBigint() {
		testSinkMsgKeyInternal("bigint_field", Arrays.asList("117927469461234", "36091809311234", "63980359211234"));
	}

	@Test
	public void testSinkMsgKeyFloat() {
		testSinkMsgKeyInternal("float_field", Arrays.asList("0.709", "0.208", "0.809"));
	}

	@Test
	public void testSinkMsgKeyDouble() {
		testSinkMsgKeyInternal("double_field", Arrays.asList("0.68354621", "0.56293294", "0.62947892"));
	}

	@Test
	public void testSinkMsgKeyVarchar() {
		testSinkMsgKeyInternal("varchar_field", Arrays.asList("hello", "world", "key"));
	}

	@Test
	public void testSinkMsgKeyBinary() {
		testSinkMsgKeyInternal("binary_field", Arrays.asList("hello", "hello", "hello"));
	}

	@Test
	public void testSinkMsgKeyDecimal() {
		testSinkMsgKeyInternal("decimal_field", Arrays.asList("123456789.987654321", "678901234.567891234", "987654321.123456789"));
	}

	@Test
	public void testSinkMsgKeyTinyint() {
		testSinkMsgKeyInternal("tinyint_field", Arrays.asList("45", "-68", "121"));
	}

	@Test
	public void testSinkMsgKeySmallint() {
		testSinkMsgKeyInternal("smallint_field", Arrays.asList("123", "456", "789"));
	}

	@Rule
	public ExpectedException exception = ExpectedException.none();
	@Test
	public void testUnsupportedKeyType() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Unsupported type for kafka sink message key field:");
		testSinkMsgKeyInternal("array_int_field", Arrays.asList("null"));
	}
}
