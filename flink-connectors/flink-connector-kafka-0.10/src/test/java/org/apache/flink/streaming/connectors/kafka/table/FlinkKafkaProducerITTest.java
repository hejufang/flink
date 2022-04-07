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
}
