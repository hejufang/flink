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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestBase.isCausedByJobFinished;
import static org.apache.flink.streaming.connectors.kafka.table.MockKafkaProducerConsumerContext.withContextPresent;
import static org.junit.Assert.assertEquals;

/**
 * IT cases for Kafka 0.10 for Table API & SQL.
 */
@RunWith(Parameterized.class)
public class KafkaTable010ITCase {

	private static final String JSON_FORMAT = "json";

	protected StreamExecutionEnvironment env;
	protected StreamTableEnvironment tEnv;


	@Parameterized.Parameter
	public boolean hasMetadataColumn;

	@Parameterized.Parameter(1)
	public String format;

	@Parameterized.Parameters(name = "hasMetadataColumn = {0}, format = {1}")
	public static Object[] parameters() {
		return new Object[][]{
			new Object[]{false, JSON_FORMAT},
			new Object[]{true, JSON_FORMAT}
		};
	}

	@Before
	public void setup() {
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

	@Test
	public void testKafkaSourceSink() {
		withContextPresent(
			ctx -> {
				String topic = "test-topic";
				String cluster = "dummy-cluster";
				String groupId = "group-id";
				String formatOptions = formatOptions(format);

				String createTableSql = this.createTableSql(
					topic,
					cluster,
					groupId,
					formatOptions,
					MockKafkaProducerConsumerContext.KafkaStaticConsumerFactoryForMock.class.getName(),
					MockKafkaProducerConsumerContext.KafkaStaticProducerFactoryForMock.class.getName(),
					hasMetadataColumn
				);

				tEnv.executeSql(createTableSql);

				//test table sink.
				String initialValues = "INSERT INTO kafka\n" +
					"SELECT CAST(price AS DECIMAL(10, 2)), currency, " +
					" CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3)), 1, 1, 1\n" +
					"FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n" +
					"  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n" +
					"  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n" +
					"  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n" +
					"  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n" +
					"  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n" +
					"  AS orders (price, currency, d, t, ts)";
				TableEnvUtil.execInsertSqlAndWaitResult(tEnv, initialValues);

				//test source.
				String query = "SELECT\n" +
					"  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n" +
					"  CAST(MAX(log_date) AS VARCHAR),\n" +
					"  CAST(MAX(log_time) AS VARCHAR),\n" +
					"  CAST(MAX(ts) AS VARCHAR),\n" +
					"  COUNT(*),\n" +
					"  CAST(MAX(price) AS DECIMAL(10, 2))\n" +
					"FROM kafka\n" +
					"GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

				DataStream<RowData> result1 = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
				KafkaTableTestBase.TestingSinkFunction sink1 = new KafkaTableTestBase.TestingSinkFunction(2);
				result1.addSink(sink1).setParallelism(1);

				try {
					env.execute("Job_Test_Source");
				} catch (Throwable e) {
					// we have to use a specific exception to indicate the job is finished,
					// because the registered Kafka source is infinite.
					if (!isCausedByJobFinished(e)) {
						// re-throw
						throw new RuntimeException(e);
					}
				}

				List<String> expected = Arrays.asList(
					"+I(2019-12-12 00:00:05.000,2019-12-12,00:00:03,2019-12-12 00:00:04.004,3,50.00)",
					"+I(2019-12-12 00:00:10.000,2019-12-12,00:00:05,2019-12-12 00:00:06.006,2,5.33)");

				assertEquals(expected, KafkaTableTestBase.TestingSinkFunction.rows);

				if (!hasMetadataColumn) {
					return;
				}

				//metadata column query test
				final String metadataQuery = "SELECT\n" +
					"  metadata_timestamp,\n" +
					"  metadata_partition,\n" +
					"  metadata_offset\n" +
					"FROM kafka";

				DataStream<RowData> result2 = tEnv.toAppendStream(tEnv.sqlQuery(metadataQuery), RowData.class);
				KafkaTableTestBase.TestingSinkFunction sink2 = new KafkaTableTestBase.TestingSinkFunction(ctx.getDataCount());
				result2.addSink(sink2).setParallelism(1);

				try {
					env.execute("Job_Test_Source_Metadata");
				} catch (Throwable e) {
					// we have to use a specific exception to indicate the job is finished,
					// because the registered Kafka source is infinite.
					if (!isCausedByJobFinished(e)) {
						// re-throw
						throw new RuntimeException(e);
					}
				}
				final String testData = Lists
					.newArrayList(ctx.getTestTs(), ctx.getTestPartition(), ctx.getTestOffset())
					.stream()
					.map(String::valueOf)
					.collect(Collectors.joining(","));
				final String expectedMetadata = "+I(" + testData + ")";
				KafkaTableTestBase.TestingSinkFunction.rows.stream().forEach(row -> assertEquals(expectedMetadata, row));
			}
		);
	}

	private String createTableSql(
		String topic,
		String cluster,
		String groupId,
		String formatOptions,
		String consumerFactoryClassName,
		String producerFactoryClassName,
		boolean hasMetadataColumn
	) {
		final String metadataColumnOption;
		if (hasMetadataColumn) {
			metadataColumnOption = "'scan.metadata-fields-mapping' = 'timestamp=metadata_timestamp,partition=metadata_partition,offset=metadata_offset',";
		} else {
			metadataColumnOption = "";
		}

		final String createTable = String.format(
			"create table kafka (\n" +
				"  `computed-price` as price + 1.0,\n" +
				"  price decimal(38, 18),\n" +
				"  currency string,\n" +
				"  log_date date,\n" +
				"  log_time time(3),\n" +
				"  log_ts timestamp(3),\n" +
				"  `metadata_offset` bigint,\n" +
				"  `metadata_timestamp` bigint,\n" +
				"  `metadata_partition` bigint,\n" +
				"  ts as log_ts + INTERVAL '1' SECOND,\n" +
				"  watermark for ts as ts\n" +
				") with (\n" +
				"  'properties.unit.test' = 'true',\n" +
				"  'connector' = '%s',\n" +
				"  'topic' = '%s',\n" +
				metadataColumnOption + "\n" +
				"  'properties.cluster' = '%s',\n" +
				"  'properties.group.id' = '%s',\n" +
				"  'scan.consumer-factory-class' = '%s',\n" +
				"  'sink.producer-factory-class' = '%s',\n" +
				"  %s\n" +
				")",
			Kafka010DynamicTableFactory.IDENTIFIER,
			topic,
			cluster,
			groupId,
			consumerFactoryClassName,
			producerFactoryClassName,
			formatOptions);

		return createTable;
	}

	private String formatOptions(String format) {
		return String.format("'format' = '%s'", format);
	}
}
