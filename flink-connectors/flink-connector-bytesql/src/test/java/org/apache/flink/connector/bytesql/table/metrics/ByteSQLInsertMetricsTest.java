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

package org.apache.flink.connector.bytesql.table.metrics;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.bytesql.table.ByteSQLOutputFormat;
import org.apache.flink.connector.bytesql.table.ByteSQLTableTestBase;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.databus.DatabusClientWrapper;
import org.apache.flink.metrics.databus.DatabusReporter;
import org.apache.flink.metrics.warehouse.ConnectorSinkMetricMessage;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.table.api.Expressions.$;
import static org.mockito.ArgumentMatchers.any;

/**
 * The ITCase for bytesql sink metrics.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ByteSQLOutputFormat.class, DatabusReporter.class})
public class ByteSQLInsertMetricsTest extends ByteSQLTableTestBase {
	@Mock
	protected DatabusReporter databusReporter;
	@Mock
	protected DatabusClientWrapper databusClientWrapper;

	@Before
	public void setUp() throws Exception {
		PowerMockito.mockStatic(ByteSQLOutputFormat.class);
		databusReporter = PowerMockito.mock(DatabusReporter.class);
		PowerMockito.whenNew(DatabusClientWrapper.class).withAnyArguments().thenReturn(databusClientWrapper);

		BDDMockito.given(ByteSQLOutputFormat.getTimeService(any())).willReturn(new ProcessingTimeService() {
			@Override
			public long getCurrentProcessingTime() {
				return 1641744840000L;    // 2022-01-10 00:14:00
			}

			@Override
			public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
				throw new UnsupportedOperationException();
			}

			@Override
			public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
				throw new UnsupportedOperationException();
			}

			@Override
			public ScheduledFuture<?> scheduleWithFixedDelay(ProcessingTimeCallback callback, long initialDelay, long period) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletableFuture<Void> quiesce() {
				throw new UnsupportedOperationException();
			}
		});
	}

	@Test
	public void testSinkMetricsOptions() throws Exception {
		Configuration config = new Configuration();
		setMetricsReporter(config);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(Runtime.getRuntime().availableProcessors(), config);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		Table t = tEnv.fromDataStream(get6TupleDataStream(env),
			$("name"), $("score"), $("bonus"), $("rank"), $("time"), $("event_ts"));

		tEnv.createTemporaryView("T", t);
		tEnv.executeSql(
			"CREATE TABLE sink (" +
				"  `name`     VARCHAR," +
				"  `score`    BIGINT," +
				"  `bonus`    INT," +
				"  `rank`     INT," +
				"  `time`     timestamp," +
				"  `event_ts` BIGINT " +
//				"  PRIMARY KEY (`name`, `score`) NOT ENFORCED" +
				") WITH (" +
				"  'connector' = 'bytesql'," +
				"  'consul' = 'test', " +
				"  'db.class'='org.apache.flink.connector.bytesql.table.client.ByteSQLDBMock'," +
				"  'database' = 'test', " +
				"  'table' = 'test', " +
				"  'username' = 'test', " +
				"  'password' = 'test', " +
				"  'sink.buffer-flush.interval' = '10s'," +
				"  'sink.metrics.event-ts.name' = 'event_ts'," +
				"  'sink.metrics.props' = 'k1:v1,k2:v2,k3:v3' " +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `score`, `bonus`, `rank`, `time`, `event_ts`\n" +
			"FROM T");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		JobClient jobClient = tableResult.getJobClient().get();
		CompletableFuture<JobExecutionResult> jobExecutionResult = jobClient.getJobExecutionResult(Thread.currentThread().getContextClassLoader());
		jobExecutionResult.get();

		Thread.sleep(10000);  // time wait to shut down the cluster
		ArgumentCaptor<String> dataArg = ArgumentCaptor.forClass(String.class);
		Mockito.verify(databusClientWrapper, Mockito.atLeastOnce()).addToBuffer(dataArg.capture());
		ObjectMapper objectMapper = new ObjectMapper();
		for (String data : dataArg.getAllValues()) {
			if (data.contains(MessageType.CONNECTOR_SINK_LATENCY.name())) {
				Message<ConnectorSinkMetricMessage> message = objectMapper.readValue(data,
					new TypeReference<Message<ConnectorSinkMetricMessage>>(){});
				Assert.assertEquals(5, message.getData().getCount());
				Assert.assertEquals("270.0", message.getData().getPercentiles().get(0.50).toString());
				Assert.assertEquals("810.0", message.getData().getPercentiles().get(0.90).toString());
				Assert.assertEquals("810.0", message.getData().getPercentiles().get(0.95).toString());
				Assert.assertEquals("810.0", message.getData().getPercentiles().get(0.99).toString());
				Assert.assertEquals("v1", message.getData().getProps().get("k1"));
				Assert.assertEquals("v2", message.getData().getProps().get("k2"));
				Assert.assertEquals("v3", message.getData().getProps().get("k3"));
			}
		}
	}

	private static void setMetricsReporter(Configuration config) {
		config.setString("metrics.reporters", "databus_reporter");
		config.setString("metrics.reporter.databus_reporter.class", "org.apache.flink.metrics.databus.DatabusReporter");
		config.setString("metrics.reporter.databus_reporter.interval", "10 min");
	}
}
