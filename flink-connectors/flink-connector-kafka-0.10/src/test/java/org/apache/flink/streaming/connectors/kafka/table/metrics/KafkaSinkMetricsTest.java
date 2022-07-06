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

package org.apache.flink.streaming.connectors.kafka.table.metrics;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.databus.DatabusClientWrapper;
import org.apache.flink.metrics.databus.DatabusReporter;
import org.apache.flink.metrics.warehouse.ConnectorSinkMetricMessage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.table.Kafka010DynamicTableFactory;
import org.apache.flink.streaming.connectors.kafka.table.MockKafkaProducerConsumerContext;
import org.apache.flink.streaming.connectors.kafka.utils.KafkaUtils;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.streaming.connectors.kafka.table.MockKafkaProducerConsumerContext.withContextPresent;
import static org.mockito.ArgumentMatchers.any;

/**
 * The test of kafka sink metrics.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaUtils.class, DatabusReporter.class})
public class KafkaSinkMetricsTest {

	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Rule
	public TestName name = new TestName();

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Mock
	protected DatabusReporter databusReporter;

	@Mock
	protected DatabusClientWrapper databusClientWrapper;

	protected StreamTableEnvironment tEnv;

	@Before
	public void setUp() throws Exception {
		environmentVariables.set("RUNTIME_IDC_NAME", "BOE");

		PowerMockito.mockStatic(KafkaUtils.class);
		BDDMockito.given(KafkaUtils.getTimeService(any())).willReturn(new ProcessingTimeService() {
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
		databusReporter = PowerMockito.mock(DatabusReporter.class);
		PowerMockito.whenNew(DatabusClientWrapper.class).withAnyArguments().thenReturn(databusClientWrapper);

		Configuration config = new Configuration();
		setMetricsReporter(config);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(
			1,
			config);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		tEnv = StreamTableEnvironment.create(env, envSettings);
	}

	@Test
	public void sinkMetricsTest() {
		withContextPresent(
			ctx -> {
				final String sinkDDL = "CREATE TABLE sink(\n" +
					"  id BIGINT,\n" +
					"  name VARCHAR,\n" +
					"  `event_ts` BIGINT,\n" +
					"  PRIMARY KEY(id) NOT ENFORCED\n" +
					") WITH (\n" +
					"  'connector' = '" + Kafka010DynamicTableFactory.IDENTIFIER + "',\n" +
					"  'topic' = 'test-topic',\n" +
					"  'properties.cluster' = 'test-cluster',\n" +
					"  'properties.group.id' = 'test-group',\n" +
					"  'sink.producer-factory-class' = '" + MockKafkaProducerConsumerContext.KafkaStaticProducerFactoryForMock.class.getName() + "',\n" +
					"  'format' = 'json',\n" +
					"  'sink.metrics.event-ts.name' = 'event_ts',\n" +
					"  'sink.metrics.props' = 'k1:v1,k2:v2,k3:v3'\n" +
					")";
				tEnv.executeSql(sinkDDL);

				String insertSql = "INSERT INTO sink\n" +
					"SELECT *\n" +
					"FROM (\n" +
					"  VALUES (1, 'Leo', 1641744060000)," +
					"         (2, 'Messi', 1641744120000)," +
					"         (3, 'Albert', 1641744600000)," +
					"         (4, 'Einstein', 1641744720000)," +
					"         (5, 'Luffy', 1641744780000)\n" +
					")";
				TableEnvUtil.execInsertSqlAndWaitResult(tEnv, insertSql);

				try {
					Thread.sleep(10000);  // time wait to shut down the cluster
				} catch (InterruptedException e) {
					// ignore
				}

				try {
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
				} catch (IOException e) {
					throw new RuntimeException("Failed to check results!", e);
				}
			}
		);
	}

	private static void setMetricsReporter(Configuration config) {
		config.setString("metrics.reporters", "databus_reporter");
		config.setString("metrics.reporter.databus_reporter.class", "org.apache.flink.metrics.databus.DatabusReporter");
		config.setString("metrics.reporter.databus_reporter.interval", "10 min");
	}
}
