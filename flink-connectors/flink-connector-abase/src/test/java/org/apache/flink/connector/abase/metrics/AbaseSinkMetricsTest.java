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

package org.apache.flink.connector.abase.metrics;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.connector.abase.AbaseTestBase;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.warehouse.ConnectorSinkMetricMessage;
import org.apache.flink.table.api.TableResult;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;

/**
 * The ITCase for abase sink metrics.
 */
public class AbaseSinkMetricsTest extends AbaseTestBase {

	@Test
	public void testMetricsReport() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`     VARCHAR,\n" +
				"  `score`    BIGINT,\n" +
				"  `bonus`    INT,\n" +
				"  `rank`     INT,\n" +
				"  `time`     timestamp,\n" +
				"  `event_ts` BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'format' = 'json',\n" +
				"  'sink.buffer-flush.max-rows' = '5',\n" +
				"  'sink.buffer-flush.interval' = '5 min',\n" +
				"  'sink.metrics.event-ts.name' = 'event_ts',\n" +
				"  'sink.metrics.props' = 'k1:v1,k2:v2,k3:v3'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `score`, `bonus`, `rank`, `time`, `event_ts`\n" +
			"FROM T3");

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

}
