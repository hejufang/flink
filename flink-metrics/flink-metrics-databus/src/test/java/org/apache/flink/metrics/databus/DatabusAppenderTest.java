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

package org.apache.flink.metrics.databus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Test for {@link DatabusAppender}.
 */
public class DatabusAppenderTest {

	private static Logger testLogger;
	private static TestDatabusAppender testDatabusAppender;

	static {
		System.setProperty("log4j.configurationFile", "log4j2-databus-test.properties");
		testLogger = LogManager.getLogger(DatabusAppenderTest.class);
		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final Configuration config = ctx.getConfiguration();
		testDatabusAppender = (TestDatabusAppender) config.getAppenders().get("TestDatabusAppender");
	}

	@Test
	public void testLogging() {

		testDatabusAppender.refresh();
		testLogger.info("test_info");
		testLogger.error("test_error");
		testLogger.warn("test_warn");
		List expect = Arrays.asList("db4jv3 test_thread_context_value INFO DatabusAppenderTest: test_info",
			"db4jv3 test_thread_context_value ERROR DatabusAppenderTest: test_error",
			"db4jv3 test_thread_context_value WARN DatabusAppenderTest: test_warn");
		Assert.assertArrayEquals(expect.toArray(), testDatabusAppender.getLogResult().toArray());
	}

	@Test
	public void testLogMetrics() {

		testDatabusAppender.refresh();
		testLogger.info("test_info");
		testLogger.error("test_error");
		testLogger.warn("test_warn");
		testLogger.warn("test_warn");

		Assert.assertEquals(4L, testDatabusAppender.getMetricsReportResult().get("sendMessageCounts"));
	}

	@Test
	@Ignore
	public void testRateLimiter() {

		testDatabusAppender.refresh();
		testDatabusAppender.setRateLimter(1L);

		long startTestTime = System.currentTimeMillis();
		testLogger.info("test_info");
		testLogger.error("test_error");
		testLogger.warn("test_warn");
		testLogger.debug("test_debug");
		testLogger.fatal("test_fatal");
		long finishTestTime = System.currentTimeMillis();

		if (finishTestTime - startTestTime < 1000L) {
			Map metrcisResult = testDatabusAppender.getMetricsReportResult();
			Assert.assertEquals(1L, metrcisResult.get("sendMessageCounts"));
			Assert.assertEquals(3L, metrcisResult.get("droppedMessageCounts"));
			Assert.assertEquals(4L, metrcisResult.get("totalMessageCounts"));
		}
	}
}
