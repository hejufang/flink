/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */

package org.apache.flink.metrics.databus;

import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.bytedance.data.databus.DatabusClient;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.AbstractLifeCycle;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.util.Throwables;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Test Class For  {@link DatabusAppender}.
 */
@Plugin(name = "org.apache.flink.metrics.databus.TestDatabusAppender", category = "Core", elementType = Appender.ELEMENT_TYPE, printObject = true)
public class TestDatabusAppender extends AbstractAppender {

	private static final String LINE_SEP = System.getProperty("line.separator");

	private String channel;
	private String metricsChannel;
	private long metricsUpdateInterval;
	private int port;
	private int permitsPerSecond;

	private TestDatabusClient databusClient;
	private RateLimiter rateLimiter;
	private final ScheduledExecutorService executor;
	private TimerTask reporterTask;

	public volatile long droppedMessageCounts = 0L;
	public volatile long sendMessageCounts = 0L;
	public volatile long totalMessageCounts = 0L;

	TestDatabusAppender(final String channel, final String metricsChannel, final long metricsUpdateInterval, final int port, int permitsPerSecond, final String name, final Layout<? extends Serializable> layout, final Filter filter, final boolean ignoreExceptions, final Property[] properties) {
		super(name, filter, layout, ignoreExceptions, properties);
		this.channel = channel;
		this.metricsChannel = metricsChannel;
		this.metricsUpdateInterval = metricsUpdateInterval;
		this.port = port;
		this.permitsPerSecond = permitsPerSecond;
		this.executor = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("TestDatabusAppender-metrics-reporter"));
	}

	@Override
	public void append(final LogEvent event) {
		assert databusClient != null;
		totalMessageCounts++;
		final byte[] bytes = getLayout().toByteArray(event);
		String message = new String(bytes);
		if (!rateLimiter.tryAcquire()) {
			droppedMessageCounts++;
			return;
		}

		try {
			StringBuilder sb = new StringBuilder(message);
			String[] s = Throwables.toStringList(event.getThrown()).toArray(new String[0]);
			if (s.length != 0) {
				for (String l : s) {
					sb.append(LINE_SEP).append(l);
				}
				message = sb.toString();
			}
		} catch (Exception e) {
			LOGGER.error("Count not get error stack info.", e);
		}
		try {
			databusClient.send(ThreadContext.get("ip"), message, 0);
			sendMessageCounts++;
		} catch (IOException e) {
			LOGGER.error("Could send message by databus client.", e);
		}
	}

	@Override
	public void start() {
		super.start();

		initThreadContext();

		this.databusClient = new TestDatabusClient(port, channel);
		this.rateLimiter = RateLimiter.create(permitsPerSecond);

		reporterTask = new TestDatabusAppenderMetricsReporter(this);
		executor.scheduleWithFixedDelay(reporterTask, 0L, metricsUpdateInterval, TimeUnit.MILLISECONDS);
	}

	private void initThreadContext() {
		ThreadContext.put("test_thread_context", "test_thread_context_value");
	}

	@Override
	public void stop() {
		super.stop();
		this.databusClient.close();
	}

	public Map<String, Long> getMetricsAndRefresh(){
		Map<String, Long> metrics = new HashMap<>();
		metrics.put("totalMessageCounts", totalMessageCounts);
		metrics.put("droppedMessageCounts", droppedMessageCounts);
		metrics.put("sendMessageCounts", sendMessageCounts);

		totalMessageCounts = 0L;
		droppedMessageCounts = 0L;
		sendMessageCounts = 0L;

		return metrics;
	}

	public Map getMetricsReportResult(){
		return ((TestDatabusAppenderMetricsReporter) this.reporterTask).getReportResult();
	}

	public ArrayList getLogResult(){
		return databusClient.getMessageList();
	}

	public void refresh(){
		getMetricsReportResult().clear();
		getLogResult().clear();
	}

	public void setRateLimter(Long recordsPerSecond){
		this.rateLimiter.setRate(recordsPerSecond);
	}

	private static final class TestDatabusAppenderMetricsReporter extends TimerTask {

		private ObjectMapper objectMapper = new ObjectMapper();
		private TestDatabusAppender testDatabusAppender;
		private TestDatabusClient databusMetricsClient;
		private HashMap appenderMetrics;

		public TestDatabusAppenderMetricsReporter(TestDatabusAppender testDatabusAppender) {
			this.testDatabusAppender = testDatabusAppender;
			this.databusMetricsClient = new TestDatabusClient(DatabusClient.DEFAULT_PORT, testDatabusAppender.metricsChannel);
		}

		@Override
		public void run() {

			if (testDatabusAppender.totalMessageCounts > 0){
				reportAppenderMetrics();
			}
		}

		private void reportAppenderMetrics() {

			appenderMetrics = new HashMap(ThreadContext.getContext());
			appenderMetrics.putAll(testDatabusAppender.getMetricsAndRefresh());

			try {
				this.databusMetricsClient.send(ThreadContext.get("ip"), objectMapper.writeValueAsString(appenderMetrics), 0);
			} catch (IOException e) {
				LOGGER.error("Could send message by databus client.", e);
			}
		}

		public Map getReportResult(){
			reportAppenderMetrics();
			return this.appenderMetrics;
		}
	}

	/**
	 * test for datdabusClient , write messages to ArrayList.
	 */
	public static class TestDatabusClient{

		private int port;
		private String channel;
		private ArrayList<String> messageList;

		TestDatabusClient(int port, String channel){
			this.channel = channel;
			this.port = port;
			this.messageList = new ArrayList<>();
		}

		public void send(String key, String message, int codec) throws IOException{
			messageList.add(message);
		}

		public ArrayList getMessageList(){
			return messageList;
		}

		public void close() {}

	}

	/**
	 * builder for TestDatabusAppender.
	 * @param <B>
	 */
	public static class Builder<B extends TestDatabusAppender.Builder<B>> extends AbstractAppender.Builder<B>
		implements org.apache.logging.log4j.core.util.Builder<TestDatabusAppender> {

		@PluginAttribute(value = "channel")
		private String channel;

		@PluginAttribute(value = "port", defaultInt = DatabusClient.DEFAULT_PORT)
		private int port;

		@PluginAttribute(value = "permitsPerSecond", defaultInt = Integer.MAX_VALUE)
		private int permitsPerSecond;

		@PluginAttribute(value = "metricsChannel")
		private String metricsChannel;

		@PluginAttribute(value = "metricsUpdateInterval", defaultLong = 10000L)
		private int metricsUpdateInterval;

		@SuppressWarnings("resource")
		@Override
		public TestDatabusAppender build() {
			final Layout<? extends Serializable> layout = getLayout();
			if (layout == null) {
				AbstractLifeCycle.LOGGER.error("No layout provided for TestDatabusAppender");
				return null;
			}
			return new TestDatabusAppender(channel, metricsChannel, metricsUpdateInterval, port, permitsPerSecond, getName(), layout, getFilter(), isIgnoreExceptions(), getPropertyArray());
		}
	}

	@PluginBuilderFactory
	public static <B extends TestDatabusAppender.Builder<B>> B newBuilder() {
		return new TestDatabusAppender.Builder<B>().asBuilder();
	}
}
