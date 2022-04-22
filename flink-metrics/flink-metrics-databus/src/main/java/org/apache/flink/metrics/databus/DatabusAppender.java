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
import org.apache.flink.yarn.YarnConfigKeys;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.bytedance.data.databus.DatabusClient;
import com.bytedance.data.databus.EnvUtils;
import org.apache.logging.log4j.Level;
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
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Sends log events to DatabusAppender.
 */
@Plugin(name = "org.apache.flink.metrics.databus.DatabusAppender", category = "Core", elementType = Appender.ELEMENT_TYPE, printObject = true)
public final class DatabusAppender extends AbstractAppender {

	private static final String LINE_SEP = System.getProperty("line.separator");

	private String channel;
	private String metricsChannel;
	private long metricsUpdateInterval;
	private int port;
	private int permitsPerSecond;
	private Level databusLevel;

	private DatabusClient databusClient;
	private RateLimiter rateLimiter;
	private final ScheduledExecutorService executor;
	private TimerTask reporterTask;

	public volatile long droppedMessageCount = 0L;
	public volatile long sendMessageCount = 0L;
	public volatile long totalMessageCount = 0L;
	public volatile long sendDatabusMessageCount = 0L;
	public volatile long sendStreamlogMessageCount = 0L;

	DatabusAppender(final String channel, final String metricsChannel, final long metricsUpdateInterval, final int port, int permitsPerSecond, final String name, Level databusLevel,
					final Layout<? extends Serializable> layout, final Filter filter, final boolean ignoreExceptions, final Property[] properties) {
		super(name, filter, layout, ignoreExceptions, properties);
		this.channel = channel;
		this.metricsChannel = metricsChannel;
		this.metricsUpdateInterval = metricsUpdateInterval;
		this.port = port;
		this.permitsPerSecond = permitsPerSecond;
		this.databusLevel = databusLevel;
		this.executor = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("databusAppender-metrics-reporter"));
	}

	@Override
	public void append(final LogEvent event) {

		totalMessageCount++;

		if (!rateLimiter.tryAcquire()) {
			droppedMessageCount++;
			return;
		}

		final byte[] bytes = getLayout().toByteArray(event);
		String message = new String(bytes);

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
			//only send WARN & ERROR log to databus
			if (event.getLevel().isMoreSpecificThan(databusLevel)) {
				databusClient.send(ThreadContext.get("ip"), message, 0);
				sendDatabusMessageCount++;
			}
			sendMessageCount++;
		} catch (IOException e) {
			LOGGER.error("Could send message by databus client.", e);
		}

	}

	@Override
	public void start() {
		super.start();
		initThreadContext();
		this.databusClient = new DatabusClient(port, channel);
		this.rateLimiter = RateLimiter.create(permitsPerSecond);
		this.reporterTask = new DatabusAppenderMetricsReporter(this);
		executor.scheduleWithFixedDelay(reporterTask, 0L, metricsUpdateInterval, TimeUnit.MILLISECONDS);
	}

	private void initThreadContext() {
		ThreadContext.put("user", EnvUtils.getUser());
		try {
			ThreadContext.put("ip", EnvUtils.getIp());
		} catch (Exception e) {
			ThreadContext.put("ip", "unknown");
		}
		try {
			ThreadContext.put("pid", EnvUtils.getPid());
		} catch (Exception e) {
			ThreadContext.put("pid", "unknown");
		}
		ThreadContext.put("cid", EnvUtils.getContainerId());
		ThreadContext.put("hadoop_user", EnvUtils.getHadoopUser());
		ThreadContext.put("yarn_job", EnvUtils.getYarnJob());
		ThreadContext.put("yarn_queue", EnvUtils.getYarnQueue());
		String applicationId = System.getenv(YarnConfigKeys.ENV_APP_ID) == null ? "unkown" : System.getenv(YarnConfigKeys.ENV_APP_ID);
		ThreadContext.put("application_id", applicationId);
	}

	@Override
	public void stop() {
		super.stop();
		this.databusClient.close();
	}

	public Map<String, Long> getMetricsAndRefresh() {

		Map<String, Long> metrics = new HashMap<>();
		metrics.put("totalMessageCount", totalMessageCount);
		metrics.put("droppedMessageCount", droppedMessageCount);
		metrics.put("sendMessageCount", sendMessageCount);
		metrics.put("sendDatabusMessageCount", sendDatabusMessageCount);
		metrics.put("sendStreamlogMessageCount", sendStreamlogMessageCount);

		totalMessageCount = 0L;
		droppedMessageCount = 0L;
		sendMessageCount = 0L;
		sendDatabusMessageCount = 0L;
		sendStreamlogMessageCount = 0L;
		return metrics;

	}

	private static final class DatabusAppenderMetricsReporter extends TimerTask {

		private ObjectMapper objectMapper = new ObjectMapper();
		private DatabusAppender databusAppender;
		private DatabusClient databusMetricsClient;

		public DatabusAppenderMetricsReporter(DatabusAppender databusAppender) {
			this.databusAppender = databusAppender;
			this.databusMetricsClient = new DatabusClient(DatabusClient.DEFAULT_PORT, databusAppender.metricsChannel);
		}

		@Override
		public void run() {
			if (databusAppender.totalMessageCount > 0){
				reportAppenderMetrics();
			}
		}

		private void reportAppenderMetrics() {

			HashMap appenderMetrics = new HashMap(ThreadContext.getContext());
			appenderMetrics.putAll(databusAppender.getMetricsAndRefresh());
			appenderMetrics.put("reportTime", System.currentTimeMillis());
			try {
				this.databusMetricsClient.send(ThreadContext.get("ip"), objectMapper.writeValueAsString(appenderMetrics), 0);
			} catch (IOException e) {
				LOGGER.error("Could send message by databus client.", e);
			}
		}
	}

	/**
	 * builder for DatabusAppender.
	 * @param <B>
	 */
	public static class Builder<B extends DatabusAppender.Builder<B>> extends AbstractAppender.Builder<B>
		implements org.apache.logging.log4j.core.util.Builder<DatabusAppender> {

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

		@PluginAttribute(value = "databusLevel", defaultString = "WARN")
		private String databusLevel;

		@SuppressWarnings("resource")
		@Override
		public DatabusAppender build() {
			final Layout<? extends Serializable> layout = getLayout();
			if (layout == null) {
				AbstractLifeCycle.LOGGER.error("No layout provided for DatabusAppender");
				return null;
			}
			System.out.println("DatabusAppender start with : channel " + channel + ", metricsChannel : " + metricsChannel);
			return new DatabusAppender(channel, metricsChannel, metricsUpdateInterval, port, permitsPerSecond, getName(), Level.getLevel(databusLevel), layout, getFilter(), isIgnoreExceptions(), getPropertyArray());
		}
	}

	@PluginBuilderFactory
	public static <B extends DatabusAppender.Builder<B>> B newBuilder() {
		return new DatabusAppender.Builder<B>().asBuilder();
	}
}
