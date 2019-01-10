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

package com.bytedance.flink.component;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.pyjstorm.PYJStormProgressCache;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.bytedance.flink.collector.SpoutCollector;
import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A spout wrapper is a RichParallelSourceFunction and it wraps a ShellSpout.
 */
public class SpoutWrapper<OUT> extends RichParallelSourceFunction<OUT> implements StoppableFunction {
	private static final Logger LOG = LoggerFactory.getLogger(SpoutWrapper.class);

	private Spout spout;
	private String name;
	private volatile boolean isRunning;
	/**
	 * Number of attributes of the spouts's output tuples per stream.
	 */
	private Integer numberOfAttributes;
	/**
	 * Number of this parallel subtask, The numbering starts from 0 and goes up to parallelism-1.
	 */
	private Integer subTaskId;
	private volatile boolean localFailover;
	private volatile PySpoutProcess spoutProgress;

	public SpoutWrapper(Spout spout, String name, Integer numberOfAttributes) {
		this.spout = spout;
		this.name = name;
		this.numberOfAttributes = numberOfAttributes;
		this.isRunning = true;
		LOG.info("spout = {}", spout);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		subTaskId = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public void run(final SourceContext<OUT> sourceContext) throws Exception {
		RuntimeConfig runtimeConfig = (RuntimeConfig)
			getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		runtimeConfig.setSubTaskId(subTaskId);
		runtimeConfig.setTaskName(name);
		EnvironmentInitUtils.prepareLocalDir(runtimeConfig, spout);
		SpoutCollector<OUT> spoutCollector = new SpoutCollector<>(numberOfAttributes, sourceContext);
		localFailover = (boolean) runtimeConfig.getOrDefault(Constants.LOCAL_FAILOVER, false);
		String spoutProgressKey = runtimeConfig.getJobName() + "-" + this.name + "-"
			+ runtimeConfig.getSubTaskId();

		boolean attached = false;
		if (localFailover) {
			spoutProgress = (PySpoutProcess) PYJStormProgressCache.getInstance().get(spoutProgressKey);
			if (spoutProgress != null) {
				try {
					ShellSpout shellSpout = (ShellSpout) spoutProgress.getSpout();
					shellSpout.attach(spoutCollector);
					this.spout = shellSpout;
					attached = true;
					spoutProgress.markInUse();
					LOG.warn("attach successed spout, {}", spoutProgressKey);
				} catch (Exception e) {
					LOG.warn("attach failed spout, " + spoutProgressKey, e);
				}
			} else {
				LOG.warn("attach init spout, {}" + spoutProgressKey);
			}
		}

		if (!attached) {
			spout.open(runtimeConfig, spoutCollector);
			if (localFailover) {
				spoutProgress = new PySpoutProcess(runtimeConfig, name, subTaskId, spout);
				PYJStormProgressCache.getInstance().put(spoutProgressKey, spoutProgress);
				LOG.info("cached spout progress, name:{}, taskId:{}", name, subTaskId);
			}
		}

		while (isRunning) {
			spout.nextTuple();
		}
	}

	@Override
	public void stop() {
		this.isRunning = false;
		if (this.spoutProgress != null) {
			this.spoutProgress.markUnUse();
		}
	}

	@Override
	public void cancel() {
		this.isRunning = false;
	}

	@Override
	public void close() throws Exception {
		LOG.info("Try to close spout {}-{}", name, subTaskId);
		if (this.localFailover) {
			LOG.info("Suspend spout progress");
			((ShellSpout) this.spout).suspend();
			this.spoutProgress.markUnUse();
		} else {
			LOG.info("Close spout progress");
			this.spout.close();
		}
	}
}
