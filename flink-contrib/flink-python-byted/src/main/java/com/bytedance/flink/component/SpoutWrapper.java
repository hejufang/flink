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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.bytedance.flink.collector.SpoutCollector;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A spout wrapper is a RichParallelSourceFunction and it wraps a ShellSpout.
 */
public class SpoutWrapper<OUT> extends RichParallelSourceFunction<OUT> {
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
		spout.open(runtimeConfig, spoutCollector);

		while (isRunning) {
			spout.nextTuple();
		}
	}

	@Override
	public void cancel() {
		this.isRunning = false;
	}

	@Override
	public void close() throws Exception {
		LOG.info("Try to close spout {}-{}", name, subTaskId);
		this.spout.close();
	}
}
