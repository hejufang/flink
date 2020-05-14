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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PreviewPlanEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A special {@link StreamExecutionEnvironment} that is used in the web frontend when generating
 * a user-inspectable graph of a streaming job.
 */
@PublicEvolving
public class StreamPlanEnvironment extends StreamExecutionEnvironment {
	private static final Logger LOG = LoggerFactory.getLogger(StreamPlanEnvironment.class);

	private ExecutionEnvironment env;

	protected StreamPlanEnvironment(ExecutionEnvironment env, Configuration configuration) {
		super(configuration);
		this.env = env;

		int parallelism = env.getParallelism();
		if (parallelism > 0) {
			setParallelism(parallelism);
		} else {
			// determine parallelism
			setParallelism(GlobalConfiguration.loadConfiguration().getInteger(CoreOptions.DEFAULT_PARALLELISM));
		}

		if (env.getConfig().getDefaultPartitioner() != null) {
			getConfig().setDefaultPartitioner(env.getConfig().getDefaultPartitioner());
		}
	}

	@Override
	public JobExecutionResult execute() throws Exception {
		return execute("");
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		replaceJobName(streamGraph);
		saveMetaAndRegisterDashboard(streamGraph);
		LOG.info("StreamPlanEnvironment execute {}", streamGraph.getJobName());
		transformations.clear();

		if (env instanceof OptimizerPlanEnvironment) {
			((OptimizerPlanEnvironment) env).setPlan(streamGraph);
		} else if (env instanceof PreviewPlanEnvironment) {
			((PreviewPlanEnvironment) env).setPreview(streamGraph.getStreamingPlanAsJSON());
		}

		throw new OptimizerPlanEnvironment.ProgramAbortException();
	}
}
