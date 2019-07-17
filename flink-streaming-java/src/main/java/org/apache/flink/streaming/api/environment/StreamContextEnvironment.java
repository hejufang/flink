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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.DetachedEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.monitor.Dashboard;
import org.apache.flink.monitor.JobMeta;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Special {@link StreamExecutionEnvironment} that will be used in cases where the CLI client or
 * testing utilities create a {@link StreamExecutionEnvironment} that should be used when
 * {@link StreamExecutionEnvironment#getExecutionEnvironment()} is called.
 */
@PublicEvolving
public class StreamContextEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(StreamContextEnvironment.class);

	private final ContextEnvironment ctx;

	protected StreamContextEnvironment(ContextEnvironment ctx) {
		this.ctx = ctx;
		if (ctx.getParallelism() > 0) {
			setParallelism(ctx.getParallelism());
		}
	}

	@Override
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		transformations.clear();

		String jobName = streamGraph.getJobName();
		String newClusterName = System.getProperty(ConfigConstants.CLUSTER_NAME_KEY,
			ConfigConstants.CLUSTER_NAME_DEFAULT);
		String newJobName = System.getProperty(ConfigConstants.JOB_NAME_KEY);
		String dataSource = ConfigConstants.DATA_SOURCE_DEFAULT;

		//Replace inner name with yarn app name.
		if (newJobName != null && !"".equals(newJobName)){
			jobName = newJobName;
		}
		streamGraph.setJobName(jobName);

		JobGraph jobGraph = ClusterClient.getJobGraph(ctx.getClient().getFlinkConfiguration(),
			streamGraph, ctx.getJars(),
			ctx.getClasspaths(), ctx.getSavepointRestoreSettings());
		boolean saveJobMetaSuccessfully;

		try {
			Configuration flinkConfig = ctx.getClient().getFlinkConfiguration();
			dataSource = flinkConfig.getString(ConfigConstants.DATA_SOURCE_KEY,
				ConfigConstants.DATA_SOURCE_DEFAULT);
			JobMeta jobMeta = new JobMeta(streamGraph, jobGraph, flinkConfig);
			saveJobMetaSuccessfully = jobMeta.saveToDB();
		} catch (Throwable e) {
			saveJobMetaSuccessfully = false;
			LOG.warn("Failed to save job meta to database.", e);
		}
		if (saveJobMetaSuccessfully){
			LOG.info("Succeed in save job meta to database.");
		} else {
			LOG.warn("Failed to save job meta to database.");
		}

		boolean registerDashboardSuccessfully;
		try {
			Dashboard dashboard = new Dashboard(newClusterName, dataSource, streamGraph, jobGraph);
			registerDashboardSuccessfully = dashboard.registerDashboard();
		} catch (Throwable e){
			registerDashboardSuccessfully = false;
			LOG.warn("Failed to registering dashboard!", e);
		}
		if (registerDashboardSuccessfully){
			LOG.info("Succeed in registering dashboard.");
		} else {
			LOG.warn("Failed to registering dashboard!");
		}
		// execute the programs
		if (ctx instanceof DetachedEnvironment) {
			LOG.warn("Job was executed in detached mode, the results will be available on completion.");
			((DetachedEnvironment) ctx).setDetachedPlan(streamGraph);
			return DetachedEnvironment.DetachedJobExecutionResult.INSTANCE;
		} else {
			return ctx
				.getClient()
				.run(streamGraph, ctx.getJars(), ctx.getClasspaths(), ctx.getUserCodeClassLoader(),
						ctx.getSavepointRestoreSettings())
				.getJobExecutionResult();
		}
	}
}
