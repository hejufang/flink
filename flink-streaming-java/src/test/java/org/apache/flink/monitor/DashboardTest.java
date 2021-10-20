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

package org.apache.flink.monitor;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.StringUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for Dashboard.
 */
public class DashboardTest {

	@Test
	public void generateDashboard(){
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataStream<Long> input1 = env.generateSequence(0, 0)
			.map(new MapFunction<Long, Long>() {
				@Override
				public Long map(Long value) throws Exception {
					return null;
				}
			});

		DataStream<Long> selfUnion = input1.union(input1).map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return null;
			}
		});

		StreamGraph streamGraph = env.getStreamGraph(StreamExecutionEnvironment.DEFAULT_JOB_NAME);
		JobGraph jobGraph = streamGraph.getJobGraph();
		Dashboard dashboard = new Dashboard("flink", "bytetsd", streamGraph, jobGraph);
		String dashboardJson = dashboard.renderDashboard();

		Assert.assertTrue(!StringUtils.isNullOrWhitespaceOnly(dashboardJson));
	}
}
