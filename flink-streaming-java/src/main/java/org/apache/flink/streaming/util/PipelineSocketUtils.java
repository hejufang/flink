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

package org.apache.flink.streaming.util;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.sink.TaskPushSinkOperatorFactory;

import java.util.Collection;

/**
 * Utils to check whether pipeline can use socket client.
 */
public class PipelineSocketUtils {
	/**
	 * Check whether pipeline can use socket client.
	 *
	 * @param pipeline the job pipeline
	 * @param configuration the job config
	 * @return true or false result
	 */
	public static boolean validatePipelineSocketEnable(Pipeline pipeline, Configuration configuration) {
		if (pipeline instanceof StreamGraph) {
			StreamGraph streamGraph = (StreamGraph) pipeline;
			Collection<Integer> sinkIds = streamGraph.getSinkIDs();
			// For socket endpoint, it should manage the pipeline executor and share it between jobs.
			return sinkIds != null && sinkIds.size() == 1 &&
				streamGraph.getStreamNode(sinkIds.iterator().next()).getOperatorFactory() instanceof TaskPushSinkOperatorFactory &&
				configuration.getBoolean(ClusterOptions.CLUSTER_SOCKET_ENDPOINT_ENABLE);
		}
		return false;
	}
}
