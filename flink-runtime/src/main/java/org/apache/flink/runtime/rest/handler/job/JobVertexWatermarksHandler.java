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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.metrics.MetricNames.IO_CURRENT_INPUT_WATERMARK;

/**
 * Handler that returns current watermarks metric for the specified job vertex.
 */
public class JobVertexWatermarksHandler extends AbstractExecutionGraphHandler<MetricCollectionResponseBody, JobVertexMessageParameters> {
	private MetricFetcher metricFetcher;

	public JobVertexWatermarksHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, MetricCollectionResponseBody, JobVertexMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			MetricFetcher metricFetcher) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);
		this.metricFetcher = Preconditions.checkNotNull(metricFetcher);
	}

	@Override
	protected MetricCollectionResponseBody handleRequest(
			HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			AccessExecutionGraph executionGraph) throws RestHandlerException {
		JobID jobID = request.getPathParameter(JobIDPathParameter.class);
		JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
		AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

		if (jobVertex == null) {
			throw new NotFoundException(String.format("JobVertex %s not found", jobVertexID));
		}

		metricFetcher.update();
		MetricStore.TaskMetricStore taskMetricStore = metricFetcher.getMetricStore()
			.getTaskMetricStore(jobID.toString(), jobVertexID.toString());

		int parallelism = jobVertex.getParallelism();
		List<String> metricNames = new ArrayList<>(parallelism);
		for (int i = 0; i < parallelism; ++i) {
			metricNames.add(i + "." + IO_CURRENT_INPUT_WATERMARK);
		}

		final List<Metric> metrics = new ArrayList<>(parallelism);
		for (final String requestedMetric : metricNames) {
			final String value = taskMetricStore.getMetric(requestedMetric, null);
			if (value != null) {
				metrics.add(new Metric(requestedMetric, value));
			}
		}

		return new MetricCollectionResponseBody(metrics);
	}
}
