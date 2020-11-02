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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Handler returning the details for the specified job.
 */
public class JobDetailsHandler extends AbstractExecutionGraphHandler<JobDetailsInfo, JobMessageParameters> implements JsonArchivist {
	private static final Logger LOGGER = LoggerFactory.getLogger(JobDetailsHandler.class);

	private final MetricFetcher metricFetcher;
	private final Configuration configuration;

	public JobDetailsHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobDetailsInfo, JobMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			MetricFetcher metricFetcher,
			Configuration configuration) {
		super(
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);

		this.metricFetcher = Preconditions.checkNotNull(metricFetcher);
		this.configuration = Preconditions.checkNotNull(configuration);
	}

	@Override
	protected JobDetailsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, JobMessageParameters> request,
			AccessExecutionGraph executionGraph) throws RestHandlerException {
		return createJobDetailsInfo(executionGraph, metricFetcher, configuration);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		ResponseBody json = createJobDetailsInfo(graph, null, null);
		String path = getMessageHeaders().getTargetRestEndpointURL()
			.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());
		return Collections.singleton(new ArchivedJson(path, json));
	}

	private static JobDetailsInfo createJobDetailsInfo(AccessExecutionGraph executionGraph,
			@Nullable MetricFetcher metricFetcher, @Nullable Configuration configuration) {
		final long now = System.currentTimeMillis();
		final long startTime = executionGraph.getStatusTimestamp(JobStatus.CREATED);
		final long endTime = executionGraph.getState().isGloballyTerminalState() ?
			executionGraph.getStatusTimestamp(executionGraph.getState()) : -1L;
		final long duration = (endTime > 0L ? endTime : now) - startTime;

		final Map<JobStatus, Long> timestamps = new HashMap<>(JobStatus.values().length);

		for (JobStatus jobStatus : JobStatus.values()) {
			timestamps.put(jobStatus, executionGraph.getStatusTimestamp(jobStatus));
		}

		Collection<JobDetailsInfo.JobVertexDetailsInfo> jobVertexInfos = new ArrayList<>(executionGraph.getAllVertices().size());
		int[] jobVerticesPerState = new int[ExecutionState.values().length];

		for (AccessExecutionJobVertex accessExecutionJobVertex : executionGraph.getVerticesTopologically()) {
			final JobDetailsInfo.JobVertexDetailsInfo vertexDetailsInfo = createJobVertexDetailsInfo(
				accessExecutionJobVertex,
				now,
				executionGraph.getJobID(),
				metricFetcher);

			jobVertexInfos.add(vertexDetailsInfo);
			jobVerticesPerState[vertexDetailsInfo.getExecutionState().ordinal()]++;
		}

		Map<ExecutionState, Integer> jobVerticesPerStateMap = new HashMap<>(ExecutionState.values().length);

		for (ExecutionState executionState : ExecutionState.values()) {
			jobVerticesPerStateMap.put(executionState, jobVerticesPerState[executionState.ordinal()]);
		}

		String metric = WebMonitorUtils.createMetricUrl(configuration);
		String dtop = WebMonitorUtils.createDtopUrl(configuration);
		LOGGER.info("metric = {}, dtop = {}", metric, dtop);

		return new JobDetailsInfo(
			executionGraph.getJobID(),
			executionGraph.getJobName(),
			executionGraph.isStoppable(),
			executionGraph.getState(),
			startTime,
			endTime,
			duration,
			now,
			timestamps,
			jobVertexInfos,
			jobVerticesPerStateMap,
			executionGraph.getJsonPlan(),
			metric,
			dtop);
	}

	private static JobDetailsInfo.JobVertexDetailsInfo createJobVertexDetailsInfo(
			AccessExecutionJobVertex ejv,
			long now,
			JobID jobId,
			MetricFetcher metricFetcher) {
		int[] tasksPerState = new int[ExecutionState.values().length];
		long startTime = Long.MAX_VALUE;
		long endTime = 0;
		boolean allFinished = true;

		for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
			ExecutionState state = vertex.getExecutionState();
			for (final AccessExecution exec : vertex.getCopyExecutions()) {
				if (exec.getState().isFinished()) {
					// 任意一个 Execution FINISHED 就算 FINISHED
					state = ExecutionState.FINISHED;
					break;
				}
			}
			tasksPerState[state.ordinal()]++;

			// take the earliest start time
			long started = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
			if (started > 0L) {
				startTime = Math.min(startTime, started);
			}

			allFinished &= state.isTerminal();
			endTime = Math.max(endTime, vertex.getStateTimestamp(state));
		}

		long duration;
		if (startTime < Long.MAX_VALUE) {
			if (allFinished) {
				duration = endTime - startTime;
			}
			else {
				endTime = -1L;
				duration = now - startTime;
			}
		}
		else {
			startTime = -1L;
			endTime = -1L;
			duration = -1L;
		}

		ExecutionState jobVertexState =
			ExecutionJobVertex.getAggregateJobVertexState(tasksPerState, ejv.getParallelism());

		Map<ExecutionState, Integer> tasksPerStateMap = new HashMap<>(tasksPerState.length);

		for (ExecutionState executionState : ExecutionState.values()) {
			tasksPerStateMap.put(executionState, tasksPerState[executionState.ordinal()]);
		}

		MutableIOMetrics counts = new MutableIOMetrics();

		boolean isCopy = false;
		for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
			counts.addIOMetrics(
				vertex.getCurrentExecutionAttempt(),
				metricFetcher,
				jobId.toString(),
				ejv.getJobVertexId().toString());

			if (vertex.getCopyExecutions().size() > 0) {
				isCopy = true;
				for (AccessExecution ae : vertex.getCopyExecutions()) {
					counts.addIOMetrics(
							ae,
							metricFetcher,
							jobId.toString(),
							ejv.getJobVertexId().toString());
				}
			}
		}

		final IOMetricsInfo jobVertexMetrics = new IOMetricsInfo(
			counts.getNumBytesIn(),
			counts.isNumBytesInComplete(),
			counts.getNumBytesOut(),
			counts.isNumBytesOutComplete(),
			counts.getNumRecordsIn(),
			counts.isNumRecordsInComplete(),
			counts.getNumRecordsOut(),
			counts.isNumRecordsOutComplete());

		return new JobDetailsInfo.JobVertexDetailsInfo(
			ejv.getJobVertexId(),
			ejv.getName(),
			ejv.getParallelism(),
			jobVertexState,
			startTime,
			endTime,
			duration,
			tasksPerStateMap,
			jobVertexMetrics,
			isCopy);
	}
}
