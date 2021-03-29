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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.handler.util.SourceMetaMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Request handler for the job vertex details.
 */
public class JobVertexDetailsHandler extends AbstractExecutionGraphHandler<JobVertexDetailsInfo, JobVertexMessageParameters> implements JsonArchivist {

	private static final Logger LOG = LoggerFactory.getLogger(JobVertexDetailsHandler.class);


	private static final int SIMPLE_NAME_SIZE = 20;
	private static final String INPUT_OUTPUT_ALL_DESCRIBE = "all";

	private final MetricFetcher metricFetcher;

	public JobVertexDetailsHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobVertexDetailsInfo, JobVertexMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			MetricFetcher metricFetcher) {
		super(
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
		this.metricFetcher = metricFetcher;
	}

	@Override
	protected JobVertexDetailsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			AccessExecutionGraph executionGraph) throws NotFoundException {
		JobID jobID = request.getPathParameter(JobIDPathParameter.class);
		JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
		AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

		if (jobVertex == null) {
			throw new NotFoundException(String.format("JobVertex %s not found", jobVertexID));
		}

		return createJobVertexDetailsInfo(jobVertex, jobID, metricFetcher);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		Collection<? extends AccessExecutionJobVertex> vertices = graph.getAllVertices().values();
		List<ArchivedJson> archive = new ArrayList<>(vertices.size());
		for (AccessExecutionJobVertex task : vertices) {
			ResponseBody json = createJobVertexDetailsInfo(task, graph.getJobID(), null);
			String path = getMessageHeaders().getTargetRestEndpointURL()
				.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString())
				.replace(':' + JobVertexIdPathParameter.KEY, task.getJobVertexId().toString());
			archive.add(new ArchivedJson(path, json));
		}
		return archive;
	}

	private static JobVertexDetailsInfo createJobVertexDetailsInfo(AccessExecutionJobVertex jobVertex, JobID jobID, @Nullable MetricFetcher metricFetcher) {
		List<JobVertexDetailsInfo.VertexTaskDetail> subtasks = new ArrayList<>();
		final long now = System.currentTimeMillis();
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			// add main execution first
			subtasks.add(generateVertexTaskDetail(vertex.getMainExecution(), now, jobID, jobVertex.getJobVertexId(), metricFetcher, vertex));

			// add copy executions
			for (AccessExecution exec : vertex.getCopyExecutions()) {
				subtasks.add(generateVertexTaskDetail(exec, now, jobID, jobVertex.getJobVertexId(), metricFetcher, vertex));
			}
		}

		return new JobVertexDetailsInfo(
				jobVertex.getJobVertexId(),
				jobVertex.getName(),
				jobVertex.getParallelism(),
				now,
				subtasks);
	}

	private static JobVertexDetailsInfo.VertexTaskDetail generateVertexTaskDetail(AccessExecution exec, long now, JobID jobID, JobVertexID jobVertex, MetricFetcher metricFetcher, AccessExecutionVertex vertex){
		final ExecutionState status = exec.getState();

		TaskManagerLocation location = exec.getAssignedResourceLocation();
		String locationString = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();

		long startTime = exec.getStateTimestamp(ExecutionState.DEPLOYING);
		if (startTime == 0) {
			startTime = -1;
		}
		long endTime = status.isTerminal() ? exec.getStateTimestamp(status) : -1;
		long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

		MutableIOMetrics counts = new MutableIOMetrics();
		counts.addIOMetrics(
				exec,
				metricFetcher,
				jobID.toString(),
				jobVertex.toString());

		IOMetricsInfo ioMetricsInfo = new IOMetricsInfo(
					counts.getNumBytesIn(),
					counts.isNumBytesInComplete(),
					counts.getNumBytesOut(),
					counts.isNumBytesOutComplete(),
					counts.getNumRecordsIn(),
					counts.isNumRecordsInComplete(),
					counts.getNumRecordsOut(),
					counts.isNumRecordsOutComplete());

		final SourceMetaMetrics sourceMeta = new SourceMetaMetrics();
		sourceMeta.addMeta(exec, metricFetcher, jobID.toString(), jobVertex.toString());

		return new JobVertexDetailsInfo.VertexTaskDetail(
				vertex.getParallelSubtaskIndex(),
				status,
				exec.getAttemptNumber(),
				locationString,
				startTime,
				endTime,
				duration,
				ioMetricsInfo,
				sourceMeta.getMetaInfo().calculatePartitions(),
				getSubTaskStr(vertex.getInputSubTasks()),
				getSubTaskStr(vertex.getOutputSubTasks())
			);
	}

	private static String getSubTaskStr(Map<String, List<Integer>> subTasks) {
		try {
			if (MapUtils.isEmpty(subTasks)) {
				return StringUtils.EMPTY;
			}

			Set<Map.Entry<String, List<Integer>>> entries = subTasks.entrySet();
			boolean keyNameVisible = entries.size() != 1;

			return entries.stream().map(entry -> {
				String subTaskIds;
				List<Integer> value = entry.getValue();
				if (CollectionUtils.isEmpty(value)) {
					subTaskIds = INPUT_OUTPUT_ALL_DESCRIBE;
				} else {
					//Match with Web-UI ID
					subTaskIds = value.stream().map(i -> String.valueOf(i + 1)).collect(Collectors.joining("-"));
				}
				return (keyNameVisible ? getUpstreamTaskSimpleName(entry.getKey()) + ":" : StringUtils.EMPTY) + subTaskIds;
			}).collect(Collectors.joining(";"));
		} catch (Exception e) {
			LOG.error("get subtask resource fail", e);
		}
		return StringUtils.EMPTY;
	}

	private static String getUpstreamTaskSimpleName(String name) {
		String[] nameSplit = StringUtils.split(name, "->");
		StringBuilder nameBuilder = new StringBuilder();
		for (int i = 0; i < nameSplit.length; i++) {
			String subName = nameSplit[i].trim();
			if (StringUtils.isBlank(subName)) {
				continue;
			}
			if (i != nameSplit.length - 1) {
				nameBuilder.append(subName.charAt(0)).append("_");
			} else {
				nameBuilder.append(subName);
			}
		}
		if (nameBuilder.length() > SIMPLE_NAME_SIZE) {
			return nameBuilder.substring(0, SIMPLE_NAME_SIZE);
		}
		return nameBuilder.toString();
	}

}
