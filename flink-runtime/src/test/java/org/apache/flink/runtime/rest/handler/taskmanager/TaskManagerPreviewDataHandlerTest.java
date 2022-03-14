/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataHandler;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataJobIdParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataJobVertexIdParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataResponse;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Test for the {@link PreviewDataHandler}.
 */
public class TaskManagerPreviewDataHandlerTest extends TestLogger {

	private static final ResourceID EXPECTED_TASK_MANAGER_ID = ResourceID.generate();
	private TestingResourceManagerGateway resourceManagerGateway;
	private PreviewDataHandler previewDataHandler;
	private HandlerRequest<EmptyRequestBody, PreviewDataParameters> handlerRequest;

	@Before
	public void setUp() throws HandlerRequestException {
		resourceManagerGateway = new TestingResourceManagerGateway();
		previewDataHandler = new PreviewDataHandler(
			() -> CompletableFuture.completedFuture(null),
			TestingUtils.TIMEOUT(),
			Collections.emptyMap(),
			PreviewDataHeaders.getInstance(),
			() -> CompletableFuture.completedFuture(resourceManagerGateway));
		handlerRequest = createRequest(EXPECTED_TASK_MANAGER_ID);
	}

	@Test
	public void testGetTaskManagerLogsList() throws Exception {
		PreviewDataResponse expected = new PreviewDataResponse();
		expected.setTableResult(Arrays.asList("result1", "result2", "result3"));
		expected.setChangeLogResult(Arrays.asList("changelog1", "changelog2", "changelog3"));
		final TestingRestfulGateway testingRestfulGateway = new TestingRestfulGateway.Builder()
			.setRequestJobStatusFunction(
				jobId -> FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId))
			)
			.build();
		resourceManagerGateway.setRequestTaskManagerPreviewFunction(EXPECTED_TASK_MANAGER_ID -> CompletableFuture.completedFuture(expected));
		PreviewDataResponse actual = previewDataHandler.handleRequest(handlerRequest, testingRestfulGateway).get();
		ObjectMapper objectMapper = new ObjectMapper();
		String actualJson = objectMapper.writeValueAsString(actual);
		String expectedJson = objectMapper.writeValueAsString(expected);
		Assert.assertEquals(actualJson, expectedJson);
	}

	private static HandlerRequest<EmptyRequestBody, PreviewDataParameters> createRequest(ResourceID taskManagerId) throws HandlerRequestException {
		Map<String, String> pathParameters = new HashMap<>();
		pathParameters.put(TaskManagerIdPathParameter.KEY, taskManagerId.toString());
		Map<String, List<String>> queryParameters = new HashMap<>();

		queryParameters.put(PreviewDataJobIdParameter.KEY, Collections.singletonList("43ffdd60cb0c88da888ba55beb1f024d"));
		queryParameters.put(PreviewDataJobVertexIdParameter.KEY, Collections.singletonList("5c51e52cde5a1c4df827ddb38fbc8da9"));

		return new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new PreviewDataParameters(),
			pathParameters,
			queryParameters);
	}
}
