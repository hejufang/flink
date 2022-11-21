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

package org.apache.flink.api.java.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.CollectionExecutor;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link PipelineExecutor} for serial, local, collection-based executions of Flink programs.
 */
@Internal
public class CollectionPipelineExecutor implements PipelineExecutor {

	public static final String NAME = "collection";

	@Override
	public CompletableFuture<JobClient> execute(
			Pipeline pipeline,
			Configuration configuration) throws Exception {
		Plan plan = (Plan) pipeline;
		CollectionExecutor exec = new CollectionExecutor(plan.getExecutionConfig());
		JobExecutionResult result = exec.execute(plan);

		return CompletableFuture.completedFuture(new JobClient() {
			@Override
			public JobID getJobID() {
				return new JobID();
			}

			@Override
			public CompletableFuture<JobStatus> getJobStatus() {
				return CompletableFuture.completedFuture(JobStatus.FINISHED);
			}

			@Override
			public CompletableFuture<Void> cancel() {
				return CompletableFuture.completedFuture(null);
			}

			@Override
			public CompletableFuture<String> stopWithSavepoint(
					boolean advanceToEndOfEventTime,
					@Nullable String savepointDirectory) {
				return CompletableFuture.completedFuture("null");
			}

			@Override
			public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
				return CompletableFuture.completedFuture("null");
			}

			@Override
			public CompletableFuture<Map<String, Object>> getAccumulators(ClassLoader classLoader) {
				return CompletableFuture.completedFuture(Collections.emptyMap());
			}

			@Override
			public CompletableFuture<JobExecutionResult> getJobExecutionResult(ClassLoader userClassloader) {
				return CompletableFuture.completedFuture(result);
			}

			@Override
			public <T> CloseableIterator<T> getJobResultIterator(ClassLoader userClassLoader) {
				throw new UnsupportedOperationException();
			}
		});
	}
}