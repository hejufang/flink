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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.socket.TaskJobResultGateway;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Task push batch result to job manager.
 * @param <IN> the result type
 */
public class TaskPushSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction, CheckpointListener {
	private final TypeSerializer<IN> serializer;
	private final int maxResultsPerBatch;
	private final List<IN> batchResults;

	private transient JobID jobId;
	private transient TaskJobResultGateway taskJobResultGateway;

	public TaskPushSinkFunction(TypeSerializer<IN> serializer, int maxResultsPerBatch) {
		this.serializer = serializer;
		this.maxResultsPerBatch = maxResultsPerBatch;
		this.batchResults = new ArrayList<>(this.maxResultsPerBatch);
	}

	public void setup(StreamTask<?, ?> containingTask) {
		this.jobId = containingTask.getEnvironment().getJobID();
		this.taskJobResultGateway = containingTask.getEnvironment().getTaskResultGateway();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {

	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {

	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {

	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		this.batchResults.add(value);
		if (this.batchResults.size() >= this.maxResultsPerBatch) {
			sendTaskResult(ResultStatus.PARTIAL);
		}
	}

	@Override
	public void close() throws Exception {
		sendTaskResult(ResultStatus.COMPLETE);
		super.close();
	}

	private void sendTaskResult(ResultStatus resultStatus) throws java.io.IOException {
		if (!this.batchResults.isEmpty()) {
			ListSerializer<IN> listSerializer = new ListSerializer<>(serializer);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
			listSerializer.serialize(this.batchResults, wrapper);
			taskJobResultGateway.sendResult(jobId, baos.toByteArray(), resultStatus);
			this.batchResults.clear();
		} else {
			taskJobResultGateway.sendResult(jobId, null, resultStatus);
		}
	}
}
