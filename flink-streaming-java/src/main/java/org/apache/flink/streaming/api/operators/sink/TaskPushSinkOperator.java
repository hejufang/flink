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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.socket.TaskJobResultGateway;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * A {@link StreamSink} that pushes the results of this tasks with {@link TaskJobResultGateway}.
 *
 * @param <IN> type of results to be written into the sink.
 */
public class TaskPushSinkOperator<IN> extends StreamSink<IN> implements OperatorEventHandler {
	private final TaskPushSinkFunction<IN> sinkFunction;

	public TaskPushSinkOperator(
			TypeSerializer<IN> serializer,
			int maxResultsPerBatch) {
		super(new TaskPushSinkFunction<>(serializer, maxResultsPerBatch));
		this.sinkFunction = (TaskPushSinkFunction<IN>) getUserFunction();
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Object>> output) {
		super.setup(containingTask, config, output);
		sinkFunction.setup(containingTask);
	}

	@Override
	public void handleOperatorEvent(OperatorEvent evt) {
		// nothing to handle
	}
}
