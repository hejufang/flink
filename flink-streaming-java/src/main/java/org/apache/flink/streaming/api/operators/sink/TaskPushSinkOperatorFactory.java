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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * The Factory class for {@link TaskPushSinkOperator}.
 */
public class TaskPushSinkOperatorFactory<IN> extends SimpleUdfStreamOperatorFactory<Object> {

	private static final long serialVersionUID = 1L;

	private static final int DEFAULT_MAX_RESULTS_PER_BATCH = 4096;

	private final TaskPushSinkOperator<IN> operator;

	public TaskPushSinkOperatorFactory(TypeSerializer<IN> serializer, int maxResultsPerBatch) {
		super(new TaskPushSinkOperator<>(serializer, maxResultsPerBatch));
		this.operator = (TaskPushSinkOperator<IN>) getOperator();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T extends StreamOperator<Object>> T createStreamOperator(StreamOperatorParameters<Object> parameters) {
		final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
		final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

		operator.setDebugLoggingConverter(converter);
		operator.setDebugLoggingLocation(location);
		operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
		eventDispatcher.registerEventHandler(operatorId, operator);

		return (T) operator;
	}
}
