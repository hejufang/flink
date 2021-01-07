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

package org.apache.flink.cep.functions.adaptors;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.MultiplePatternSelectFunction;
import org.apache.flink.cep.MultiplePatternTimeoutFunction;
import org.apache.flink.cep.functions.MultiplePatternTimedOutPartialMatchHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * @param <IN>
 * @param <OUT>
 * @param <T>
 */
public class MultiplePatternTimeoutSelectAdapter<IN, OUT, T>
		extends MultiplePatternSelectAdapter<IN, OUT>
		implements MultiplePatternTimedOutPartialMatchHandler<IN> {

	private final MultiplePatternTimeoutFunction<IN, T> timeoutFunction;
	private final OutputTag<T> timedOutPartialMatchesTag;

	public MultiplePatternTimeoutSelectAdapter(
			final MultiplePatternSelectFunction<IN, OUT> selectFunction,
			final MultiplePatternTimeoutFunction<IN, T> timeoutFunction,
			final OutputTag<T> timedOutPartialMatchesTag) {
		super(selectFunction);
		this.timeoutFunction = checkNotNull(timeoutFunction);
		this.timedOutPartialMatchesTag = checkNotNull(timedOutPartialMatchesTag);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		FunctionUtils.setFunctionRuntimeContext(timeoutFunction, getRuntimeContext());
		FunctionUtils.openFunction(timeoutFunction, parameters);
	}

	@Override
	public void close() throws Exception {
		super.close();
		FunctionUtils.closeFunction(timeoutFunction);
	}

	@Override
	public void processTimedOutMatch(
			final Tuple2<String, Map<String, List<IN>>> match,
			final Context ctx) throws Exception {

		final T timedOutPatternResult = timeoutFunction.timeout(match, ctx.timestamp());

		ctx.output(timedOutPartialMatchesTag, timedOutPatternResult);
	}
}
