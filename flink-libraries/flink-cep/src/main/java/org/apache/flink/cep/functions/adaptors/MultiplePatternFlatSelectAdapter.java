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
import org.apache.flink.cep.MultiplePatternFlatSelectFunction;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * @param <IN>
 * @param <OUT>
 */
public class MultiplePatternFlatSelectAdapter<IN, OUT> extends MultiplePatternProcessFunction<IN, OUT> {

	private final MultiplePatternFlatSelectFunction<IN, OUT> flatSelectFunction;

	public MultiplePatternFlatSelectAdapter(final MultiplePatternFlatSelectFunction<IN, OUT> flatSelectFunction) {
		this.flatSelectFunction = checkNotNull(flatSelectFunction);
	}

	@Override
	public void open(final Configuration parameters) throws Exception {
		FunctionUtils.setFunctionRuntimeContext(flatSelectFunction, getRuntimeContext());
		FunctionUtils.openFunction(flatSelectFunction, parameters);
	}

	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(flatSelectFunction);
	}

	@Override
	public void processMatch(
			final Tuple2<String, Map<String, List<IN>>> match,
			final Context ctx,
			final Object key,
			final Collector<OUT> out) throws Exception {
		flatSelectFunction.flatSelect(match, out);
	}
}
