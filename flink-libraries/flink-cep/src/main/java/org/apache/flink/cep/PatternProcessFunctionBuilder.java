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

package org.apache.flink.cep;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.adaptors.MultiplePatternFlatSelectAdapter;
import org.apache.flink.cep.functions.adaptors.MultiplePatternSelectAdapter;
import org.apache.flink.cep.functions.adaptors.MultiplePatternTimeoutFlatSelectAdapter;
import org.apache.flink.cep.functions.adaptors.MultiplePatternTimeoutSelectAdapter;
import org.apache.flink.cep.functions.adaptors.PatternFlatSelectAdapter;
import org.apache.flink.cep.functions.adaptors.PatternSelectAdapter;
import org.apache.flink.cep.functions.adaptors.PatternTimeoutFlatSelectAdapter;
import org.apache.flink.cep.functions.adaptors.PatternTimeoutSelectAdapter;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for adapting pre-1.8 functions like {@link PatternFlatSelectFunction}, {@link PatternFlatTimeoutFunction}
 * into {@link PatternProcessFunction}.
 */
@Internal
class PatternProcessFunctionBuilder {

	/**
	 * Starts constructing a {@link PatternProcessFunction} from a {@link PatternFlatSelectFunction} that
	 * emitted elements through {@link org.apache.flink.util.Collector}.
	 */
	static <IN, OUT> FlatSelectBuilder<IN, OUT> fromFlatSelect(final PatternFlatSelectFunction<IN, OUT> function) {
		return new FlatSelectBuilder<>(function);
	}

	static <IN, OUT> MultiplePatternFlatSelectBuilder<IN, OUT> fromFlatSelect(final MultiplePatternFlatSelectFunction<IN, OUT> function) {
		return new MultiplePatternFlatSelectBuilder<>(function);
	}

	/**
	 * Starts constructing a {@link PatternProcessFunction} from a {@link PatternSelectFunction} that
	 * emitted elements through return value.
	 */
	static <IN, OUT> SelectBuilder<IN, OUT> fromSelect(final PatternSelectFunction<IN, OUT> function) {
		return new SelectBuilder<>(function);
	}

	static <IN, OUT> MultiplePatternSelectBuilder<IN, OUT> fromSelect(final MultiplePatternSelectFunction<IN, OUT> function) {
		return new MultiplePatternSelectBuilder<>(function);
	}

	/**
	 * Wraps {@link PatternFlatSelectFunction} in a builder. The builder can construct a
	 * 	 * {@link PatternProcessFunction} adapter.
	 */
	static class FlatSelectBuilder<IN, OUT> {

		private final PatternFlatSelectFunction<IN, OUT> flatSelectFunction;

		FlatSelectBuilder(PatternFlatSelectFunction<IN, OUT> function) {
			this.flatSelectFunction = checkNotNull(function);
		}

		<TIMED_OUT> FlatTimeoutSelectBuilder<IN, OUT, TIMED_OUT> withTimeoutHandler(
				final OutputTag<TIMED_OUT> outputTag,
				final PatternFlatTimeoutFunction<IN, TIMED_OUT> timeoutHandler) {
			return new FlatTimeoutSelectBuilder<>(flatSelectFunction, timeoutHandler, outputTag);
		}

		PatternProcessFunction<IN, OUT> build() {
			return new PatternFlatSelectAdapter<>(flatSelectFunction);
		}
	}

	static class MultiplePatternFlatSelectBuilder<IN, OUT> {

		private final MultiplePatternFlatSelectFunction<IN, OUT> flatSelectFunction;

		MultiplePatternFlatSelectBuilder(MultiplePatternFlatSelectFunction<IN, OUT> function) {
			this.flatSelectFunction = checkNotNull(function);
		}

		<TIMED_OUT> MultiplePatternFlatTimeoutSelectBuilder<IN, OUT, TIMED_OUT> withTimeoutHandler(
				final OutputTag<TIMED_OUT> outputTag,
				final MultiplePatternFlatTimeoutFunction<IN, TIMED_OUT> timeoutHandler) {
			return new MultiplePatternFlatTimeoutSelectBuilder<>(flatSelectFunction, timeoutHandler, outputTag);
		}

		MultiplePatternProcessFunction<IN, OUT> build() {
			return new MultiplePatternFlatSelectAdapter<>(flatSelectFunction);
		}
	}

	/**
	 * Wraps {@link PatternFlatSelectFunction} and {@link PatternFlatTimeoutFunction} in a builder. The builder will
	 * create a {@link PatternProcessFunction} adapter that handles timed out partial matches as well.
	 */
	static class FlatTimeoutSelectBuilder<IN, OUT, TIMED_OUT> {
		private final PatternFlatSelectFunction<IN, OUT> flatSelectFunction;

		private final PatternFlatTimeoutFunction<IN, TIMED_OUT> timeoutHandler;
		private final OutputTag<TIMED_OUT> outputTag;

		FlatTimeoutSelectBuilder(
				final PatternFlatSelectFunction<IN, OUT> flatSelectFunction,
				final PatternFlatTimeoutFunction<IN, TIMED_OUT> timeoutHandler,
				final OutputTag<TIMED_OUT> outputTag) {
			this.flatSelectFunction = checkNotNull(flatSelectFunction);
			this.timeoutHandler = checkNotNull(timeoutHandler);
			this.outputTag = checkNotNull(outputTag);
		}

		PatternProcessFunction<IN, OUT> build() {
			return new PatternTimeoutFlatSelectAdapter<>(flatSelectFunction, timeoutHandler, outputTag);
		}
	}

	static class MultiplePatternFlatTimeoutSelectBuilder<IN, OUT, TIMED_OUT> {
		private final MultiplePatternFlatSelectFunction<IN, OUT> flatSelectFunction;

		private final MultiplePatternFlatTimeoutFunction<IN, TIMED_OUT> timeoutHandler;
		private final OutputTag<TIMED_OUT> outputTag;

		MultiplePatternFlatTimeoutSelectBuilder(
				final MultiplePatternFlatSelectFunction<IN, OUT> flatSelectFunction,
				final MultiplePatternFlatTimeoutFunction<IN, TIMED_OUT> timeoutHandler,
				final OutputTag<TIMED_OUT> outputTag) {
			this.flatSelectFunction = checkNotNull(flatSelectFunction);
			this.timeoutHandler = checkNotNull(timeoutHandler);
			this.outputTag = checkNotNull(outputTag);
		}

		MultiplePatternProcessFunction<IN, OUT> build() {
			return new MultiplePatternTimeoutFlatSelectAdapter<>(flatSelectFunction, timeoutHandler, outputTag);
		}
	}

	/**
	 * Wraps {@link PatternSelectFunction} in a builder. The builder can construct a
	 * {@link PatternProcessFunction} adapter.
	 */
	static class SelectBuilder<IN, OUT> {

		private final PatternSelectFunction<IN, OUT> selectFunction;

		SelectBuilder(PatternSelectFunction<IN, OUT> function) {
			this.selectFunction = checkNotNull(function);
		}

		<TIMED_OUT> TimeoutSelectBuilder<IN, OUT, TIMED_OUT> withTimeoutHandler(
				final OutputTag<TIMED_OUT> outputTag,
				final PatternTimeoutFunction<IN, TIMED_OUT> timeoutHandler) {
			return new TimeoutSelectBuilder<>(selectFunction, timeoutHandler, outputTag);
		}

		PatternProcessFunction<IN, OUT> build() {
			return new PatternSelectAdapter<>(selectFunction);
		}
	}

	static class MultiplePatternSelectBuilder<IN, OUT> {

		private final MultiplePatternSelectFunction<IN, OUT> selectFunction;

		MultiplePatternSelectBuilder(MultiplePatternSelectFunction<IN, OUT> function) {
			this.selectFunction = checkNotNull(function);
		}

		<TIMED_OUT> MultiplePatternTimeoutSelectBuilder<IN, OUT, TIMED_OUT> withTimeoutHandler(
				final OutputTag<TIMED_OUT> outputTag,
				final MultiplePatternTimeoutFunction<IN, TIMED_OUT> timeoutHandler) {
			return new MultiplePatternTimeoutSelectBuilder<>(selectFunction, timeoutHandler, outputTag);
		}

		MultiplePatternProcessFunction<IN, OUT> build() {
			return new MultiplePatternSelectAdapter<>(selectFunction);
		}
	}


	/**
	 * Wraps {@link PatternSelectFunction} and {@link PatternTimeoutFunction} in a builder. The builder will create a
	 * {@link PatternProcessFunction} adapter that handles timed out partial matches as well.
	 */
	static class TimeoutSelectBuilder<IN, OUT, TIMED_OUT> {
		private final PatternSelectFunction<IN, OUT> selectFunction;

		private final PatternTimeoutFunction<IN, TIMED_OUT> timeoutHandler;
		private final OutputTag<TIMED_OUT> outputTag;

		TimeoutSelectBuilder(
				final PatternSelectFunction<IN, OUT> flatSelectFunction,
				final PatternTimeoutFunction<IN, TIMED_OUT> timeoutHandler,
				final OutputTag<TIMED_OUT> outputTag) {
			this.selectFunction = checkNotNull(flatSelectFunction);
			this.timeoutHandler = checkNotNull(timeoutHandler);
			this.outputTag = checkNotNull(outputTag);
		}

		PatternProcessFunction<IN, OUT> build() {
			return new PatternTimeoutSelectAdapter<>(selectFunction, timeoutHandler, outputTag);
		}
	}

	static class MultiplePatternTimeoutSelectBuilder<IN, OUT, TIMED_OUT> {
		private final MultiplePatternSelectFunction<IN, OUT> selectFunction;

		private final MultiplePatternTimeoutFunction<IN, TIMED_OUT> timeoutHandler;
		private final OutputTag<TIMED_OUT> outputTag;

		MultiplePatternTimeoutSelectBuilder(
				final MultiplePatternSelectFunction<IN, OUT> flatSelectFunction,
				final MultiplePatternTimeoutFunction<IN, TIMED_OUT> timeoutHandler,
				final OutputTag<TIMED_OUT> outputTag) {
			this.selectFunction = checkNotNull(flatSelectFunction);
			this.timeoutHandler = checkNotNull(timeoutHandler);
			this.outputTag = checkNotNull(outputTag);
		}

		MultiplePatternProcessFunction<IN, OUT> build() {
			return new MultiplePatternTimeoutSelectAdapter<>(selectFunction, timeoutHandler, outputTag);
		}
	}
}
