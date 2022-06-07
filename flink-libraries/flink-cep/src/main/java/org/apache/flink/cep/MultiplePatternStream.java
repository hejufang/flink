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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.functions.EmptyPatternStreamFactory;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.functions.timestamps.CepTimestampExtractor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.parser.CepEventParserFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

import static org.apache.flink.cep.PatternProcessFunctionBuilder.fromFlatSelect;
import static org.apache.flink.cep.PatternProcessFunctionBuilder.fromSelect;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * @param <T>
 */
public class MultiplePatternStream<T> {

	private final MultiplePatternStreamBuilder<T> builder;

	private MultiplePatternStream(final MultiplePatternStreamBuilder<T> builder) {
		this.builder = checkNotNull(builder);
	}

	MultiplePatternStream(final DataStream<T> inputStream, final DataStream<?> patternDataStream) {
		this(MultiplePatternStreamBuilder.forStreamAndPatternDataStream(inputStream, patternDataStream));
	}

	MultiplePatternStream(final DataStream<T> inputStream, final DataStream<String> patternJsonStream, final CepEventParserFactory factory) {
		this(MultiplePatternStreamBuilder.forStreamAndPatternJsonStream(inputStream, patternJsonStream, factory));
	}

	MultiplePatternStream(final DataStream<T> inputStream, final Pattern<T, ?> pattern) {
		this(MultiplePatternStreamBuilder.forStreamAndPattern(inputStream, pattern));
	}

	MultiplePatternStream(final DataStream<T> inputStream, final List<Pattern<T, T>> patternList) {
		this(MultiplePatternStreamBuilder.forStreamAndPatternList(inputStream, patternList));
	}

	public MultiplePatternStream<T> withComparator(final EventComparator<T> comparator) {
		return new MultiplePatternStream<>(builder.withComparator(comparator));
	}

	public MultiplePatternStream<T> sideOutputLateData(OutputTag<T> lateDataOutputTag) {
		return new MultiplePatternStream<>(builder.withLateDataOutputTag(lateDataOutputTag));
	}

	public MultiplePatternStream<T> withInitialPatternJsons(List<String> jsons) {
		return new MultiplePatternStream<>(builder.withInitialPatternJsons(jsons));
	}

	public MultiplePatternStream<T> withInitialPatterns(List<Pattern<T, T>> patterns) {
		return new MultiplePatternStream<>(builder.withInitialPatterns(patterns));
	}

	public MultiplePatternStream<T> withTimestampExtractor(CepTimestampExtractor<T> timestampExtractor) {
		return new MultiplePatternStream<>(builder.withTimestampExtractor(timestampExtractor));
	}

	public MultiplePatternStream<T> withProperties(Map<String, String> properties) {
		return new MultiplePatternStream<>(builder.withProperties(properties));
	}

	public MultiplePatternStream<T> withEmptyPatternStreamFactory(EmptyPatternStreamFactory emptyPatternStreamFactory) {
		return new MultiplePatternStream<>(builder.withEmptyPatternStreamFactory(emptyPatternStreamFactory));
	}

	public <R> SingleOutputStreamOperator<R> process(final MultiplePatternProcessFunction<T, R> patternProcessFunction) {
		final TypeInformation<R> returnType = TypeExtractor.getUnaryOperatorReturnType(
				patternProcessFunction,
				MultiplePatternProcessFunction.class,
				0,
				1,
				TypeExtractor.NO_INDEX,
				builder.getInputType(),
				null,
				false);

		return process(patternProcessFunction, returnType);
	}

	public <R> SingleOutputStreamOperator<R> process(
			final MultiplePatternProcessFunction<T, R> patternProcessFunction,
			final TypeInformation<R> outTypeInfo) {

		return builder.build(
				outTypeInfo,
				builder.clean(patternProcessFunction));
	}

	public  <R> SingleOutputStreamOperator<R> process(final TypeInformation<R> outTypeInfo) {
		return builder.build(outTypeInfo);
	}

	public <R> SingleOutputStreamOperator<R> select(final MultiplePatternSelectFunction<T, R> patternSelectFunction) {
		// we have to extract the output type from the provided pattern selection function manually
		// because the TypeExtractor cannot do that if the method is wrapped in a MapFunction

		final TypeInformation<R> returnType = TypeExtractor.getUnaryOperatorReturnType(
				patternSelectFunction,
				PatternSelectFunction.class,
				0,
				1,
				TypeExtractor.NO_INDEX,
				builder.getInputType(),
				null,
				false);

		return select(patternSelectFunction, returnType);
	}

	public <R> SingleOutputStreamOperator<R> select(
			final MultiplePatternSelectFunction<T, R> patternSelectFunction,
			final TypeInformation<R> outTypeInfo) {

		final MultiplePatternProcessFunction<T, R> processFunction =
				fromSelect(builder.clean(patternSelectFunction)).build();

		return process(processFunction, outTypeInfo);
	}

	public <L, R> SingleOutputStreamOperator<R> select(
			final OutputTag<L> timedOutPartialMatchesTag,
			final MultiplePatternTimeoutFunction<T, L> patternTimeoutFunction,
			final MultiplePatternSelectFunction<T, R> patternSelectFunction) {

		final TypeInformation<R> rightTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
				patternSelectFunction,
				PatternSelectFunction.class,
				0,
				1,
				TypeExtractor.NO_INDEX,
				builder.getInputType(),
				null,
				false);

		return select(
				timedOutPartialMatchesTag,
				patternTimeoutFunction,
				rightTypeInfo,
				patternSelectFunction);
	}

	public <L, R> SingleOutputStreamOperator<R> select(
			final OutputTag<L> timedOutPartialMatchesTag,
			final MultiplePatternTimeoutFunction<T, L> patternTimeoutFunction,
			final TypeInformation<R> outTypeInfo,
			final MultiplePatternSelectFunction<T, R> patternSelectFunction) {

		final MultiplePatternProcessFunction<T, R> processFunction =
				fromSelect(builder.clean(patternSelectFunction))
						.withTimeoutHandler(timedOutPartialMatchesTag, builder.clean(patternTimeoutFunction))
						.build();

		return process(processFunction, outTypeInfo);
	}

	public <R> SingleOutputStreamOperator<R> flatSelect(final MultiplePatternFlatSelectFunction<T, R> patternFlatSelectFunction) {
		// we have to extract the output type from the provided pattern selection function manually
		// because the TypeExtractor cannot do that if the method is wrapped in a MapFunction

		final TypeInformation<R> outTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
				patternFlatSelectFunction,
				PatternFlatSelectFunction.class,
				0,
				1,
				new int[]{1, 0},
				builder.getInputType(),
				null,
				false);

		return flatSelect(patternFlatSelectFunction, outTypeInfo);
	}

	public <R> SingleOutputStreamOperator<R> flatSelect(
			final MultiplePatternFlatSelectFunction<T, R> patternFlatSelectFunction,
			final TypeInformation<R> outTypeInfo) {

		final MultiplePatternProcessFunction<T, R> processFunction =
				fromFlatSelect(builder.clean(patternFlatSelectFunction))
						.build();

		return process(processFunction, outTypeInfo);
	}

	public <L, R> SingleOutputStreamOperator<R> flatSelect(
			final OutputTag<L> timedOutPartialMatchesTag,
			final MultiplePatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
			final MultiplePatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

		final TypeInformation<R> rightTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
				patternFlatSelectFunction,
				PatternFlatSelectFunction.class,
				0,
				1,
				new int[]{1, 0},
				builder.getInputType(),
				null,
				false);

		return flatSelect(
				timedOutPartialMatchesTag,
				patternFlatTimeoutFunction,
				rightTypeInfo,
				patternFlatSelectFunction);
	}

	public <L, R> SingleOutputStreamOperator<R> flatSelect(
			final OutputTag<L> timedOutPartialMatchesTag,
			final MultiplePatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
			final TypeInformation<R> outTypeInfo,
			final MultiplePatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

		final MultiplePatternProcessFunction<T, R> processFunction =
				fromFlatSelect(builder.clean(patternFlatSelectFunction))
						.withTimeoutHandler(timedOutPartialMatchesTag, builder.clean(patternFlatTimeoutFunction))
						.build();

		return process(processFunction, outTypeInfo);
	}
}
