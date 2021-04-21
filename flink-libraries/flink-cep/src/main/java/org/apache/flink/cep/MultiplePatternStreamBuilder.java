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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.functions.timestamps.CepTimestampExtractor;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.operator.CoCepOperator;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.parser.CepEventParserFactory;
import org.apache.flink.cep.pattern.parser.PatternConverter;
import org.apache.flink.cep.pattern.parser.PojoStreamToPatternStreamConverter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * @param <IN>
 */
public class MultiplePatternStreamBuilder<IN> {
	private static final Logger LOG = LoggerFactory.getLogger(MultiplePatternStreamBuilder.class);

	private final DataStream<IN> inputStream;

	@Nullable
	private DataStream<Pattern<IN, IN>> patternDataStream;

	@Nullable
	private DataStream<String> patternJsonStream;

	@Nullable
	private final CepEventParserFactory cepEventParserFactory;

	private final EventComparator<IN> comparator;

	@Nullable
	private CepTimestampExtractor timestampExtractor;

	/**
	 * Side output {@code OutputTag} for late data.
	 * If no tag is set late data will be simply dropped.
	 */
	private final OutputTag<IN> lateDataOutputTag;

	private final List<Pattern<IN, IN>> initialPatterns;

	private final Map<String, String> properties;

	private MultiplePatternStreamBuilder(
			final DataStream<IN> inputStream,
			@Nullable final DataStream<Pattern<IN, IN>> patternDataStream,
			@Nullable final DataStream<String> patternJsonStream,
			@Nullable final EventComparator<IN> comparator,
			@Nullable final OutputTag<IN> lateDataOutputTag,
			@Nullable final CepEventParserFactory cepEventParserFactory,
			@Nullable final CepTimestampExtractor timestampExtractor,
			final List<Pattern<IN, IN>> initialPatterns,
			final Map<String, String> properties) {
		Preconditions.checkArgument(patternDataStream != null || patternJsonStream != null, "none streams for pattern.");

		if (patternDataStream != null) {
			Preconditions.checkArgument(patternJsonStream == null);
		}

		if (patternJsonStream != null) {
			Preconditions.checkArgument(cepEventParserFactory != null);
		}

		this.inputStream = checkNotNull(inputStream);
		this.patternDataStream = patternDataStream;
		this.patternJsonStream = patternJsonStream;
		this.comparator = comparator;
		this.lateDataOutputTag = lateDataOutputTag;
		this.cepEventParserFactory = cepEventParserFactory;
		this.initialPatterns = initialPatterns;
		this.properties = properties;
		this.timestampExtractor = timestampExtractor;
	}

	TypeInformation<IN> getInputType() {
		return inputStream.getType();
	}

	/**
	 * Invokes the {@link org.apache.flink.api.java.ClosureCleaner}
	 * on the given function if closure cleaning is enabled in the {@link ExecutionConfig}.
	 *
	 * @return The cleaned Function
	 */
	<F> F clean(F f) {
		return inputStream.getExecutionEnvironment().clean(f);
	}

	MultiplePatternStreamBuilder<IN> withProperties(final Map<String, String> properties) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, timestampExtractor, initialPatterns, checkNotNull(properties));
	}

	MultiplePatternStreamBuilder<IN> withComparator(final EventComparator<IN> comparator) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternJsonStream, checkNotNull(comparator), lateDataOutputTag, cepEventParserFactory, timestampExtractor, initialPatterns, properties);
	}

	MultiplePatternStreamBuilder<IN> withLateDataOutputTag(final OutputTag<IN> lateDataOutputTag) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternJsonStream, comparator, checkNotNull(lateDataOutputTag), cepEventParserFactory, timestampExtractor, initialPatterns, properties);
	}

	MultiplePatternStreamBuilder<IN> withInitialPatternJsons(List<String> jsons) {
		ObjectMapper objectMapper = new ObjectMapper();
		List<Pattern<IN, IN>> patterns = new ArrayList<>();
		for (String json : jsons) {
			try {
				Optional<Pattern<IN, IN>> patternOpt = PatternConverter.buildPattern(objectMapper, json, cepEventParserFactory.create());
				if (patternOpt.isPresent()) {
					Pattern<IN, IN> pattern = patternOpt.get();
					if (!pattern.isDisabled()) {
						patterns.add(pattern);
					} else {
						LOG.warn("Fail to initialized with disabled pattern.");
					}
				} else {
					LOG.error("Fail to parse initial pattern {}.", json);
				}
			} catch (IOException e) {
				LOG.error("Fail to parse initial pattern {}.", json);
			}
		}
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, timestampExtractor, checkNotNull(patterns), properties);
	}

	MultiplePatternStreamBuilder<IN> withInitialPatterns(List<Pattern<IN, IN>> patterns) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, timestampExtractor, checkNotNull(patterns), properties);
	}

	MultiplePatternStreamBuilder<IN> withTimestampExtractor(CepTimestampExtractor<IN> timestampExtractor) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, checkNotNull(timestampExtractor), initialPatterns, properties);
	}

	<OUT, K> SingleOutputStreamOperator<OUT> build(
			final TypeInformation<OUT> outTypeInfo,
			final MultiplePatternProcessFunction<IN, OUT> processFunction) {

		checkNotNull(outTypeInfo);
		checkNotNull(processFunction);

		if (patternDataStream != null) {
			return buildTwoInputStream(outTypeInfo, processFunction);
		} else if (patternJsonStream != null) {
			// convert json stream to pattern data stream
			this.patternDataStream = PojoStreamToPatternStreamConverter.convert(patternJsonStream, cepEventParserFactory);
			return buildTwoInputStream(outTypeInfo, processFunction);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	private <OUT, K> SingleOutputStreamOperator<OUT> buildTwoInputStream(
			final TypeInformation<OUT> outTypeInfo,
			final MultiplePatternProcessFunction<IN, OUT> processFunction) {
		Preconditions.checkState(patternDataStream != null, "cannot support dynamic update without pattern stream.");

		final TypeSerializer<IN> inputSerializer = inputStream.getType().createSerializer(inputStream.getExecutionConfig());
		final boolean isProcessingTime = inputStream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

		final CoCepOperator<IN, K, OUT> operator = new CoCepOperator<>(
				inputSerializer,
				isProcessingTime,
				comparator,
				AfterMatchSkipStrategy.skipPastLastEvent(),
				processFunction,
				lateDataOutputTag,
				timestampExtractor,
				initialPatterns,
				properties);

		if (!(inputStream instanceof KeyedStream)) {
			throw new UnsupportedOperationException();
		}

		final KeyedStream<IN, K> keyedStream = (KeyedStream<IN, K>) inputStream;

		TwoInputTransformation<IN, Pattern<IN, IN>, OUT> transform = new TwoInputTransformation<>(
				inputStream.getTransformation(),
				patternDataStream.broadcast().getTransformation(),
				"CoCepOperator",
				operator,
				outTypeInfo,
				inputStream.getExecutionEnvironment().getParallelism());

		TypeInformation<?> keyType1 = keyedStream.getKeyType();
		transform.setUid("cep-co-operator");
		transform.setStateKeySelectors(keyedStream.getKeySelector(), null);
		transform.setStateKeyType(keyType1);

		StreamExecutionEnvironment environment = inputStream.getExecutionEnvironment();
		SingleOutputStreamOperator<OUT> returnStream = new SingleOutputStreamOperator(environment, transform);

		environment.addOperator(transform);

		return returnStream;
	}

	// ---------------------------------------- factory-like methods ---------------------------------------- //

	static <IN> MultiplePatternStreamBuilder<IN> forStreamAndPattern(final DataStream<IN> inputStream, final Pattern<IN, ?> pattern) {
		return new MultiplePatternStreamBuilder<>(inputStream, null, null, null, null, null, null, Collections.emptyList(), new HashMap<>());
	}

	static <IN> MultiplePatternStreamBuilder<IN> forStreamAndPatternDataStream(final DataStream<IN> inputStream, final DataStream<Pattern<IN, IN>> patternDataStream) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, null, null, null, null, null, Collections.emptyList(), new HashMap<>());
	}

	static <IN> MultiplePatternStreamBuilder<IN> forStreamAndPatternJsonStream(final DataStream<IN> inputStream, final DataStream<String> patternJsonStream, final CepEventParserFactory factory) {
		return new MultiplePatternStreamBuilder<>(inputStream, null, patternJsonStream, null, null, factory, null, Collections.emptyList(), new HashMap<>());
	}
}
