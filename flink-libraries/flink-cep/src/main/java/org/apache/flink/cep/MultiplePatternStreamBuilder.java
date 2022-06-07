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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.cep.functions.EmptyPatternStreamFactory;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.functions.SimpleEmptyPatternStreamFactory;
import org.apache.flink.cep.functions.timestamps.CepTimestampExtractor;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.operator.CepKeyGenOperator;
import org.apache.flink.cep.operator.CoCepOperator;
import org.apache.flink.cep.operator.CoCepOperatorV2;
import org.apache.flink.cep.pattern.KeyedCepEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.PatternProcessor;
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
 * @param <IN>
 */
public class MultiplePatternStreamBuilder<IN> {
	private static final Logger LOG = LoggerFactory.getLogger(MultiplePatternStreamBuilder.class);

	private final DataStream<IN> inputStream;

	@Nullable
	private DataStream<Pattern<IN, IN>> patternDataStream;

	@Nullable
	private DataStream<PatternProcessor<IN, ?>> patternProcessorDataStream;

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

	private EmptyPatternStreamFactory emptyPatternStreamFactory;

	private MultiplePatternStreamBuilder(
		final DataStream<IN> inputStream,
		@Nullable final DataStream<Pattern<IN, IN>> patternDataStream,
		@Nullable final DataStream<PatternProcessor<IN, ?>> patternProcessorDataStream,
		@Nullable final DataStream<String> patternJsonStream,
		@Nullable final EventComparator<IN> comparator,
		@Nullable final OutputTag<IN> lateDataOutputTag,
		@Nullable final CepEventParserFactory cepEventParserFactory,
		@Nullable final CepTimestampExtractor timestampExtractor,
		final List<Pattern<IN, IN>> initialPatterns,
		final EmptyPatternStreamFactory emptyPatternStreamFactory,
		final Map<String, String> properties) {
		Preconditions.checkArgument(patternDataStream != null || patternProcessorDataStream != null || patternJsonStream != null || initialPatterns != null, "none streams for pattern.");

		if (patternDataStream != null) {
			Preconditions.checkArgument(patternProcessorDataStream == null);
			Preconditions.checkArgument(patternJsonStream == null);
		}

		if (patternJsonStream != null) {
			Preconditions.checkArgument(cepEventParserFactory != null);
		}

		if (patternJsonStream == null && patternProcessorDataStream == null && patternDataStream == null) {
			Preconditions.checkArgument(initialPatterns != null);
		}

		this.inputStream = checkNotNull(inputStream);
		this.patternDataStream = patternDataStream;
		this.patternProcessorDataStream = patternProcessorDataStream;
		this.patternJsonStream = patternJsonStream;
		this.comparator = comparator;
		this.lateDataOutputTag = lateDataOutputTag;
		this.cepEventParserFactory = cepEventParserFactory;
		this.initialPatterns = initialPatterns;
		this.properties = properties;
		this.timestampExtractor = timestampExtractor;
		this.emptyPatternStreamFactory = emptyPatternStreamFactory;
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
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternProcessorDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, timestampExtractor, initialPatterns, emptyPatternStreamFactory, checkNotNull(properties));
	}

	MultiplePatternStreamBuilder<IN> withComparator(final EventComparator<IN> comparator) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternProcessorDataStream, patternJsonStream, checkNotNull(comparator), lateDataOutputTag, cepEventParserFactory, timestampExtractor, initialPatterns, emptyPatternStreamFactory, properties);
	}

	MultiplePatternStreamBuilder<IN> withLateDataOutputTag(final OutputTag<IN> lateDataOutputTag) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternProcessorDataStream, patternJsonStream, comparator, checkNotNull(lateDataOutputTag), cepEventParserFactory, timestampExtractor, initialPatterns, emptyPatternStreamFactory, properties);
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
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternProcessorDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, timestampExtractor, checkNotNull(patterns), emptyPatternStreamFactory, properties);
	}

	MultiplePatternStreamBuilder<IN> withInitialPatterns(List<Pattern<IN, IN>> patterns) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternProcessorDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, timestampExtractor, checkNotNull(patterns), emptyPatternStreamFactory, properties);
	}

	MultiplePatternStreamBuilder<IN> withTimestampExtractor(CepTimestampExtractor<IN> timestampExtractor) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternProcessorDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, checkNotNull(timestampExtractor), initialPatterns, emptyPatternStreamFactory, properties);
	}

	MultiplePatternStreamBuilder<IN> withEmptyPatternStreamFactory(EmptyPatternStreamFactory emptyPatternStreamFactory) {
		return new MultiplePatternStreamBuilder<>(inputStream, patternDataStream, patternProcessorDataStream, patternJsonStream, comparator, lateDataOutputTag, cepEventParserFactory, timestampExtractor, initialPatterns, checkNotNull(emptyPatternStreamFactory), properties);
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
		} else if (initialPatterns != null) {
			this.patternDataStream = emptyPatternStreamFactory.createEmptyPatternStream();
			return buildTwoInputStream(outTypeInfo, processFunction);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	<OUT, K> SingleOutputStreamOperator<OUT> build(final TypeInformation<OUT> outTypeInfo) {
		if (patternProcessorDataStream != null){
			return buildTwoInputStreamV2(outTypeInfo);
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

		// enable the new Timer mechanism to use "PayLoad" feature in CoCepOperator
		inputStream.getExecutionEnvironment().getConfig().enableNewTimerMechanism();

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

		final KeyedStream keyedStream;

		if (!(inputStream instanceof KeyedStream)) {
			KeySelector<IN, Byte> keySelector = new NullByteKeySelector<>();
			keyedStream = inputStream.keyBy(keySelector);
		} else {
			keyedStream = (KeyedStream<IN, K>) inputStream;
		}

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

		if (!(inputStream instanceof KeyedStream)) {
			returnStream.forceNonParallel();
		}

		return returnStream;
	}

	/**
	 *
	 * @param outTypeInfo
	 * @param <OUT>
	 * @param <K>
	 * @return
	 */
	private <OUT, K> SingleOutputStreamOperator<OUT> buildTwoInputStreamV2(
		final TypeInformation<OUT> outTypeInfo) {
		Preconditions.checkState(patternProcessorDataStream != null, "cannot support dynamic update without patternProcessor stream.");

		final TypeSerializer<IN> inputSerializer = inputStream.getType().createSerializer(inputStream.getExecutionConfig());
		final TypeSerializer<KeyedCepEvent<IN>> keyedCepEventSerializer = TypeInformation.of(new TypeHint<KeyedCepEvent<IN>>(){}).createSerializer(inputStream.getExecutionConfig());
		final boolean isProcessingTime = inputStream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

		// enable the new Timer mechanism to use "PayLoad" feature in CoCepOperator
		inputStream.getExecutionEnvironment().getConfig().enableNewTimerMechanism();

		final CepKeyGenOperator cepKeyGenOperator = new CepKeyGenOperator<>();

		final CoCepOperatorV2 operator = new CoCepOperatorV2<>(
			inputSerializer,
			keyedCepEventSerializer,
			isProcessingTime,
			AfterMatchSkipStrategy.skipPastLastEvent(),
			lateDataOutputTag,
			timestampExtractor,
			properties);

		TwoInputTransformation<IN, PatternProcessor<IN, ?>, KeyedCepEvent<IN>> keyGenTransform = new TwoInputTransformation<>(
			inputStream.getTransformation(),
			patternProcessorDataStream.broadcast().getTransformation(),
			"CepKeyGenOperator",
			cepKeyGenOperator,
			outTypeInfo,
			inputStream.getExecutionEnvironment().getParallelism());
		keyGenTransform.setUid("cep-keygen-operator");
		StreamExecutionEnvironment environment = inputStream.getExecutionEnvironment();
		// Keyed Stream
		KeyedStream<KeyedCepEvent<IN>, Object> keyedStream = new SingleOutputStreamOperator(environment, keyGenTransform).keyBy((KeySelector) value -> ((KeyedCepEvent<IN>) value).getKey());

		TwoInputTransformation<KeyedCepEvent<IN>, PatternProcessor<IN, ?>, Object> transform = new TwoInputTransformation<>(
			keyedStream.getTransformation(),
			patternProcessorDataStream.broadcast().getTransformation(),
			"CoCepOperatorV2",
			operator,
			outTypeInfo,
			inputStream.getExecutionEnvironment().getParallelism());

		TypeInformation<?> keyType = keyedStream.getKeyType();
		transform.setUid("cep-co-operatorv2");
		transform.setStateKeySelectors(keyedStream.getKeySelector(), null);
		transform.setStateKeyType(keyType);
		SingleOutputStreamOperator<OUT> returnStream = new SingleOutputStreamOperator(environment, transform);

		environment.addOperator(transform);

		return returnStream;
	}

	// ---------------------------------------- factory-like methods ---------------------------------------- //

	static <IN> MultiplePatternStreamBuilder<IN> forStreamAndPattern(final DataStream<IN> inputStream, final Pattern<IN, ?> pattern) {
		return new MultiplePatternStreamBuilder<>(inputStream, null, null, null, null, null, null, null, Collections.emptyList(), null, new HashMap<>());
	}

	static <IN> MultiplePatternStreamBuilder<IN> forStreamAndPatternDataStream(final DataStream<IN> inputStream, final DataStream<?> patternDataStream) {
		if (patternDataStream.getType().getTypeClass() == Pattern.class) {
			return new MultiplePatternStreamBuilder<>(inputStream, (DataStream<Pattern<IN, IN>>) patternDataStream, null, null, null, null, null, null, Collections.emptyList(), null, new HashMap<>());
		} else if (patternDataStream.getType().getTypeClass() == PatternProcessor.class){
			return new MultiplePatternStreamBuilder<>(inputStream, null , (DataStream<PatternProcessor<IN, ?>>) patternDataStream, null, null, null, null, null, Collections.emptyList(), null, new HashMap<>());
		} else {
			throw new UnsupportedOperationException("PatternDataStream data type :" + patternDataStream.getType().getTypeClass().getName() + " is unsupported");
		}
	}

	static <IN> MultiplePatternStreamBuilder<IN> forStreamAndPatternJsonStream(final DataStream<IN> inputStream, final DataStream<String> patternJsonStream, final CepEventParserFactory factory) {
		return new MultiplePatternStreamBuilder<>(inputStream, null, null, patternJsonStream, null, null, factory, null, Collections.emptyList(), null, new HashMap<>());
	}

	static <IN> MultiplePatternStreamBuilder<IN> forStreamAndPatternList(final DataStream<IN> inputStream, List<Pattern<IN, IN>> patternList) {
		return new MultiplePatternStreamBuilder<>(inputStream, null, null, null, null, null, null, null, patternList, new SimpleEmptyPatternStreamFactory(), new HashMap<>());
	}

}
