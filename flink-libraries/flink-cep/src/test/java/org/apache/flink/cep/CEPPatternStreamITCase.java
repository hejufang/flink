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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.parser.TestCepEventParser;
import org.apache.flink.cep.test.TestData;
import org.apache.flink.cep.time.Time;
import org.apache.flink.cep.utils.TestEmptyPatternStreamFactory;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for CEP Pattern Stream.
 */
@RunWith(Parameterized.class)
public class CEPPatternStreamITCase {

	@Parameterized.Parameter
	public String statebackend;

	@Parameterized.Parameters(name = "backend = {0}")
	public static Object[] data() {
		return new Object[]{"filesystem", "rocksdb"};
	}

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testEmptyPatternStream() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream());

		// (Event, timestamp)
		DataStream<Event> input = env.fromElements(
				Tuple2.of(new Event(1, "start", 1.0), 5L),
				Tuple2.of(new Event(2, "middle", 2.0), 1L),
				Tuple2.of(new Event(3, "end", 3.0), 3L),
				Tuple2.of(new Event(4, "end", 4.0), 10L),
				Tuple2.of(new Event(5, "middle", 5.0), 7L),
				// last element for high final watermark
				Tuple2.of(new Event(5, "middle", 5.0), 100L)
		).map(x -> x.f0).keyBy(Event::getId);

		DataStream<String> result = CEP.pattern(input, patternDataStream).select(
				(MultiplePatternSelectFunction<Event, String>)
						pattern -> pattern.f0 + "," +
						pattern.f1.get("start").get(0).getId() + "," +
						pattern.f1.get("middle").get(0).getId() + "," +
						pattern.f1.get("end").get(0).getId());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Collections.emptyList(), resultList);
	}

	@Test
	public void testPatternJsonStream() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);

		DataStream<String> patternJsonStream = env.addSource(new PatternJsonStream(TestData.PATTERN_DATA_1));
		// (Event, timestamp)
		DataStream<Event> input = env.addSource(new EventStream(
				Tuple2.of(new Event(1, "start", 1.0), 5L),
				Tuple2.of(new Event(2, "middle", 2.0), 4L),
				Tuple2.of(new Event(2, "start", 2.0), 3L),
				Tuple2.of(new Event(3, "start", 4.1), 5L),
				Tuple2.of(new Event(1, "end", 4.0), 10L),
				Tuple2.of(new Event(2, "end", 2.0), 8L),
				Tuple2.of(new Event(1, "middle", 5.0), 7L),
				Tuple2.of(new Event(3, "middle", 6.0), 9L),
				Tuple2.of(new Event(3, "end", 7.0), 7L),
				// last element for high final watermark
				Tuple2.of(new Event(7, "middle", 5.0), 100L)))
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				})
				.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
				.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternJsonStream, TestCepEventParser::new).select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
						pattern1.f0 + "," +
						pattern1.f1.get("start").get(0).getId() + "," +
						pattern1.f1.get("middle").get(0).getId() + "," +
						pattern1.f1.get("end").get(0).getId());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("test_pattern,1,1,1", "test_pattern,2,2,2"), resultList);
	}

	@Test
	public void testPatternDataStream() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(10);
		env.setParallelism(2);

		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream(pattern));

		// (Event, timestamp)
		DataStream<Event> input = env.addSource(new EventStream(
				Tuple2.of(new Event(1, "warmUp", 1.0), 5L),
				Tuple2.of(new Event(1, "start", 1.0), 5L),
				Tuple2.of(new Event(2, "middle", 2.0), 4L),
				Tuple2.of(new Event(2, "start", 2.0), 3L),
				Tuple2.of(new Event(3, "start", 4.1), 5L),
				Tuple2.of(new Event(1, "end", 4.0), 10L),
				Tuple2.of(new Event(2, "end", 2.0), 8L),
				Tuple2.of(new Event(1, "middle", 5.0), 7L),
				Tuple2.of(new Event(3, "middle", 6.0), 9L),
				Tuple2.of(new Event(3, "end", 7.0), 7L),
				// last element for high final watermark
				Tuple2.of(new Event(7, "middle", 5.0), 100L)))
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				})
				.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
				.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternDataStream)
				.process(new MultiplePatternProcessFunction<Event, String>() {
					@Override
					public void processMatch(Tuple2<String, Map<String, List<Event>>> match, Context ctx, Object key, Collector<String> out) throws Exception {
						String res = match.f0 + "," +
								match.f1.get("start").get(0).getId() + "," +
								match.f1.get("middle").get(0).getId() + "," +
								match.f1.get("end").get(0).getId();
						out.collect(res);
					}
				});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("unknown,1,1,1", "unknown,2,2,2"), resultList);
	}

	@Test
	public void testNotFollowedBy() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);

		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).notFollowedBy("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.milliseconds(10));

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream());

		DataStream<Event> input = env.addSource(new EventStream(
				Tuple2.of(new Event(1, "start", 1.0), 5L),
				// last element for high final watermark
				Tuple2.of(new Event(7, "middle", 5.0), 100L)))
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				})
				.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
				.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternDataStream)
				.withInitialPatterns(Collections.singletonList(pattern))
				.select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
						pattern1.f0 + "," +
						pattern1.f1.get("start").get(0).getId() + "");

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Collections.singletonList("unknown,1"), resultList);
	}

	@Test
	public void testAllowSinglePartialMatchPerKeyStream() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);

		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});
		pattern.setAllowSinglePartialMatchPerKey(true);

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream());

		DataStream<Event> input = env.addSource(new EventStream(
				Tuple2.of(new Event(1, "start", 1.0), 5L),
				Tuple2.of(new Event(1, "end", 1.0), 6L),
				Tuple2.of(new Event(1, "start", 1.0), 7L),
				Tuple2.of(new Event(1, "end", 1.0), 8L),
				// last element for high final watermark
				Tuple2.of(new Event(7, "middle", 5.0), 100L)))
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				})
				.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
				.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternDataStream)
				.withInitialPatterns(Collections.singletonList(pattern))
				.select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
						pattern1.f0 + "," +
						pattern1.f1.get("start").get(0).getId() + "," +
						pattern1.f1.get("end").get(0).getId());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("unknown,1,1", "unknown,1,1"), resultList);
	}

	@Test
	public void testCountPattern() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);

		DataStream<String> patternJsonStream = env.addSource(new PatternJsonStream());

		DataStream<Event> input = env.addSource(new EventStream(
				Tuple2.of(new Event(1, "buy", 1.0), 5L),
				Tuple2.of(new Event(2, "end", 1.0), 6L),
				Tuple2.of(new Event(1, "buy", 3.0), 7L),
				Tuple2.of(new Event(1, "buy", 5.0), 8L),
				// last element for high final watermark
				Tuple2.of(new Event(7, "middle", 5.0), 100L)))
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				})
				.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
				.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternJsonStream, TestCepEventParser::new)
				.withInitialPatternJsons(Collections.singletonList(TestData.SUM_PATTERN_1))
				.select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
						pattern1.f0 + "," + pattern1.f1.get("imp").get(0).getId());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("test_agg,1"), resultList);
	}

	@Test
	public void testSumPattern() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);

		DataStream<String> patternJsonStream = env.addSource(new PatternJsonStream());

		DataStream<Event> input = env.addSource(new EventStream(
				Tuple2.of(new Event(1, "buy", 1.0), 5L),
				Tuple2.of(new Event(2, "end", 1.0), 6L),
				Tuple2.of(new Event(1, "buy", 3.0), 7L),
				Tuple2.of(new Event(1, "buy", 5.0), 8L),
				// last element for high final watermark
				Tuple2.of(new Event(7, "middle", 5.0), 100L)))
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				})
				.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
				.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternJsonStream, TestCepEventParser::new)
				.withInitialPatternJsons(Arrays.asList(TestData.SUM_PATTERN_1))
				.select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
						pattern1.f0 + "," + pattern1.f1.get("imp").get(0).getId());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("test_agg,1"), resultList);
	}

	@Test
	public void testMultiplePatterns() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<String> patternJsonStream = env.addSource(new PatternJsonStream(TestData.FOLLOWEDBY_PATTERN, TestData.COUNT_PATTERN_1));

		DataStream<Event> input = env.addSource(new EventStream(
				Tuple2.of(new Event(1, "buy", 1.0), 5L),
				Tuple2.of(new Event(2, "end", 1.0), 6L),
				Tuple2.of(new Event(1, "buy", 3.0), 7L),
				Tuple2.of(new Event(1, "buy", 5.0), 8L),
				Tuple2.of(new Event(1, "middle", 2.0), 9L),
				// last element for high final watermark
				Tuple2.of(new Event(7, "middle", 5.0), 100L)))
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				})
				.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
				.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternJsonStream, TestCepEventParser::new).select(
				(MultiplePatternSelectFunction<Event, String>) pattern -> {
					if (pattern.f0.equals("pattern_followedby")) {
						return pattern.f0 + "," + pattern.f1.get("start").get(0).getId();
					} else {
						return pattern.f0 + "," + pattern.f1.get("imp").get(0).getId();
					}
				});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("pattern_followedby,1", "test_count,1"), resultList);
	}

	@Test
	public void testNoFollowByWithWindow() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).notFollowedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.milliseconds(5));

		pattern.setPatternMeta("notFollowByPattern", 1);

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream());

		DataStream<Event> input = env.addSource(new EventStream(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(1, "end", 2.0), 6L),
			Tuple2.of(new Event(1, "start", 3.0), 7L),
			// last element for high final watermark
			Tuple2.of(new Event(7, "middle", 5.0), 100L)
		))
			.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {
				@Override
				public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
					return element.f1;
				}

				@Override
				public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
					return new Watermark(lastElement.f1 - 5);
				}
			})
			.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
			.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternDataStream)
			.withInitialPatterns(Collections.singletonList(pattern))
			.select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
					pattern1.f0 + "," +
						pattern1.f1.get("start").get(0).getPrice());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("notFollowByPattern,3.0"), resultList);
	}

	@Test
	public void testNoFollowByWithInPatternInProcessingTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setParallelism(1);

		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).notFollowedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.seconds(2));

		pattern.setPatternMeta("notFollowByPattern", 1);

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream());

		DataStream<Event> input = env.addSource(new EventStream(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			new Barrier(),
			Tuple2.of(new Event(1, "end", 2.0), 6L),
			Tuple2.of(new Event(1, "start", 3.0), 7L),
			new BigBarrier(
		))).map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
			.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternDataStream)
			.withInitialPatterns(Collections.singletonList(pattern))
			.select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
					pattern1.f0 + "," +
						pattern1.f1.get("start").get(0).getPrice());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("notFollowByPattern,3.0"), resultList);
	}

	@Test
	public void testUnMatchPatternProcessing() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setParallelism(1);

		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.seconds(2));

		pattern.setPatternMeta("followByPattern", 1);

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream());

		DataStream<Event> input = env.addSource(new EventStream(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			// unMatched event
			Tuple2.of(new Event(1, "followedBy", 1.0), 6L),
			Tuple2.of(new Event(1, "end", 1.0), 7L),
			Tuple2.of(new Event(1, "start", 2.0), 8L),
			Tuple2.of(new Event(1, "end", 2.0), 10L),
			// unMatched event
			Tuple2.of(new Event(1, "end", 3.0), 11L),

			new BigBarrier(
		))).map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
			.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternDataStream)
			.withInitialPatterns(Collections.singletonList(pattern))
			.process(new MultiplePatternProcessFunction<Event, String>() {
				@Override
				public void processMatch(Tuple2<String, Map<String, List<Event>>> match, Context ctx, Object key, Collector<String> out) {
				}

				@Override
				public void processUnMatch(final Event event, final Context ctx, final Object key, final Collector<String> out) {
					out.collect(event.toString());
				}

			});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("Event(1, end, 3.0)", "Event(1, followedBy, 1.0)"), resultList);
	}

	private static class EventStream implements SourceFunction<Tuple2<Event, Long>> {

		static boolean sendFlag = false;

		private List<Object> data = new ArrayList<>();

		EventStream(Object... elements) {
			sendFlag = false;
			data.addAll(Arrays.asList(elements));
		}

		@Override
		public void run(SourceContext<Tuple2<Event, Long>> ctx) throws Exception {
			while (!sendFlag) {
				Thread.sleep(100);
			}
			for (Object o : data) {
				if (o instanceof Tuple2){
					ctx.collect((Tuple2<Event, Long>) o);
				} else if (o instanceof Barrier){
					Thread.sleep(1000);
				} else if (o instanceof BigBarrier){
					Thread.sleep(5000);
				}
			}
		}

		@Override
		public void cancel() {}
	}

		@Test
	public void testSinglePattern() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Event> input = env.addSource(new EventStream(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(2, "end", 1.0), 6L),
			Tuple2.of(new Event(1, "buy", 3.0), 7L),
			Tuple2.of(new Event(1, "buy", 5.0), 8L),
			Tuple2.of(new Event(1, "middle", 2.0), 9L),
			// last element for high final watermark
			Tuple2.of(new Event(7, "middle", 5.0), 100L)))
			.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

				@Override
				public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
					return element.f1;
				}

				@Override
				public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
					return new Watermark(lastElement.f1 - 5);
				}
			})
			.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
			.keyBy((KeySelector<Event, Integer>) Event::getId);

		EventStream.sendFlag = true;

		Pattern<Event, Event> singlePattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		});
		singlePattern.setPatternMeta("pattern1", 1);

		DataStream<String> result = CEP.pattern(input, Collections.singletonList(singlePattern))
			.withEmptyPatternStreamFactory(new TestEmptyPatternStreamFactory())
			.select(
			(MultiplePatternSelectFunction<Event, String>) pattern -> {
				return pattern.f0 + "," + pattern.f1.get("start").get(0).getId();
			});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("pattern1,1"), resultList);
	}

	@Test
	public void testPatternList() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Event> input = env.addSource(new EventStream(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(2, "end", 2.0), 6L),
			Tuple2.of(new Event(1, "buy", 3.0), 7L),
			Tuple2.of(new Event(1, "buy", 4.0), 8L),
			Tuple2.of(new Event(1, "middle", 5.0), 9L),
			// last element for high final watermark
			Tuple2.of(new Event(7, "middle", 6.0), 100L)))
			.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

				@Override
				public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
					return element.f1;
				}

				@Override
				public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
					return new Watermark(lastElement.f1 - 5);
				}
			})
			.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
			.keyBy((KeySelector<Event, Integer>) Event::getId);

		EventStream.sendFlag = true;

		Pattern<Event, Event> singlePattern1 = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		});

		Pattern<Event, Event> singlePattern2 = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("buy");
			}
		});
		singlePattern1.setPatternMeta("pattern1", 1);
		singlePattern2.setPatternMeta("pattern2", 2);

		DataStream<String> result = CEP.pattern(input, Arrays.asList(singlePattern1, singlePattern2))
			.withEmptyPatternStreamFactory(new TestEmptyPatternStreamFactory())
			.select(
			(MultiplePatternSelectFunction<Event, String>) pattern -> {
				return pattern.f0 + "," + pattern.f1.get("start").get(0).getPrice() + "," + pattern.f1.get("middle").get(0).getPrice();
			});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("pattern1,1.0,5.0", "pattern2,1.0,3.0"), resultList);
	}

	@Test
	public void testTimeoutHandling() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.milliseconds(5));

		pattern.setPatternMeta("followByPatternWithIn", 1);

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream());

		DataStream<Event> input = env.addSource(new EventStream(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(2, "start", 2.0), 5L),
			Tuple2.of(new Event(3, "start", 3.0), 5L),
			Tuple2.of(new Event(1, "end", 4.0), 6L),
			Tuple2.of(new Event(3, "end", 5.0), 20L),

			// last element for high final watermark
			Tuple2.of(new Event(7, "middle", 5.0), 100L)
		))
			.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {
				@Override
				public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
					return element.f1;
				}

				@Override
				public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
					return new Watermark(lastElement.f1 - 5);
				}
			})
			.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
			.keyBy((KeySelector<Event, Integer>) Event::getId);

		final OutputTag<String> timeoutTag = new OutputTag<String>("timeoutTag"){};

		SingleOutputStreamOperator<String> result = CEP.pattern(input, patternDataStream)
			.withInitialPatterns(Collections.singletonList(pattern))
			.select(
				timeoutTag,
				new MultiplePatternTimeoutFunction<Event, String>() {
					@Override
					public String timeout(Tuple2<String, Map<String, List<Event>>> pattern, long timeoutTimestamp) throws Exception {
						return pattern.f0 + "," +
						pattern.f1.get("start").get(0).getPrice();
					}
				},
		(MultiplePatternSelectFunction<Event, String>) pattern1 ->
					pattern1.f0 + "," +
						pattern1.f1.get("start").get(0).getPrice());

		List<String> resultList = new ArrayList<>();
		List<String> timeoutResultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);
		DataStreamUtils.collect(result.getSideOutput(timeoutTag)).forEachRemaining(timeoutResultList::add);

		resultList.sort(String::compareTo);
		timeoutResultList.sort(String::compareTo);

		assertEquals(Arrays.asList("followByPatternWithIn,1.0"), resultList);
		assertEquals(Arrays.asList("followByPatternWithIn,2.0", "followByPatternWithIn,3.0"), timeoutResultList);

	}

	private static class PatternJsonStream implements SourceFunction<String> {

		List<String> patternJsons = new ArrayList<>();

		PatternJsonStream(String... jsons) {
			this.patternJsons.addAll(Arrays.asList(jsons));
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			for (String json : patternJsons) {
				ctx.collect(json);
			}

			Thread.sleep(1000);
			EventStream.sendFlag = true;
		}

		@Override
		public void cancel() {}
	}

	private static class PatternDataStream implements SourceFunction<Pattern<Event, Event>> {

		List<Pattern<Event, Event>> patterns = new ArrayList<>();

		PatternDataStream(Pattern<Event, Event>... patterns) {
			this.patterns.addAll(Arrays.asList(patterns));
		}

		@Override
		public void run(SourceContext<Pattern<Event, Event>> ctx) throws Exception {
			for (Pattern<Event, Event> pattern : patterns) {
				ctx.collect(pattern);
			}

			// let the flusher flush the data
			Thread.sleep(100);
			EventStream.sendFlag = true;
		}

		@Override
		public void cancel() {}
	}

	/**
	 * Just for sleep 1s.
	 */
	private static class Barrier implements Serializable {}

	/**
	 * Just for sleep 5s.
	 */
	private static class BigBarrier implements Serializable {}
}
