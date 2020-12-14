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
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.parser.TestCepEventParser;
import org.apache.flink.cep.test.TestData;
import org.apache.flink.cep.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for CEP Pattern Stream.
 */
public class CEPPatternStreamITCase {

	@Test
	public void testEmptyPatternStream() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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

		DataStream<String> result = CEP.pattern(input, patternDataStream).select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
						pattern1.f0 + "," +
						pattern1.f1.get("start").get(0).getId() + "," +
						pattern1.f1.get("middle").get(0).getId() + "," +
						pattern1.f1.get("end").get(0).getId());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("unknown,1,1,1", "unknown,2,2,2"), resultList);
	}

	@Test
	public void testNotFollowedBy() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream(pattern));

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

		DataStream<String> result = CEP.pattern(input, patternDataStream).select(
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

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream(pattern));

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

		DataStream<String> result = CEP.pattern(input, patternDataStream).select(
				(MultiplePatternSelectFunction<Event, String>) pattern1 ->
						pattern1.f0 + "," +
						pattern1.f1.get("start").get(0).getId() + "," +
						pattern1.f1.get("end").get(0).getId());

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("unknown,1,1", "unknown,1,1"), resultList);
	}

	private static class EventStream implements SourceFunction<Tuple2<Event, Long>> {

		static boolean sendFlag = false;

		private List<Tuple2<Event, Long>> data = new ArrayList<>();

		EventStream(Tuple2<Event, Long>... elements) {
			sendFlag = false;
			data.addAll(Arrays.asList(elements));
		}

		@Override
		public void run(SourceContext<Tuple2<Event, Long>> ctx) throws Exception {
			while (!sendFlag) {
				Thread.sleep(100);
			}
			for (Tuple2<Event, Long> event : data) {
				ctx.collect(event);
			}
		}

		@Override
		public void cancel() {}
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
}
