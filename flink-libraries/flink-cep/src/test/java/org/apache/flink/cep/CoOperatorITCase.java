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
import org.apache.flink.cep.pattern.parser.TestCepEventParser;
import org.apache.flink.cep.test.TestData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * CoOperatorITCase.
 */
public class CoOperatorITCase {

	private static ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<>(1024);

	@Before
	public void setup() {
		queue.clear();
	}

	private String readPatternFromResource(String path) throws IOException {
		StringBuilder contentBuilder = new StringBuilder();
		boolean skip = false;
		try (InputStream stream = getClass().getClassLoader().getResourceAsStream(path)) {
			BufferedReader br = new BufferedReader(new InputStreamReader(stream));
			String line;
			line = br.readLine();
			while (line != null){
				if (line.trim().equals("/*")) {
					skip = true;
				} else if (line.trim().equals("*/")) {
					skip = false;
				} else {
					if (!skip) {
						contentBuilder.append(line);
						contentBuilder.append("\n");
					}
				}
				line = br.readLine();
			}
		}
		return contentBuilder.toString();
	}

	@Test
	public void testCustomizedCondition() throws IOException {
		Object[] data = {
				readPatternFromResource("patterns/customized_pattern.txt"),
				new Barrier(),
				Tuple2.of(new Event(1, "v3", 2.0), 2L),
				Tuple2.of(new Event(4, "v1", 2.0), 2L),
				Tuple2.of(new Event(12, "buy", 199999999.0), 2L), // match
				Tuple2.of(new Event(1, "v1", 12.0), 2L),
				Tuple2.of(new Event(11, "v1", 12.0), 200L)
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Arrays.asList("customized_pattern,12"), runTest());
	}

	@Test
	public void testOrPattern() throws IOException {
		Object[] data = {
				readPatternFromResource("patterns/or_pattern.txt"),
				new Barrier(),
				Tuple2.of(new Event(1, "v3", 2.0), 2L),
				Tuple2.of(new Event(4, "v1", 2.0), 2L), // match
				Tuple2.of(new Event(12, "v99", 199.0), 2L),
				Tuple2.of(new Event(1, "v1", 12.0), 2L), // match
				Tuple2.of(new Event(11, "v1", 12.0), 200L)
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Arrays.asList("or_pattern,1", "or_pattern,11", "or_pattern,4"), runTest());
	}

	@Test
	public void testSumPattern() throws IOException {
		Object[] data = {
				TestData.SUM_PATTERN_1,
				new Barrier(),
				Tuple2.of(new Event(1, "buy", 2.0), 2L),
				Tuple2.of(new Event(1, "buy", 4.0), 1L),
				Tuple2.of(new Event(1, "buy", 1.0), 8L),
				Tuple2.of(new Event(1, "middle", 2.0), 6L),
				Tuple2.of(new Event(7, "middle", 5.0), 100L)
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Collections.singletonList("test_agg,1"), runTest());
	}

	@Test
	public void testMultiplePattern() throws IOException {
		Object[] data = {
				TestData.SUM_PATTERN_1,
				TestData.FOLLOWEDBY_PATTERN,
				new Barrier(),
				Tuple2.of(new Event(1, "buy", 1.0), 2L),
				Tuple2.of(new Event(1, "buy", 3.0), 1L),
				Tuple2.of(new Event(1, "buy", 5.0), 8L),
				Tuple2.of(new Event(1, "middle", 2.0), 6L),
				Tuple2.of(new Event(7, "middle", 5.0), 100L)
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Arrays.asList("pattern_followedby,1", "test_agg,1"), runTest());
	}

	@Test
	public void testMultiplePatternAndUpdate() throws IOException {
		Object[] data = {
				TestData.SUM_PATTERN_1,
				TestData.FOLLOWEDBY_PATTERN,
				new Barrier(),
				Tuple2.of(new Event(1, "buy", 1.0), 2L),
				Tuple2.of(new Event(1, "buy", 3.0), 1L),
				Tuple2.of(new Event(1, "buy", 5.0), 8L),
				Tuple2.of(new Event(1, "end", 5.0), 20L),
				new Barrier(),
				TestData.COUNT_PATTERN_2,
				new Barrier(),
				Tuple2.of(new Event(1, "imp", 2.0), 21L),
				Tuple2.of(new Event(1, "imp", 2.0), 21L),
				Tuple2.of(new Event(1, "middle", 2.0), 21L),
				Tuple2.of(new Event(7, "middle", 5.0), 100L)
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Arrays.asList("pattern_followedby,1", "test_agg,1", "test_agg,1"), runTest());
	}

	@Test
	public void testDisablePattern() throws IOException {
		Object[] data = {
				TestData.SUM_PATTERN_1,
				new Barrier(),
				Tuple2.of(new Event(2, "buy", 1.0), 2L),
				Tuple2.of(new Event(2, "buy", 3.0), 1L),
				new Barrier(),
				TestData.disablePattern("test_agg"),
				new Barrier(),
				Tuple2.of(new Event(2, "buy", 5.0), 8L),
				Tuple2.of(new Event(7, "middle", 5.0), 100L)
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Collections.emptyList(), runTest());
	}

	private List<String> runTest() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(10);

		DataStream<String> patternJsonStream = env.addSource(new PatternJsonStream());
		DataStream<Event> input = env.addSource(new EventStream())
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

		DataStream<String> result = CEP.pattern(input, patternJsonStream, TestCepEventParser::new).process(new MultiplePatternProcessFunction<Event, String>() {
			@Override
			public void processMatch(Tuple2<String, Map<String, List<Event>>> match, Context ctx, Object key, Collector<String> out) throws Exception {
				out.collect(match.f0 + "," + key);
			}
		});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);
		return resultList;
	}

	private static class PatternJsonStream implements SourceFunction<String> {

		PatternJsonStream() {}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (!queue.isEmpty()) {
				if (queue.peek() instanceof String) {
					ctx.collect((String) queue.poll());
					if (queue.peek() instanceof Barrier) {
						Thread.sleep(100);
						queue.poll();
					}
				} else {
					Thread.sleep(10);
				}
			}
		}

		@Override
		public void cancel() {}
	}

	private static class EventStream implements SourceFunction<Tuple2<Event, Long>> {

		EventStream() {}

		@Override
		public void run(SourceContext<Tuple2<Event, Long>> ctx) throws Exception {
			while (!queue.isEmpty()) {
				if (queue.peek() instanceof Tuple2) {
					ctx.collect((Tuple2<Event, Long>) queue.poll());
					if (queue.peek() instanceof Barrier) {
						Thread.sleep(100);
						queue.poll();
					}
				} else {
					Thread.sleep(10);
				}
			}
		}

		@Override
		public void cancel() {}
	}

	/**
	 * Just for sleep 1s.
	 */
	private static class Barrier implements Serializable {}
}
