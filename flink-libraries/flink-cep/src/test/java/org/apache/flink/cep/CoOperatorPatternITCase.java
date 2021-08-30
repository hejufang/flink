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
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import org.junit.Before;
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
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link org.apache.flink.cep.operator.CoCepOperator}.
 */
@RunWith(Parameterized.class)
public class CoOperatorPatternITCase {

	@Parameterized.Parameter
	public String statebackend;

	@Parameterized.Parameters(name = "backend = {0}")
	public static Object[] data() {
		return new Object[]{"filesystem", "rocksdb"};
	}

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<>(1024);

	@Before
	public void setup() {
		queue.clear();
	}

	@Test
	public void testSumCondition() throws IOException {
		Pattern<Event, Event> pattern = Pattern.<Event>begin("begin")
			.where(new TestRichIterativeCondition<Event>() {
				private static final String SUM_PRICE_KEY = "sum(price)";

				@Override
				public void accumulate(Event value, Context<Event> ctx) throws Exception {
					if (value.getName().equals("buy")) {
						Double acc = ctx.getAccumulator(SUM_PRICE_KEY, DoubleSerializer.INSTANCE);
						if (acc == null) {
							ctx.putAccumulator(SUM_PRICE_KEY, value.getPrice(), DoubleSerializer.INSTANCE);
						} else {
							ctx.putAccumulator(SUM_PRICE_KEY, acc + value.getPrice(), DoubleSerializer.INSTANCE);
						}
					}
				}

				@Override
				public boolean filter(Event value, Context<Event> ctx) throws Exception {
					Double acc = ctx.getAccumulator(SUM_PRICE_KEY, DoubleSerializer.INSTANCE);
					return acc != null && acc > 10.0;
				}
			});
		Object[] data = {
			pattern,
			new Barrier(),
			Tuple2.of(new Event(2, "buy", 1.0), 2L),
			Tuple2.of(new Event(2, "buy", 3.0), 1L),
			Tuple2.of(new Event(2, "buy", 5.0), 5L),
			Tuple2.of(new Event(2, "buy", 5.0), 5L),
			Tuple2.of(new Event(7, "middle", 5.0), 100L)
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Collections.singletonList("unknown,2"), runTest());
	}

	@Test
	public void testSumAfterCount() throws IOException {
		Pattern<Event, Event> pattern = Pattern.<Event>begin("begin")
			.where(new TestRichIterativeCondition<Event>() {
				private static final String COUNT_KEY = "count(*)";

				@Override
				public void accumulate(Event value, Context<Event> ctx) throws Exception {
					if (value.getName().equals("buy")) {
						Integer acc = ctx.getAccumulator(COUNT_KEY, IntSerializer.INSTANCE);
						if (acc == null) {
							ctx.putAccumulator(COUNT_KEY, 1, IntSerializer.INSTANCE);
						} else {
							ctx.putAccumulator(COUNT_KEY, ++acc, IntSerializer.INSTANCE);
						}
					}
				}

				@Override
				public boolean filter(Event value, Context<Event> ctx) throws Exception {
					Integer acc = ctx.getAccumulator(COUNT_KEY, IntSerializer.INSTANCE);
					return acc != null && acc > 1;
				}
			}).followedBy("middle").where(
				new TestRichIterativeCondition<Event>() {
					private static final String SUM_PRICE_KEY = "sum(price)";

					@Override
					public void accumulate(Event value, Context<Event> ctx) throws Exception {
						if (value.getName().equals("buy")) {
							Double acc = ctx.getAccumulator(SUM_PRICE_KEY, DoubleSerializer.INSTANCE);
							if (acc == null) {
								ctx.putAccumulator(SUM_PRICE_KEY, value.getPrice(), DoubleSerializer.INSTANCE);
							} else {
								ctx.putAccumulator(SUM_PRICE_KEY, acc + value.getPrice(), DoubleSerializer.INSTANCE);
							}
						}
					}

					@Override
					public boolean filter(Event value, Context<Event> ctx) throws Exception {
						Double acc = ctx.getAccumulator(SUM_PRICE_KEY, DoubleSerializer.INSTANCE);
						return acc != null && acc >= 6.0;
					}
				}
			);
		Object[] data = {
			pattern,
			new Barrier(),
			Tuple2.of(new Event(1, "go", 1.0), 1L),
			Tuple2.of(new Event(2, "buy", 1.0), 2L),
			Tuple2.of(new Event(30, "buy", 3.0), 1L),
			Tuple2.of(new Event(2, "buy", 5.0), 5L),
			Tuple2.of(new Event(2, "buy", 6.0), 6L),
			Tuple2.of(new Event(30, "buy", 5.0), 5L),
			Tuple2.of(new Event(30, "buy", 4.0), 8L),
			Tuple2.of(new Event(30, "buy", 2.0), 6L),
			Tuple2.of(new Event(7, "middle", 5.0), 100L)
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Arrays.asList("unknown,2", "unknown,30"), runTest());
	}

	private List<String> runTest() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.newFolder().toURI().toString());
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(10);

		DataStream<Pattern<Event, Event>> patternDataStream = env.addSource(new PatternDataStream());
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

		DataStream<String> result = CEP.pattern(input, patternDataStream).process(new MultiplePatternProcessFunction<Event, String>() {
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

	private static class PatternDataStream implements SourceFunction<Pattern<Event, Event>> {
		@Override
		public void run(SourceContext<Pattern<Event, Event>> ctx) throws Exception {
			while (!queue.isEmpty()) {
				if (queue.peek() instanceof Pattern) {
					ctx.collect((Pattern<Event, Event>) queue.poll());
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
