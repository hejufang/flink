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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.functions.MultiplePatternProcessFunctionV2;
import org.apache.flink.cep.functions.MultiplePatternTimedOutPartialMatchHandlerV2;
import org.apache.flink.cep.pattern.EventMatcher;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.PatternProcessor;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link org.apache.flink.cep.operator.CoCepOperatorV2}.
 */
@RunWith(Parameterized.class)
public class CoOperatorV2ITCase extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(CoOperatorV2ITCase.class);
	private static final String WATCH_LIVE_ROOM = "WATCH_LIVE_ROOM";
	private static final String WATCH_PRODUCT = "WATCH_PRODUCT";
	private static final String CREATE_ORDER = "CREATE_ORDER";
	private static final String PAY_ORDER = "PAY_ORDER";
	private static final String REFUND = "REFUND";

	@Parameterized.Parameter
	public String statebackend;

	@Parameterized.Parameters(name = "backend = {0}")
	public static Object[] data() {
		return new Object[] { "filesystem", "rocksdb" };
	}

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<>(1024);

	@Before
	public void setup() {
		queue.clear();
	}

	@Test
	public void testSingePattern() throws Exception {
		PatternProcessor<EventV2, ?> patternProcessor = createTestPatternProcessor();
		EventV2 e1 = new EventV2(1000, WATCH_PRODUCT, new HashMap<>(), 1);
		EventV2 e2 = new EventV2(1000, CREATE_ORDER, new HashMap<>(), 3);
		EventV2 e3 = new EventV2(1000, PAY_ORDER, new HashMap<>(), 5);
		EventV2 e4 = new EventV2(2000, REFUND, new HashMap<>(), 8);
		Object[] data = {
				patternProcessor,
				new Barrier(),
				Tuple2.of(e1, 2L),
				Tuple2.of(e2, 4L),
				Tuple2.of(e3, 6L),
				Tuple2.of(e4, 100L)
		};

		queue.addAll(Arrays.asList(data));

		assertEquals(Arrays.asList("1,1000"), runTest());
	}

	@Test
	public void testDisablePattern() throws Exception {
		List<String> runTest = runTestDisablePattern();
		assertTrue(runTest != null && runTest.isEmpty());
	}

	@Test
	public void testMultiPattern() throws Exception {
		PatternProcessor<EventV2, ?> patternProcessor31 = createTestPatternProcessor31();
		PatternProcessor<EventV2, ?> patternProcessor32 = createTestPatternProcessor32();
		int eventId1 = 1000;
		int eventId2 = 2000;
		EventV2 e1 = new EventV2(eventId1, WATCH_LIVE_ROOM, new HashMap<>(), 1);
		EventV2 e2 = new EventV2(eventId1, CREATE_ORDER, new HashMap<>(), 3);
		EventV2 e3 = new EventV2(eventId1, PAY_ORDER, new HashMap<>(), 5);
		EventV2 e4 = new EventV2(eventId2, PAY_ORDER, new HashMap<>(), 8);
		EventV2 e5 = new EventV2(eventId2, CREATE_ORDER, new HashMap<>(), 9);
		Object[] data = {
				patternProcessor31,
				patternProcessor32,
				new Barrier(),
				Tuple2.of(e1, 2L),
				Tuple2.of(e2, 4L),
				Tuple2.of(e3, 6L),
				Tuple2.of(e4, 100L),
				Tuple2.of(e5, 110L)
		};

		queue.addAll(Arrays.asList(data));

		assertEquals(Arrays.asList(patternProcessor31.getId() + "," + eventId1, patternProcessor32.getId() + "," + eventId1), runTest());
	}

	private List<String> runTest() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
				temporaryFolder.newFolder().toURI().toString());
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(60);
		DataStreamSource<PatternProcessor<EventV2, ?>> patternDataStream = env.addSource(new PatternDataStream());

		DataStream<EventV2> input = env.addSource(new EventStream())
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<EventV2, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<EventV2, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<EventV2, Long> lastElement,
							long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				}).map((MapFunction<Tuple2<EventV2, Long>, EventV2>) value -> value.f0)
				.keyBy((KeySelector<EventV2, Integer>) EventV2::getEventId);

		DataStream<String> result = CEP.patternProcess(input, patternDataStream, TypeInformation.of(String.class));

		List<Object> list = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(list::add);
		List<String> resultList = list.stream().map(e -> (String) e).collect(Collectors.toList());

		resultList.sort(String::compareTo);
		return resultList;
	}

	private List<String> runTestDisablePattern() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		env.getConfiguration().setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
				temporaryFolder.newFolder().toURI().toString());
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(60);
		DataStreamSource<PatternProcessor<EventV2, ?>> patternDataStream = env.addSource(new PatternDataStreamForDisablePattern());

		DataStream<EventV2> input = env.addSource(new EventStreamForDisablePattern())
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<EventV2, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<EventV2, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<EventV2, Long> lastElement,
							long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				}).map((MapFunction<Tuple2<EventV2, Long>, EventV2>) value -> value.f0)
				.keyBy((KeySelector<EventV2, Integer>) EventV2::getEventId);

		DataStream<String> result = CEP.patternProcess(input, patternDataStream, TypeInformation.of(String.class));

		List<Object> list = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(list::add);
		List<String> resultList = list.stream().map(e -> (String) e).collect(Collectors.toList());

		resultList.sort(String::compareTo);
		return resultList;
	}

	private static class PatternDataStream implements SourceFunction<PatternProcessor<EventV2, ?>> {
		@Override
		public void run(SourceContext<PatternProcessor<EventV2, ?>> ctx) throws Exception {
			while (!queue.isEmpty()) {
				if (queue.peek() instanceof PatternProcessor) {
					ctx.collect((PatternProcessor<EventV2, ?>) queue.poll());
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
		public void cancel() {
		}
	}

	private static class PatternDataStreamForDisablePattern implements SourceFunction<PatternProcessor<EventV2, ?>> {
		@Override
		public void run(SourceContext<PatternProcessor<EventV2, ?>> ctx) throws Exception {
			PatternProcessor<EventV2, ?> patternProcessor = createTestPatternProcessor();
			ctx.collect(patternProcessor);
			Thread.sleep(5000);
			TestPatternProcessor patternProcessor2 = (TestPatternProcessor) createTestPatternProcessor();
			patternProcessor2.setIsAlive(false);
			ctx.collect(patternProcessor2);
		}

		@Override
		public void cancel() {
		}
	}

	private static class EventStreamForDisablePattern implements SourceFunction<Tuple2<EventV2, Long>> {

		EventStreamForDisablePattern() {
		}

		@Override
		public void run(SourceContext<Tuple2<EventV2, Long>> ctx) throws Exception {
			Thread.sleep(2000);
			int eventId1 = 1000;
			int eventId2 = 2000;
			EventV2 e1 = new EventV2(eventId1, WATCH_PRODUCT, new HashMap<>(), 1);
			EventV2 e2 = new EventV2(eventId1, CREATE_ORDER, new HashMap<>(), 3);
			EventV2 e3 = new EventV2(eventId1, PAY_ORDER, new HashMap<>(), 5);
			ctx.collect(Tuple2.of(e1, 2L));
			ctx.collect(Tuple2.of(e2, 4L));
			ctx.collect(Tuple2.of(e3, 6L));
			Thread.sleep(8000);
			EventV2 e4 = new EventV2(eventId2, WATCH_PRODUCT, new HashMap<>(), 6);
			EventV2 e5 = new EventV2(eventId2, CREATE_ORDER, new HashMap<>(), 7);
			EventV2 e6 = new EventV2(eventId2, PAY_ORDER, new HashMap<>(), 8);
			ctx.collect(Tuple2.of(e4, 100L));
			ctx.collect(Tuple2.of(e5, 101L));
			ctx.collect(Tuple2.of(e6, 102L));
		}

		@Override
		public void cancel() {
		}
	}

	private static class EventStream implements SourceFunction<Tuple2<EventV2, Long>> {

		EventStream() {
		}

		@Override
		public void run(SourceContext<Tuple2<EventV2, Long>> ctx) throws Exception {
			Thread.sleep(2000);
			while (!queue.isEmpty()) {
				if (queue.peek() instanceof Tuple2) {
					ctx.collect((Tuple2<EventV2, Long>) queue.poll());
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
		public void cancel() {
		}
	}

	/**
	 * Just for sleep 1s.
	 */
	private static class Barrier implements Serializable {
	}

	static class TestPatternProcessor implements PatternProcessor<EventV2, String> {

		private String ruleId;
		private int version;
		private Boolean isAlive;
		private EventMatcher<EventV2> eventMatcher;
		private KeySelector<EventV2, Object> keySelector;
		private Pattern<EventV2, EventV2> pattern;
		private MultiplePatternProcessFunctionV2<EventV2, String> functionV2;

		public TestPatternProcessor() {
		}

		@Override
		public String getId() {
			return ruleId;
		}

		@Override
		public int getVersion() {
			return version;
		}

		@Override
		public Boolean getIsAlive() {
			return isAlive;
		}

		@Override
		public EventMatcher<EventV2> getEventMatcher() {
			return eventMatcher;
		}

		@Override
		public KeySelector<EventV2, Object> getKeySelector() {
			return keySelector;
		}

		@Override
		public Pattern<EventV2, ?> getPattern() {
			return pattern;
		}

		@Override
		public MultiplePatternProcessFunctionV2<EventV2, String> getPatternProcessFunction() {
			return functionV2;
		}

		public void setRuleId(String ruleId) {
			this.ruleId = ruleId;
		}

		public void setVersion(int version) {
			this.version = version;
		}

		public void setIsAlive(Boolean isAlive) {
			this.isAlive = isAlive;
		}

		public void setEventMatcher(EventMatcher<EventV2> eventMatcher) {
			this.eventMatcher = eventMatcher;
		}

		public void setKeySelector(KeySelector<EventV2, Object> keySelector) {
			this.keySelector = keySelector;
		}

		public void setPattern(Pattern<EventV2, EventV2> pattern) {
			this.pattern = pattern;
		}

		public void setFunctionV2(MultiplePatternProcessFunctionV2<EventV2, String> functionV2) {
			this.functionV2 = functionV2;
		}
	}

	private static PatternProcessor<EventV2, String> createTestPatternProcessor() {
		TestPatternProcessor patternProcessor = new TestPatternProcessor();

		patternProcessor.setRuleId("1");
		patternProcessor.setVersion(1);
		patternProcessor.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals(WATCH_PRODUCT)
				|| event.getEventName().equals(CREATE_ORDER)
						|| event.getEventName().equals(PAY_ORDER);
			}
		};
		patternProcessor.setEventMatcher(eventMatcher);
		KeySelector<EventV2, Object> keySelector = new KeySelector<EventV2, Object>() {

			@Override
			public Object getKey(EventV2 value) throws Exception {
				return value.getEventId();
			}
		};
		patternProcessor.setKeySelector(keySelector);

		Pattern<EventV2, EventV2> pattern = Pattern.<EventV2>begin("start").where(new SimpleCondition<EventV2>() {
			@Override
			public boolean filter(EventV2 event) {
				return event.getEventName().equals(WATCH_PRODUCT);
			}
		}).next("middle").where(new SimpleCondition<EventV2>() {
			@Override
			public boolean filter(EventV2 event) {
				return event.getEventName().equals(CREATE_ORDER);
			}
		}).followedBy("end").where(new SimpleCondition<EventV2>() {
			@Override
			public boolean filter(EventV2 event) {
				return event.getEventName().equals(PAY_ORDER);
			}
		}).within(Time.seconds(60));
		patternProcessor.setPattern(pattern);

		MultiplePatternProcessFunctionV2<EventV2, String> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor.setFunctionV2(functionV2);

		return patternProcessor;
	}

	private PatternProcessor<EventV2, String> createTestPatternProcessor31() {
		TestPatternProcessor patternProcessor = new TestPatternProcessor();
		patternProcessor.setRuleId("1");
		patternProcessor.setVersion(1);
		patternProcessor.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals(CREATE_ORDER) || event.getEventName().equals(PAY_ORDER);
			}
		};
		patternProcessor.setEventMatcher(eventMatcher);
		KeySelector<EventV2, Object> keySelector = new KeySelector<EventV2, Object>() {

			@Override
			public Object getKey(EventV2 value) throws Exception {
				return value.getEventId();
			}
		};
		patternProcessor.setKeySelector(keySelector);

		Pattern<EventV2, EventV2> pattern = Pattern.<EventV2>begin("begin").where(new SimpleCondition<EventV2>() {
			@Override
			public boolean filter(EventV2 event) {
				return event.getEventName().equals(CREATE_ORDER);
			}
		}).followedBy("end").where(new SimpleCondition<EventV2>() {
			@Override
			public boolean filter(EventV2 event) {
				return event.getEventName().equals(PAY_ORDER);
			}
		}).within(Time.seconds(10));
		patternProcessor.setPattern(pattern);

		MultiplePatternProcessFunctionV2<EventV2, String> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor.setFunctionV2(functionV2);
		return patternProcessor;
	}

	private PatternProcessor<EventV2, String> createTestPatternProcessor32() {
		TestPatternProcessor patternProcessor = new TestPatternProcessor();

		patternProcessor.setRuleId("2");
		patternProcessor.setVersion(1);
		patternProcessor.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals(WATCH_LIVE_ROOM)
						|| event.getEventName().equals(CREATE_ORDER);
			}
		};
		patternProcessor.setEventMatcher(eventMatcher);
		KeySelector<EventV2, Object> keySelector = new KeySelector<EventV2, Object>() {

			@Override
			public Object getKey(EventV2 value) throws Exception {
				return value.getEventId();
			}
		};
		patternProcessor.setKeySelector(keySelector);

		Pattern<EventV2, EventV2> pattern2 = Pattern.<EventV2>begin("begin").where(new SimpleCondition<EventV2>() {
			@Override
			public boolean filter(EventV2 event) {
				return event.getEventName().equals(WATCH_LIVE_ROOM);
			}
		}).followedBy("end").where(new SimpleCondition<EventV2>() {
			@Override
			public boolean filter(EventV2 event) {
				return event.getEventName().equals(CREATE_ORDER);
			}
		}).within(Time.seconds(10));
		patternProcessor.setPattern(pattern2);
		MultiplePatternProcessFunctionV2<EventV2, String> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor.setFunctionV2(functionV2);

		return patternProcessor;
	}

	static class TestMultiplePatternProcessFunctionV2 extends MultiplePatternProcessFunctionV2<EventV2, String>
			implements MultiplePatternTimedOutPartialMatchHandlerV2<EventV2> {

		@Override
		public void open(Configuration parameters) throws Exception {
			LOG.trace("Init MultiplePatternProcessFunctionV2!");
		}

		@Override
		public void processMatch(Tuple2<String, Map<String, List<EventV2>>> match, Context ctx, Object key,
				Collector<String> out) throws Exception {
			out.collect(match.f0 + "," + key);
		}

		@Override
		public void processUnMatch(EventV2 event, Context ctx, Object key, Collector<String> out) {
			LOG.trace("output process unmatch, patternId: " + ctx.currentPattern().getPatternId());
		}

		@Override
		public void close() throws Exception {
			LOG.trace("Close MultiplePatternProcessFunctionV2!");
		}

		@Override
		public void processTimedOutMatch(Tuple2<String, Map<String, List<EventV2>>> match, Object key, Context ctx)
				throws Exception {
			LOG.trace("timeout, current eventTs: " + ctx.timestamp() + ", patternId: "
					+ ctx.currentPattern().getPatternId());
		}
	}
}
