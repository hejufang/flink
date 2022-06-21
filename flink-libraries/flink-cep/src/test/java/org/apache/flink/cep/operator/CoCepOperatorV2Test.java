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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventV2;
import org.apache.flink.cep.functions.MultiplePatternProcessFunctionV2;
import org.apache.flink.cep.functions.MultiplePatternTimedOutPartialMatchHandlerV2;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.pattern.EventMatcher;
import org.apache.flink.cep.pattern.KeyedCepEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.PatternProcessor;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.flink.cep.operator.CoCepOperatorV2TestUtilities.getCoCepOpearatorV2;
import static org.apache.flink.cep.operator.CoCepOperatorV2TestUtilities.getCoCepTestHarness;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.validateMockitoUsage;

/**
 * Tests for {@link CoCepOperatorV2}.
 */
@Ignore
public class CoCepOperatorV2Test extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(CoCepOperatorV2Test.class);
	private static final String ORDER_ID = "order_id";
	private static final String USER_ID = "user_id";
	private static final String WATCH_LIVE_ROOM = "WATCH_LIVE_ROOM";
	private static final String CREATE_ORDER = "CREATE_ORDER";
	private static final String PAY_ORDER = "PAY_ORDER";
	private static final String WATCH_PRODUCT = "WATCH_PRODUCT";
	private static final String PRODUCT_WATCH = "PRODUCT_WATCH";
	private static final String PRODUCT_CLICK = "PRODUCT_CLICK";
	private static final String START = "start";
	private static final String MIDDLE = "middle";
	private static final String END = "end";

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@After
	public void validate() {
		validateMockitoUsage();
	}

	@Test
	public void testKeyedCEPOperatorWatermarkForwarding() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCoCepTestHarness(getCoCepOpearatorV2(false));
		try {
			String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
			RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
			rocksDBStateBackend.setDbStoragePath(rocksDbPath);

			PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> testPatternProcessor = createTestPatternProcessor();
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();
			harness.processWatermark1(new Watermark(0));

			harness.processElement2(new StreamRecord(testPatternProcessor));

			EventV2 e1 = new EventV2(1, WATCH_PRODUCT, new HashMap<>(), 1000);
			KeyedCepEvent<EventV2> keyedCepEvent1 = new KeyedCepEvent<>();
			keyedCepEvent1.setKey(1);
			keyedCepEvent1.setEvent(e1);
			keyedCepEvent1.setPatternProcessorIds(new HashSet<>(Arrays.asList(testPatternProcessor.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent1, 1000));

			EventV2 e2 = new EventV2(1, CREATE_ORDER, new HashMap<>(), 2000);
			KeyedCepEvent<EventV2> keyedCepEvent2 = new KeyedCepEvent<>();
			keyedCepEvent2.setKey(1);
			keyedCepEvent2.setEvent(e2);
			keyedCepEvent2.setPatternProcessorIds(new HashSet<>(Arrays.asList(testPatternProcessor.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent2, 2000));

			EventV2 e3 = new EventV2(1, PAY_ORDER, new HashMap<>(), 3000);
			KeyedCepEvent<EventV2> keyedCepEvent3 = new KeyedCepEvent<>();
			keyedCepEvent3.setKey(1);
			keyedCepEvent3.setEvent(e3);
			keyedCepEvent3.setPatternProcessorIds(new HashSet<>(Arrays.asList(testPatternProcessor.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent3, 3000));

			harness.processWatermark1(new Watermark(3600000));
			Queue<Object> result = harness.getOutput();

			assertEquals(3, result.size());
			verifyWatermark(result.poll(), 0L);
			verifyPattern(result.poll(), testPatternProcessor.getId(), e1, e2, e3);
			verifyWatermark(result.poll(), 3600000L);
		} finally {
			harness.close();
		}
	}

	@Test
	public void testClearStateWhenDisablePattern() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCoCepTestHarness(getCoCepOpearatorV2(false));

		try {
			String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
			RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
			rocksDBStateBackend.setDbStoragePath(rocksDbPath);

			PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> testPatternProcessor = createTestPatternProcessor();
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();
			harness.processWatermark1(new Watermark(0));

			harness.processElement2(new StreamRecord(testPatternProcessor));

			EventV2 e1 = new EventV2(1, WATCH_PRODUCT, new HashMap<>(), 1);
			KeyedCepEvent<EventV2> keyedCepEvent1 = new KeyedCepEvent<>();
			keyedCepEvent1.setKey(1);
			keyedCepEvent1.setEvent(e1);
			keyedCepEvent1.setPatternProcessorIds(new HashSet<>(Arrays.asList(testPatternProcessor.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent1, 1));

			EventV2 e2 = new EventV2(1, CREATE_ORDER, new HashMap<>(), 2);
			KeyedCepEvent<EventV2> keyedCepEvent2 = new KeyedCepEvent<>();
			keyedCepEvent2.setKey(1);
			keyedCepEvent2.setEvent(e2);
			keyedCepEvent2.setPatternProcessorIds(new HashSet<>(Arrays.asList(testPatternProcessor.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent2, 2));

			harness.processWatermark1(new Watermark(5));
			BroadcastState patternStates = ((CoCepOperatorV2) harness.getOperator()).getPatternStates();
			assertTrue(patternStates.contains(testPatternProcessor.getId()));
			Queue<Object> result0 = harness.getOutput();
			assertEquals(2, result0.size());
			verifyWatermark(result0.poll(), 0L);
			verifyWatermark(result0.poll(), 5L);
			Map usingNFAs = ((CoCepOperatorV2) harness.getOperator()).getUsingNFAs();
			assertTrue(usingNFAs.size() == 1 && usingNFAs.containsKey(testPatternProcessor.getId()));
			Map<String, SharedBuffer> partialMatches = ((CoCepOperatorV2) harness.getOperator()).getPartialMatches();
			assertEquals(1, partialMatches.size());
			assertEquals(2, partialMatches.get(testPatternProcessor.getId()).getEventsBufferSize());
			assertEquals(2, partialMatches.get(testPatternProcessor.getId()).getSharedBufferNodeSize());

			((TestPatternProcessor) testPatternProcessor).setIsAlive(false);
			harness.processElement2(new StreamRecord(testPatternProcessor));
			assertFalse(patternStates.contains(testPatternProcessor.getId()));
			assertEquals(0, usingNFAs.size());
			assertEquals(0, partialMatches.size());
		} finally {
			harness.close();
		}
	}

	@Test
	public void testMultiPattern() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCoCepTestHarness(getCoCepOpearatorV2(false));
		try {
			String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
			RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
			rocksDBStateBackend.setDbStoragePath(rocksDbPath);

			harness.setStateBackend(rocksDBStateBackend);
			harness.open();
			harness.processWatermark1(new Watermark(0));

			// create first test PatternProcessor.
			PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> patternProcessor1 = createTestPatternProcessor31();

			// Create second test PatternProcessor.
			PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> patternProcessor2 = createTestPatternProcessor32();

			harness.processElement2(new StreamRecord(patternProcessor1));
			harness.processElement2(new StreamRecord(patternProcessor2));

			// Trigger PatternProcessors by events
			int orderId1 = 1;
			EventV2 e1 = new EventV2(orderId1, WATCH_LIVE_ROOM, new HashMap<>(), 1);
			KeyedCepEvent<EventV2> keyedCepEvent1 = new KeyedCepEvent<>();
			keyedCepEvent1.setKey(orderId1);
			keyedCepEvent1.setEvent(e1);
			keyedCepEvent1.setPatternProcessorIds(new HashSet<>(Arrays.asList(patternProcessor2.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent1, 1));

			EventV2 e2 = new EventV2(orderId1, CREATE_ORDER, new HashMap<>(), 2);
			KeyedCepEvent<EventV2> keyedCepEvent2 = new KeyedCepEvent<>();
			keyedCepEvent2.setKey(orderId1);
			keyedCepEvent2.setEvent(e2);
			keyedCepEvent2.setPatternProcessorIds(new HashSet<>(Arrays.asList(patternProcessor1.getId(), patternProcessor2.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent2, 2));
			harness.processWatermark1(new Watermark(5));
			Queue<Object> result0 = harness.getOutput();
			assertEquals(3, result0.size());
			verifyWatermark(result0.poll(), 0);
			verifyPattern(result0.poll(), patternProcessor2.getId(), e1, null, e2);
			verifyWatermark(result0.poll(), 5);
			EventV2 e3 = new EventV2(orderId1, PAY_ORDER, new HashMap<>(), 6);
			KeyedCepEvent<EventV2> keyedCepEvent3 = new KeyedCepEvent<>();
			keyedCepEvent3.setKey(orderId1);
			keyedCepEvent3.setEvent(e3);
			keyedCepEvent3.setPatternProcessorIds(new HashSet<>(Arrays.asList(patternProcessor1.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent3, 6));

			harness.processWatermark1(new Watermark(10));

			Queue<Object> result1 = harness.getOutput();
			assertEquals(2, result1.size());
			verifyPattern(result1.poll(), patternProcessor1.getId(), e2, null,  e3);
			verifyWatermark(result1.poll(), 10);
			PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> patternProcessor3 = createTestPatternProcessor33();
			harness.processElement2(new StreamRecord(patternProcessor3));
			String  userId1 = "user_id1";
			HashMap<String, Object> map = new HashMap<String, Object>();
			map.put(USER_ID, userId1);
			map.put(ORDER_ID, orderId1);
			EventV2 e4 = new EventV2(orderId1, PRODUCT_WATCH, map, 11);
			KeyedCepEvent<EventV2> keyedCepEvent4 = new KeyedCepEvent<>();
			keyedCepEvent4.setKey(userId1);
			keyedCepEvent4.setEvent(e4);
			keyedCepEvent4.setPatternProcessorIds(new HashSet<>(Arrays.asList(patternProcessor3.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent4, 11));
			EventV2 e5 = new EventV2(orderId1, CREATE_ORDER, map, 12);
			KeyedCepEvent<EventV2> keyedCepEvent5 = new KeyedCepEvent<>();
			keyedCepEvent5.setKey(orderId1);
			keyedCepEvent5.setEvent(e5);
			keyedCepEvent5.setPatternProcessorIds(new HashSet<>(Arrays.asList(patternProcessor1.getId(), patternProcessor2.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent5, 12));
			EventV2 e6 = new EventV2(orderId1, PRODUCT_CLICK, map, 13);
			KeyedCepEvent<EventV2> keyedCepEvent6 = new KeyedCepEvent<>();
			keyedCepEvent6.setKey(userId1);
			keyedCepEvent6.setEvent(e6);
			keyedCepEvent6.setPatternProcessorIds(new HashSet<>(Arrays.asList(patternProcessor3.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent6, 13));
			EventV2 e7 = new EventV2(orderId1, PAY_ORDER, map, 14);
			KeyedCepEvent<EventV2> keyedCepEvent7 = new KeyedCepEvent<>();
			keyedCepEvent7.setKey(orderId1);
			keyedCepEvent7.setEvent(e7);
			keyedCepEvent7.setPatternProcessorIds(new HashSet<>(Arrays.asList(patternProcessor1.getId())));
			harness.processElement1(new StreamRecord(keyedCepEvent7, 14));
			harness.processWatermark1(new Watermark(15));
			Queue<Object> result2 = harness.getOutput();
			verifyPattern(result2.poll(), patternProcessor3.getId(), e4, null, e6);
			verifyPattern(result2.poll(), patternProcessor1.getId(), e5, null, e7);
			verifyWatermark(result2.poll(), 15);
		} finally {
			harness.close();
		}
	}

	private void verifyPattern(Object outputObject,  String patternId, EventV2 start , EventV2 middle , EventV2 end) throws Exception {
		assertTrue(outputObject instanceof StreamRecord);

		StreamRecord<?> resultRecord = (StreamRecord<?>) outputObject;
		assertTrue(resultRecord.getValue() instanceof Tuple2);
		Tuple2<String, Map<String, List<EventV2>>> tuple2 = (Tuple2<String, Map<String, List<EventV2>>>) resultRecord.getValue();
		tuple2.f0.equals(patternId);
		Map<String, List<EventV2>> patternMap = tuple2.f1;
		if (start != null && patternMap.get(START) != null) {
			assertEquals(start, patternMap.get(START).get(0));
		}

		if (middle != null && patternMap.get(MIDDLE) != null) {
			assertEquals(middle, patternMap.get(MIDDLE).get(0));
		}
		if (end != null && patternMap.get(END) != null) {
			assertEquals(end, patternMap.get(END).get(0));
		}
	}

	private void verifyWatermark(Object outputObject, long timestamp) {
		assertTrue(outputObject instanceof Watermark);
		assertEquals(timestamp, ((Watermark) outputObject).getTimestamp());
	}

	private PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> createTestPatternProcessor() {
		TestPatternProcessor patternProcessor = new TestPatternProcessor();

		patternProcessor.setRuleId("111");
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

		Pattern<EventV2, EventV2> pattern = Pattern.<EventV2>begin(START).where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals(WATCH_PRODUCT);
				}
			}
		).next(MIDDLE).where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals(CREATE_ORDER);
				}
			}
		).followedBy(END).where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals(PAY_ORDER);
				}
			}
		).within(Time.seconds(5));
		patternProcessor.setPattern(pattern);

		MultiplePatternProcessFunctionV2<EventV2, Tuple2<String, Map<String, List<EventV2>>>> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor.setFunctionV2(functionV2);

		return patternProcessor;
	}

	private PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> createTestPatternProcessor31() {
		TestPatternProcessor patternProcessor = new TestPatternProcessor();
		patternProcessor.setRuleId("1");
		patternProcessor.setVersion(1);
		patternProcessor.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals(CREATE_ORDER)
					|| event.getEventName().equals(PAY_ORDER);
			}
		};
		patternProcessor.setEventMatcher(eventMatcher);

		Pattern<EventV2, EventV2> pattern = Pattern.<EventV2>begin(START).where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals(CREATE_ORDER);
				}
			}
		).followedBy(END).where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals(PAY_ORDER);
				}
			}
		).within(Time.seconds(10));
		patternProcessor.setPattern(pattern);

		MultiplePatternProcessFunctionV2<EventV2, Tuple2<String, Map<String, List<EventV2>>>> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor.setFunctionV2(functionV2);
		return patternProcessor;
	}

	private PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> createTestPatternProcessor32() {
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

		Pattern<EventV2, EventV2> pattern = Pattern.<EventV2>begin(START).where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals(WATCH_LIVE_ROOM);
				}
			}
		).followedBy(END).where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals(CREATE_ORDER);
				}
			}
		).within(Time.seconds(10));
		patternProcessor.setPattern(pattern);

		MultiplePatternProcessFunctionV2<EventV2, Tuple2<String, Map<String, List<EventV2>>>> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor.setFunctionV2(functionV2);
		return patternProcessor;
	}

	private PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> createTestPatternProcessor33() {
		Map<String, String> eventPartitionFiledMap = new HashMap<>();
		eventPartitionFiledMap.put(PRODUCT_WATCH, USER_ID);
		eventPartitionFiledMap.put(PRODUCT_CLICK, USER_ID);
		TestPatternProcessor patternProcessor = new TestPatternProcessor();
		patternProcessor.setRuleId("3");
		patternProcessor.setVersion(1);
		patternProcessor.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals(PRODUCT_WATCH) || event.getEventName().equals(PRODUCT_CLICK);
			}
		};
		patternProcessor.setEventMatcher(eventMatcher);
		KeySelector<EventV2, Object> keySelector = new KeySelector<EventV2, Object>() {

			@Override
			public Object getKey(EventV2 value) throws Exception {
				return value.getEventProps().get(eventPartitionFiledMap.get(value.getEventName()));
			}
		};
		patternProcessor.setKeySelector(keySelector);

		Pattern<EventV2, EventV2> pattern = Pattern.<EventV2>begin(START).where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals(PRODUCT_WATCH);
				}
			}
		).followedBy(END).where(
				new SimpleCondition<EventV2>() {
					@Override
					public boolean filter(EventV2 event) {
						return event.getEventName().equals(PRODUCT_CLICK);
					}
				}
			).within(Time.seconds(10));
		patternProcessor.setPattern(pattern);

		MultiplePatternProcessFunctionV2<EventV2, Tuple2<String, Map<String, List<EventV2>>>> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor.setFunctionV2(functionV2);
		return patternProcessor;
	}

	static class TestPatternProcessor implements PatternProcessor<EventV2, Tuple2<String, Map<String, List<EventV2>>>> {

		private String ruleId;
		private int version;
		private Boolean isAlive;
		private EventMatcher<EventV2> eventMatcher;
		private KeySelector<EventV2, Object> keySelector;
		private Pattern<EventV2, EventV2> pattern;
		private MultiplePatternProcessFunctionV2<EventV2, Tuple2<String, Map<String, List<EventV2>>>> functionV2;

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
		public MultiplePatternProcessFunctionV2<EventV2, Tuple2<String, Map<String, List<EventV2>>>> getPatternProcessFunction() {
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

		public void setFunctionV2(MultiplePatternProcessFunctionV2<EventV2, Tuple2<String, Map<String, List<EventV2>>>> functionV2) {
			this.functionV2 = functionV2;
		}
	}

	static class TestMultiplePatternProcessFunctionV2 extends MultiplePatternProcessFunctionV2<EventV2, Tuple2<String, Map<String, List<EventV2>>>> implements MultiplePatternTimedOutPartialMatchHandlerV2<EventV2> {

		@Override
		public void open(Configuration parameters) throws Exception {
			LOG.trace("Init MultiplePatternProcessFunctionV2!");
		}

		@Override
		public void processMatch(Tuple2<String, Map<String, List<EventV2>>> match, Context ctx, Object key, Collector<Tuple2<String, Map<String, List<EventV2>>>> out) throws Exception {
			out.collect(match);
		}

		@Override
		public void processUnMatch(EventV2 event, Context ctx, Object key, Collector<Tuple2<String, Map<String, List<EventV2>>>> out) {
			LOG.trace("output process unmatch, patternId: " + ctx.currentPattern().getPatternId());
		}

		@Override
		public void close() throws Exception {
			LOG.trace("Close MultiplePatternProcessFunctionV2!");
		}

		@Override
		public void processTimedOutMatch(Tuple2<String, Map<String, List<EventV2>>> match, Object key, Context ctx) throws Exception {
			LOG.trace("timeout, current eventTs: " + ctx.timestamp() + ", patternId: " + ctx.currentPattern().getPatternId());
		}
	}
}
