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
import org.apache.flink.cep.pattern.EventMatcher;
import org.apache.flink.cep.pattern.KeyedCepEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.PatternProcessor;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
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

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cep.operator.CepKeyGenOperatorTestUtilities.getCepKeyGenOperator;
import static org.apache.flink.cep.operator.CepKeyGenOperatorTestUtilities.getCepKeyGenOperatorTestHarness;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.validateMockitoUsage;

/**
 * Tests for {@link CepKeyGenOperator}.
 */
@Ignore
public class CepKeyGenOperatorTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(CepKeyGenOperatorTest.class);
	private static final String ORDER_ID = "order_id";
	private static final String USER_ID = "user_id";
	private static final String WATCH_LIVE_ROOM = "WATCH_LIVE_ROOM";
	private static final String WATCH_PRODUCT = "WATCH_PRODUCT";
	private static final String CREATE_ORDER = "CREATE_ORDER";
	private static final String PAY_ORDER = "PAY_ORDER";
	private static final String REFUND = "REFUND";

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@After
	public void validate() {
		validateMockitoUsage();
	}

	@Test
	public void testSingePattern() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCepKeyGenOperatorTestHarness(getCepKeyGenOperator());
		try {
			String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
			RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
			rocksDBStateBackend.setDbStoragePath(rocksDbPath);

			PatternProcessor<EventV2, String> testPatternProcessor = createTestPatternProcessor();
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();

			harness.processElement2(new StreamRecord(testPatternProcessor));

			EventV2 e1 = new EventV2(1, WATCH_PRODUCT, new HashMap<>(), 1000);
			harness.processElement1(new StreamRecord(e1, e1.getEventTs()));

			EventV2 e2 = new EventV2(1, CREATE_ORDER, new HashMap<>(), 2000);
			harness.processElement1(new StreamRecord(e2, e2.getEventTs()));

			EventV2 e3 = new EventV2(1, PAY_ORDER, new HashMap<>(), 3000);
			harness.processElement1(new StreamRecord(e3, e3.getEventTs()));
			EventV2 e4 = new EventV2(1, REFUND, new HashMap<>(), 4000);
			harness.processElement1(new StreamRecord(e4, e4.getEventTs()));
			EventV2 f4 = new EventV2(2, WATCH_PRODUCT, new HashMap<>(), 4000);
			harness.processElement1(new StreamRecord(f4, f4.getEventTs()));

			Queue<Object> result = harness.getOutput();

			assertEquals(4, result.size());

			Set<String> set = Arrays.stream(new String[] {testPatternProcessor.getId()}).collect(Collectors.toSet());
			verifyPattern(result.poll(), e1, set);
			verifyPattern(result.poll(), e2, set);
			verifyPattern(result.poll(), e3, set);
			verifyPattern(result.poll(), f4, set);
		} finally {
			harness.close();
		}
	}

	@Test
	public void testClearStateWhenDisablePattern() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCepKeyGenOperatorTestHarness(getCepKeyGenOperator());
		try {
			String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
			RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
			rocksDBStateBackend.setDbStoragePath(rocksDbPath);

			PatternProcessor<EventV2, String> testPatternProcessor = createTestPatternProcessor();
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();

			harness.processElement2(new StreamRecord(testPatternProcessor));
			Set<String> set = Arrays.stream(new String[] {testPatternProcessor.getId()}).collect(Collectors.toSet());

			EventV2 e1 = new EventV2(1, WATCH_PRODUCT, new HashMap<>(), 1);
			harness.processElement1(new StreamRecord(e1, 1));

			EventV2 e2 = new EventV2(1, CREATE_ORDER, new HashMap<>(), 2);
			harness.processElement1(new StreamRecord(e2, 2));

			BroadcastState patternStates = ((CepKeyGenOperator) harness.getOperator()).getPatternStates();
			assertTrue(patternStates.contains(testPatternProcessor.getId()));
			Queue<Object> result0 = harness.getOutput();
			assertEquals(2, result0.size());
			verifyPattern(result0.poll(), e1, set);
			verifyPattern(result0.poll(), e2, set);
			((TestPatternProcessor) testPatternProcessor).setIsAlive(false);
			harness.processElement2(new StreamRecord(testPatternProcessor));
			EventV2 e3 = new EventV2(1, PAY_ORDER, new HashMap<>(), 3);
			harness.processElement1(new StreamRecord(e3, e3.getEventTs()));

			assertFalse(patternStates.contains(testPatternProcessor.getId()));
			Queue<Object> result1 = harness.getOutput();
			assertEquals(0, result1.size());
		} finally {
			harness.close();
		}
	}

	@Test
	public void testMultiPattern() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCepKeyGenOperatorTestHarness(getCepKeyGenOperator());

		try {
			String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
			RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
			rocksDBStateBackend.setDbStoragePath(rocksDbPath);
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();

			// create first test PatternProcessor.
			PatternProcessor<EventV2, String> patternProcessor31 = createTestPatternProcessor31();

			// Create second test PatternProcessor.
			PatternProcessor<EventV2, String> patternProcessor32 = createTestPatternProcessor32();

			harness.processElement2(new StreamRecord(patternProcessor31));
			harness.processElement2(new StreamRecord(patternProcessor32));
			Set<String> set31 = Arrays.stream(new String[] {patternProcessor31.getId()}).collect(Collectors.toSet());
			Set<String> set32 = Arrays.stream(new String[] {patternProcessor32.getId()}).collect(Collectors.toSet());
			Set<String> set312 = Arrays.stream(new String[] {patternProcessor31.getId(), patternProcessor32.getId()}).collect(Collectors.toSet());

			// Trigger PatternProcessors by events
			EventV2 e1 = new EventV2(1, WATCH_LIVE_ROOM, new HashMap<>(), 1);
			harness.processElement1(new StreamRecord(e1, e1.getEventTs()));
			EventV2 e2 = new EventV2(1, CREATE_ORDER, new HashMap<>(), 2);
			harness.processElement1(new StreamRecord(e2, e2.getEventTs()));
			Queue<Object> result0 = harness.getOutput();
			assertEquals(2, result0.size());
			verifyPattern(result0.poll(), e1, set32);
			verifyPattern(result0.poll(), e2, set312);
			EventV2 e3 = new EventV2(1, PAY_ORDER, new HashMap<>(), 6);
			harness.processElement1(new StreamRecord(e3, e3.getEventTs()));
			Queue<Object> result1 = harness.getOutput();
			assertEquals(1, result1.size());
			verifyPattern(result1.poll(), e3, set31);
			EventV2 e4 = new EventV2(1, "REFOUND", new HashMap<>(), 9);
			harness.processElement1(new StreamRecord(e4, e4.getEventTs()));
			Queue<Object> result2 = harness.getOutput();
			assertEquals(0, result2.size());
			PatternProcessor<EventV2, String> patternProcessor33 = createTestPatternProcessor33();
			harness.processElement2(new StreamRecord(patternProcessor33));
			Set<String> set33 = Arrays.stream(new String[] {patternProcessor33.getId()}).collect(Collectors.toSet());
			HashMap<String, Object> map = new HashMap<String, Object>();
			map.put(USER_ID, "user_id1");
			map.put(ORDER_ID, 1);
			EventV2 e5 = new EventV2(1, CREATE_ORDER, map, 10);
			harness.processElement1(new StreamRecord(e5, e5.getEventTs()));
			Queue<Object> result3 = harness.getOutput();
			assertEquals(2, result3.size());
			verifyPattern(result3.poll(), e5, set33);
			verifyPattern(result3.poll(), e5, set312);
		} finally {
			harness.close();
		}
	}

	private PatternProcessor<EventV2, String> createTestPatternProcessor() {
		TestPatternProcessor patternProcessor = new TestPatternProcessor();

		patternProcessor.setRuleId("111");
		patternProcessor.setVersion(1);
		patternProcessor.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals(WATCH_PRODUCT) || event.getEventName().equals(CREATE_ORDER)
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
				return event.getEventName().equals("PAY_ORDER");
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

	private PatternProcessor<EventV2, String> createTestPatternProcessor33() {
		Map<String, String> eventPartitionFiledMap = new HashMap<>();
		eventPartitionFiledMap.put(PAY_ORDER, ORDER_ID);
		eventPartitionFiledMap.put(CREATE_ORDER, USER_ID);
		TestPatternProcessor patternProcessor = new TestPatternProcessor();

		patternProcessor.setRuleId("3");
		patternProcessor.setVersion(1);
		patternProcessor.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals(PAY_ORDER)
						|| event.getEventName().equals(CREATE_ORDER);
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

	private void verifyPattern(Object outputObject, EventV2 eventV2, Set<String> set) throws Exception {
		assertTrue(outputObject instanceof StreamRecord);

		StreamRecord<?> resultRecord = (StreamRecord<?>) outputObject;
		assertTrue(resultRecord.getValue() instanceof KeyedCepEvent);

		KeyedCepEvent keyedCepEvent = (KeyedCepEvent) resultRecord.getValue();

		assertTrue(isObjectFieldsEqual(eventV2, keyedCepEvent.getEvent()));
//		assertEquals(eventV2.getEventId(), keyedCepEvent.getKey());
		assertTrue(isSetEqual(set, keyedCepEvent.getPatternProcessorIds()));
	}

	public static boolean isObjectFieldsEqual(Object oldObject, Object newObject) throws Exception {
		Map<String, Map<String, Object>> map = null;

		if (oldObject.getClass() == newObject.getClass()) {
			map = new HashMap<String, Map<String, Object>>();

			Class clazz = oldObject.getClass();
			PropertyDescriptor[] pds = Introspector.getBeanInfo(clazz, Object.class).getPropertyDescriptors();

			for (PropertyDescriptor pd : pds) {
				String name = pd.getName();
				Method readMethod = pd.getReadMethod();
				Object oldValue = readMethod.invoke(oldObject);
				Object newValue = readMethod.invoke(newObject);
				if (oldValue instanceof List || newValue instanceof List) {
					continue;
				}
				if (oldValue instanceof Timestamp) {
					oldValue = new Date(((Timestamp) oldValue).getTime());
				}
				if (newValue instanceof Timestamp) {
					newValue = new Date(((Timestamp) newValue).getTime());
				}
				if (oldValue == null && newValue == null) {
					continue;
				} else if (oldValue == null && newValue != null) {
					Map<String, Object> valueMap = new HashMap<String, Object>();
					valueMap.put("oldValue", oldValue);
					valueMap.put("newValue", newValue);
					map.put(name, valueMap);
					continue;
				}
				if (!oldValue.equals(newValue)) {
					Map<String, Object> valueMap = new HashMap<String, Object>();
					valueMap.put("oldValue", oldValue);
					valueMap.put("newValue", newValue);
					map.put(name, valueMap);
				}
			}
		}

		return (map != null && map.isEmpty()) ? true : false;
	}

	public static boolean isSetEqual(Set set1, Set set2) {
		if (set1 == null && set2 == null) {
			return true;
		}
		if (set1 == null || set2 == null || set1.size() != set2.size() || set1.size() == 0 || set2.size() == 0) {
			return false;
		}
		Iterator ite1 = set1.iterator();
		Iterator ite2 = set2.iterator();
		boolean isFullEqual = true;
		while (ite2.hasNext()) {
			if (!set1.contains(ite2.next())) {
				isFullEqual = false;
			}
		}
		return isFullEqual;
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

	static class TestMultiplePatternProcessFunctionV2 extends MultiplePatternProcessFunctionV2<EventV2, String>
			implements MultiplePatternTimedOutPartialMatchHandlerV2<EventV2> {

		@Override
		public void open(Configuration parameters) throws Exception {
			LOG.trace("Init MultiplePatternProcessFunctionV2!");
		}

		@Override
		public void processMatch(Tuple2<String, Map<String, List<EventV2>>> match, Context ctx, Object key,
				Collector<String> out) throws Exception {
			out.collect("output process matched, patternId: " + ctx.currentPattern().getPatternId());
		}

		@Override
		public void processUnMatch(EventV2 event, Context ctx, Object key, Collector<String> out) {
			out.collect("output process unmatch, patternId: " + ctx.currentPattern().getPatternId());
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
