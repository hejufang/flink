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
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.PatternProcessor;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@After
	public void validate() {
		validateMockitoUsage();
	}

	@Test
	public void testKeyedCEPOperatorWatermarkForwarding() throws Exception {
		TwoInputStreamOperatorTestHarness harness = getCepKeyGenOperatorTestHarness(getCepKeyGenOperator());

		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		PatternProcessor<EventV2, String> testPatternProcessor = createTestPatternProcessor();
		harness.setStateBackend(rocksDBStateBackend);
		harness.open();
		harness.processWatermark1(new Watermark(0));

		harness.processElement2(new StreamRecord(testPatternProcessor));

		EventV2 e1 = new EventV2(1, "WATCH_PRODUCT", new HashMap<>(), 1000);
		harness.processElement1(new StreamRecord(e1, 1000));

		EventV2 e2 = new EventV2(1, "CREATE_ORDER", new HashMap<>(), 2000);
		harness.processElement1(new StreamRecord(e2, 2000));

		EventV2 e3 = new EventV2(1, "PAY_ORDER", new HashMap<>(), 3000);
		harness.processElement1(new StreamRecord(e3, 3000));

		harness.processWatermark1(new Watermark(3600000));
		Queue<Object> result = harness.getOutput();

		assertEquals(3, result.size());
	}

	@Test
	public void testClearStateWhenDisablePattern() throws Exception {
		TwoInputStreamOperatorTestHarness harness = getCepKeyGenOperatorTestHarness(getCepKeyGenOperator());

		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		PatternProcessor<EventV2, String> testPatternProcessor = createTestPatternProcessor();
		harness.setStateBackend(rocksDBStateBackend);
		harness.open();
		harness.processWatermark1(new Watermark(0));

		harness.processElement2(new StreamRecord(testPatternProcessor));

		EventV2 e1 = new EventV2(1, "WATCH_PRODUCT", new HashMap<>(), 1);
		harness.processElement1(new StreamRecord(e1, 1));

		EventV2 e2 = new EventV2(1, "CREATE_ORDER", new HashMap<>(), 2);
		harness.processElement1(new StreamRecord(e2, 2));

		harness.processWatermark1(new Watermark(5));

		BroadcastState patternStates = ((CepKeyGenOperator) harness.getOperator()).getPatternStates();

		assertTrue(patternStates.contains("111"));
		((TestPatternProcessor) testPatternProcessor).setIsAlive(false);
		harness.processElement2(new StreamRecord(testPatternProcessor));

		assertFalse(patternStates.contains("111"));
	}

	@Test
	public void testMultiPattern() throws Exception {
		TwoInputStreamOperatorTestHarness harness = getCepKeyGenOperatorTestHarness(getCepKeyGenOperator());

		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		harness.setStateBackend(rocksDBStateBackend);
		harness.open();
		harness.processWatermark1(new Watermark(0));

		// create first test PatternProcessor.
		TestPatternProcessor patternProcessor1 = new TestPatternProcessor();
		patternProcessor1.setRuleId("1");
		patternProcessor1.setVersion(1);
		patternProcessor1.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher1 = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals("CREATE_ORDER")
					|| event.getEventName().equals("PAY_ORDER");
			}
		};
		patternProcessor1.setEventMatcher(eventMatcher1);
		KeySelector<EventV2, Object> keySelector = new KeySelector<EventV2, Object>() {

			@Override
			public Object getKey(EventV2 value) throws Exception {
				return value.getEventId();
			}
		};
		patternProcessor1.setKeySelector(keySelector);

		Pattern<EventV2, EventV2> pattern1 = Pattern.<EventV2>begin("begin").where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals("CREATE_ORDER");
				}
			}
		).followedBy("end").where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals("PAY_ORDER");
				}
			}
		).within(Time.seconds(10));
		patternProcessor1.setPattern(pattern1);

		MultiplePatternProcessFunctionV2<EventV2, String> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor1.setFunctionV2(functionV2);

		// Create second test PatternProcessor.
		TestPatternProcessor patternProcessor2 = new TestPatternProcessor();
		patternProcessor2.setRuleId("2");
		patternProcessor2.setVersion(1);
		patternProcessor2.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher2 = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals("WATCH_LIVE_ROOM")
					|| event.getEventName().equals("CREATE_ORDER");
			}
		};
		patternProcessor2.setEventMatcher(eventMatcher2);
		KeySelector<EventV2, Object> keySelector2 = new KeySelector<EventV2, Object>() {

			@Override
			public Object getKey(EventV2 value) throws Exception {
				return value.getEventId();
			}
		};
		patternProcessor2.setKeySelector(keySelector2);

		Pattern<EventV2, EventV2> pattern2 = Pattern.<EventV2>begin("begin").where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals("WATCH_LIVE_ROOM");
				}
			}
		).followedBy("end").where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals("CREATE_ORDER");
				}
			}
		).within(Time.seconds(10));
		patternProcessor2.setPattern(pattern2);

		patternProcessor2.setFunctionV2(functionV2);

		harness.processElement2(new StreamRecord(patternProcessor1));
		harness.processElement2(new StreamRecord(patternProcessor2));

		// Trigger PatternProcessors by events
		EventV2 e1 = new EventV2(1, "WATCH_LIVE_ROOM", new HashMap<>(), 1);
		harness.processElement1(new StreamRecord(e1, 1));

		EventV2 e2 = new EventV2(1, "CREATE_ORDER", new HashMap<>(), 2);
		harness.processElement1(new StreamRecord(e2, 2));
		harness.processWatermark1(new Watermark(5));
		Queue<Object> result0 = harness.getOutput();
		assertEquals(2, result0.size());
		EventV2 e3 = new EventV2(1, "PAY_ORDER", new HashMap<>(), 6);
		harness.processElement1(new StreamRecord(e3, 6));

		harness.processWatermark1(new Watermark(10));

		Queue<Object> result1 = harness.getOutput();
		assertEquals(3, result1.size());
		EventV2 e4 = new EventV2(1, "PAY_ORDER_TEST", new HashMap<>(), 9);
		harness.processElement1(new StreamRecord(e4, 9));

		harness.processWatermark1(new Watermark(15));

		Queue<Object> result2 = harness.getOutput();
		assertEquals(3, result2.size());

	}

	private PatternProcessor<EventV2, String> createTestPatternProcessor() {
		TestPatternProcessor patternProcessor = new TestPatternProcessor();

		patternProcessor.setRuleId("111");
		patternProcessor.setVersion(1);
		patternProcessor.setIsAlive(true);

		EventMatcher<EventV2> eventMatcher = new EventMatcher<EventV2>() {
			public boolean isMatch(EventV2 event) {
				return event.getEventName().equals("WATCH_PRODUCT")
					|| event.getEventName().equals("CREATE_ORDER")
					|| event.getEventName().equals("PAY_ORDER");
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

		Pattern<EventV2, EventV2> pattern = Pattern.<EventV2>begin("start").where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals("WATCH_PRODUCT");
				}
			}
		).next("middle").where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals("CREATE_ORDER");
				}
			}
		).followedBy("end").where(
			new SimpleCondition<EventV2>() {
				@Override
				public boolean filter(EventV2 event) {
					return event.getEventName().equals("PAY_ORDER");
				}
			}
		).within(Time.seconds(5));
		patternProcessor.setPattern(pattern);

		MultiplePatternProcessFunctionV2<EventV2, String> functionV2 = new TestMultiplePatternProcessFunctionV2();
		patternProcessor.setFunctionV2(functionV2);

		return patternProcessor;
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

	static class TestMultiplePatternProcessFunctionV2 extends MultiplePatternProcessFunctionV2<EventV2, String> implements MultiplePatternTimedOutPartialMatchHandlerV2<EventV2> {

		@Override
		public void open(Configuration parameters) throws Exception {
			LOG.trace("Init MultiplePatternProcessFunctionV2!");
		}

		@Override
		public void processMatch(Tuple2<String, Map<String, List<EventV2>>> match, Context ctx, Object key, Collector<String> out) throws Exception {
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
		public void processTimedOutMatch(Tuple2<String, Map<String, List<EventV2>>> match, Object key, Context ctx) throws Exception {
			LOG.trace("timeout, current eventTs: " + ctx.timestamp() + ", patternId: " + ctx.currentPattern().getPatternId());
		}
	}
}
