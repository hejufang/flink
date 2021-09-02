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

import org.apache.flink.cep.Event;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Map;
import java.util.Queue;

import static org.apache.flink.cep.operator.CoCepOperatorTestUtilities.getCoCepTestHarness;
import static org.apache.flink.cep.operator.CoCepOperatorTestUtilities.getCoKeyedCepOpearator;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.validateMockitoUsage;

/**
 * Tests for {@link CepOperator}.
 */
public class CoCepOperatorTest extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@After
	public void validate() {
		validateMockitoUsage();
	}

	@Test
	public void testKeyedCEPOperatorWatermarkForwarding() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCoCepTestHarness(getCoKeyedCepOpearator(false));

		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		Pattern<Event, Event> pattern = Pattern.begin("start");
		try {
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();
			harness.processWatermark1(new Watermark(0));

			harness.processElement2(new StreamRecord(pattern));
			harness.processElement1(new StreamRecord(new Event(1, "a", 0.0), 1));
			harness.processElement1(new StreamRecord(new Event(1, "b", 0.0), 5));
			harness.processWatermark1(new Watermark(30));
			Queue<Object> result = harness.getOutput();

			assertEquals(4, result.size());
		} finally {
			harness.close();
		}
	}

	@Test
	public void testStateWithMultiplePattern() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCoCepTestHarness(getCoKeyedCepOpearator(false));
		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		Pattern<Event, Event> singlePattern1 = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		Pattern<Event, Event> singlePattern2 = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
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

		singlePattern1.setPatternMeta("patternId1", 0);
		singlePattern2.setPatternMeta("patternId2", 1);

		try {
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();
			harness.processWatermark1(new Watermark(0));

			harness.processElement2(new StreamRecord(singlePattern1));
			harness.processElement2(new StreamRecord(singlePattern2));

			harness.processWatermark1(new Watermark(0));
			harness.processElement1(new StreamRecord(new Event(1, "start", 0.0), 1));
			harness.processElement1(new StreamRecord(new Event(1, "middle", 0.0), 5));
			harness.processWatermark1(new Watermark(10));

			Map<String, SharedBuffer> partialMatches = ((CoCepOperator) harness.getOperator()).getPartialMatches();
			assertEquals(2, partialMatches.get("patternId1").getEventsBufferSize());
			assertEquals(2, partialMatches.get("patternId1").getSharedBufferNodeSize());

			assertEquals(2, partialMatches.get("patternId2").getEventsBufferSize());
			assertEquals(2, partialMatches.get("patternId2").getSharedBufferNodeSize());
		} finally {
			harness.close();
		}
	}

	@Test
	public void testClearStateWhenDisablePattern() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCoCepTestHarness(getCoKeyedCepOpearator(false));
		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);

		Pattern<Event, Event> singlePattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		singlePattern.setPatternMeta("patternId", 0);

		try {
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();
			harness.processWatermark1(new Watermark(0));

			harness.processElement2(new StreamRecord(singlePattern));
			harness.processWatermark1(new Watermark(0));
			harness.processElement1(new StreamRecord(new Event(1, "start", 0.0), 1));
			harness.processElement1(new StreamRecord(new Event(1, "middle", 0.0), 5));
			harness.processWatermark1(new Watermark(10));

			Map<String, SharedBuffer> partialMatches = ((CoCepOperator) harness.getOperator()).getPartialMatches();
			assertEquals(2, partialMatches.get("patternId").getEventsBufferSize());
			assertEquals(2, partialMatches.get("patternId").getSharedBufferNodeSize());

			//disable Pattern
			singlePattern.setDisabled(true);
			harness.processElement2(new StreamRecord(singlePattern));
			SharedBuffer mockSharedBuffer = new SharedBuffer(new PerPatternKeyedStateStore("patternId", harness.getOperator().getKeyedStateBackend(), harness.getOperator().getExecutionConfig()), Event.createTypeSerializer());
			mockSharedBuffer.getAccessor().setCurrentNamespace("patternId");
			assertEquals(0, partialMatches.size());
			assertEquals(0, mockSharedBuffer.getSharedBufferNodeSize());
			assertEquals(0, mockSharedBuffer.getEventsBufferSize());

		} finally {
			harness.close();
		}
	}

	@Test
	public void testClearStateWhenUpdatePattern() throws Exception {
		KeyedTwoInputStreamOperatorTestHarness harness = getCoCepTestHarness(getCoKeyedCepOpearator(false));
		String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(new MemoryStateBackend());
		rocksDBStateBackend.setDbStoragePath(rocksDbPath);
		Pattern<Event, Event> singlePattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		singlePattern.setPatternMeta("patternId", 0);

		try {
			harness.setStateBackend(rocksDBStateBackend);
			harness.open();
			harness.processWatermark1(new Watermark(0));

			harness.processElement2(new StreamRecord(singlePattern));
			harness.processWatermark1(new Watermark(0));
			harness.processElement1(new StreamRecord(new Event(1, "start", 0.0), 1));
			harness.processElement1(new StreamRecord(new Event(1, "middle", 0.0), 5));
			harness.processWatermark1(new Watermark(10));

			Map<String, SharedBuffer> partialMatches = ((CoCepOperator) harness.getOperator()).getPartialMatches();
			assertEquals(2, partialMatches.get("patternId").getEventsBufferSize());
			assertEquals(2, partialMatches.get("patternId").getSharedBufferNodeSize());

			//update Pattern
			Pattern newPattern = Pattern.begin("start");
			newPattern.setPatternMeta("patternId", 1);
			harness.processElement2(new StreamRecord(newPattern));

			assertEquals(0, partialMatches.get("patternId").getEventsBufferSize());
			assertEquals(0, partialMatches.get("patternId").getSharedBufferNodeSize());

		} finally {
			harness.close();
		}
	}
}
