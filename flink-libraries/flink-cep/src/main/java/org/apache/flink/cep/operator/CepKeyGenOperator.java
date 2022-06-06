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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.cep.pattern.EventMatcher;
import org.apache.flink.cep.pattern.KeyedCepEvent;
import org.apache.flink.cep.pattern.PatternProcessor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * The CepKeyGenOperator which can associate an event with rules(PatternProcessor).
 */
public class CepKeyGenOperator<IN> extends AbstractStreamOperator<KeyedCepEvent<IN>>
		implements TwoInputStreamOperator<IN, PatternProcessor<IN, ?>, KeyedCepEvent<IN>> {

	private static final long serialVersionUID = 1L;
	private BroadcastState<String, PatternProcessor> patternStates;
	private static final String PATTERN_STATE = "patternState";
	private Map<Object, Set<String>>  partitionKeyMatchedPatternIdsMap;

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		patternStates = context.getOperatorStateStore().getBroadcastState(
			new MapStateDescriptor<>(PATTERN_STATE, new StringSerializer(),
					new KryoSerializer<>(PatternProcessor.class, new ExecutionConfig())));
	}

	@Override
	public void open() throws Exception {
		super.open();
		partitionKeyMatchedPatternIdsMap = new HashMap<>();
	}

	@Override
	public void processElement1(StreamRecord<IN> element) throws Exception {
		IN in = element.getValue();
		partitionKeyMatchedPatternIdsMap.clear();
		Iterator<Map.Entry<String, PatternProcessor>> iter = patternStates.iterator();
		while (iter.hasNext()) {
			PatternProcessor patternProcessor = iter.next().getValue();
			if (patternProcessor != null) {
				EventMatcher<IN> eventMatcher = patternProcessor.getEventMatcher();
				if (eventMatcher.isMatch(in)) {
					String patternId = patternProcessor.getId();
					KeySelector<IN, ?> keySelector = patternProcessor.getKeySelector();
					if (keySelector != null) {
						Object key = keySelector.getKey(in);
						Set<String> patternIdSet =  partitionKeyMatchedPatternIdsMap.getOrDefault(key, new HashSet<>());

						patternIdSet.add(patternId);
						partitionKeyMatchedPatternIdsMap.put(key, patternIdSet);
					} else {
						LOG.warn("keySelector is null(pattern:id={},version={})", patternProcessor.getId(),
								patternProcessor.getVersion());
					}
				}
			}
		}

		partitionKeyMatchedPatternIdsMap.forEach((key, set) -> {
			KeyedCepEvent<IN> keyedCepEvent = new KeyedCepEvent<>();
			keyedCepEvent.setEvent(in);
			keyedCepEvent.setKey(key);
			keyedCepEvent.setPatternProcessorIds(set);
			output.collect(new StreamRecord<>(keyedCepEvent, element.getTimestamp()));
		});
	}

	@Override
	public void processElement2(StreamRecord<PatternProcessor<IN, ?>> element) throws Exception {
		PatternProcessor<IN, ?> patternProcessor = element.getValue();
		final String patternId = patternProcessor.getId();
		if (!patternProcessor.getIsAlive()) {
			disableOldPattern(patternId);
			return;
		}
		if (this.patternStates.contains(patternId) && this.patternStates.get(patternId).getVersion()
			== patternProcessor.getVersion()) {
			return;
		}
		LOG.info("Initialize a new pattern from upstream(id={},version={})", patternProcessor.getId(),
			patternProcessor.getVersion());
		initializeNewPattern(patternProcessor);
	}

	private void disableOldPattern(String patternId) throws Exception {
		this.patternStates.remove(patternId);
	}

	private void initializeNewPattern(PatternProcessor<IN, ?> patternProcessor) throws Exception {
		String patternId = patternProcessor.getId();
		this.patternStates.put(patternId, patternProcessor);
	}

	public BroadcastState<String, PatternProcessor> getPatternStates() {
		return patternStates;
	}
}
