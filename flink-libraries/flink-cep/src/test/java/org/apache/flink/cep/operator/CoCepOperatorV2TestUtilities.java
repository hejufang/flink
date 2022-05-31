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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.EventV2;
import org.apache.flink.cep.pattern.KeyedCepEvent;
import org.apache.flink.cep.pattern.PatternProcessor;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

/**
 * Tests for {@link CoCepOperatorV2}.
 */
public class CoCepOperatorV2TestUtilities {

	public static KeyedTwoInputStreamOperatorTestHarness<Object, KeyedCepEvent<EventV2>, PatternProcessor<EventV2, ?>, Object> getCoCepTestHarness(
		TwoInputStreamOperator<KeyedCepEvent<EventV2>, PatternProcessor<EventV2, ?>, Object> coCepOperator) throws Exception {

		KeySelector<KeyedCepEvent<EventV2>, Object> keySelector = new KeySelector<KeyedCepEvent<EventV2>, Object>() {
			@Override
			public Object getKey(KeyedCepEvent<EventV2> eventV2) throws Exception {
				return eventV2.getKey();
			}
		};

		return new KeyedTwoInputStreamOperatorTestHarness<Object, KeyedCepEvent<EventV2>, PatternProcessor<EventV2, ?>, Object>(
			coCepOperator, keySelector, null, PojoTypeInfo.of(Object.class));
	}

	public static CoCepOperatorV2<EventV2> getCoCepOpearatorV2(
		boolean isProcessingTime) {

		return getCoCepOpearatorV2(isProcessingTime, null);
	}

	public static CoCepOperatorV2<EventV2> getCoCepOpearatorV2(
		boolean isProcessingTime,
		OutputTag<EventV2> outputTag) {

		return new CoCepOperatorV2<EventV2>(
			createEventV2Serializer(),
			createKeyedCepEventSerializer(),
			isProcessingTime,
			null,
			outputTag,
			null,
			new HashMap<>()
		);

	}

	private static TypeSerializer<EventV2> createEventV2Serializer() {
		TypeInformation<EventV2> typeInformation = (TypeInformation<EventV2>) TypeExtractor.createTypeInfo(EventV2.class);
		return typeInformation.createSerializer(new ExecutionConfig());
	}

	private static TypeSerializer<KeyedCepEvent<EventV2>> createKeyedCepEventSerializer() {
		KeyedCepEvent<EventV2> keyedCepEvent = new KeyedCepEvent<>();
		TypeInformation<KeyedCepEvent<EventV2>> typeInformation = (TypeInformation<KeyedCepEvent<EventV2>>) TypeExtractor.createTypeInfo(keyedCepEvent.getClass());
		return typeInformation.createSerializer(new ExecutionConfig());
	}
}
