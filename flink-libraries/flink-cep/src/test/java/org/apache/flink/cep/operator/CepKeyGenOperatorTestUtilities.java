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

import org.apache.flink.cep.EventV2;
import org.apache.flink.cep.pattern.KeyedCepEvent;
import org.apache.flink.cep.pattern.PatternProcessor;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;

/**
 * Tests for {@link CepKeyGenOperator}.
 */
public class CepKeyGenOperatorTestUtilities {

	public static TwoInputStreamOperatorTestHarness<EventV2, PatternProcessor<EventV2, ?>, KeyedCepEvent<EventV2>> getCepKeyGenOperatorTestHarness(
			TwoInputStreamOperator<EventV2, PatternProcessor<EventV2, ?>, KeyedCepEvent<EventV2>> cepKeyGenOperator) throws Exception {

			return new TwoInputStreamOperatorTestHarness<EventV2, PatternProcessor<EventV2, ?>, KeyedCepEvent<EventV2>>(
					cepKeyGenOperator);
		}

	public static CepKeyGenOperator<EventV2> getCepKeyGenOperator() {
		return new CepKeyGenOperator<>();
	}
}
