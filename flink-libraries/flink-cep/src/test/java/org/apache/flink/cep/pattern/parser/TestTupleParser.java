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

package org.apache.flink.cep.pattern.parser;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.cep.pattern.v2.ConditionGroup;
import org.apache.flink.cep.pattern.v2.EventV2;
import org.apache.flink.cep.pattern.v2.LeafCondition;

/**
 * TestCepEventParser.
 */
public class TestTupleParser extends CepEventParser {

	@Override
	public Object get(String key, CepEvent data) {
		Tuple2<Event, Long> tuple = (Tuple2) data;
		if (key.equals("name")) {
			return tuple.f0.getName();
		} else if (key.equals("id")) {
			return tuple.f0.getId();
		} else if (key.equals("price")) {
			return tuple.f0.getPrice();
		} else {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public <T> RichIterativeCondition<T> buildConditionV2(EventV2 event) {
		if (event.getId().equals("customized")) {
			ConditionGroup group = event.getConditionGroup();
			LeafCondition condition = group.getConditions().get(0);
			return (RichIterativeCondition<T>) new ImpCondition(condition.getKey(), condition.getValue());
		}
		return null;
	}

	@Override
	public CepEventParser duplicate() {
		return new TestTupleParser();
	}

	private static class ImpCondition extends RichIterativeCondition<Tuple2<Event, Long>> {

		String key;
		double value;

		ImpCondition(String key, String value) {
			this.key = key;
			this.value = Double.parseDouble(value);
		}

		@Override
		public boolean filter(Tuple2<Event, Long> tuple, Context<Tuple2<Event, Long>> ctx) throws Exception {
			if (key.equals("buy expensive items")) {
				return tuple.f0.getName().equals("buy") && tuple.f0.getPrice() > value;
			}
			return false;
		}
	}
}
