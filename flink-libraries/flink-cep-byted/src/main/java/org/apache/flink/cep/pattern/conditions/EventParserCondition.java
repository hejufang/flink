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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.cep.pattern.parser.CepEvent;
import org.apache.flink.cep.pattern.parser.CepEventParser;
import org.apache.flink.cep.pattern.pojo.Condition;

import java.util.List;

/**
 * New condition for cep2.0.
 * @param <IN>
 */
public class EventParserCondition<IN> extends RichIterativeCondition<IN> {

	private final CepEventParser cepEventParser;

	private final List<Condition> conditions;

	public EventParserCondition(CepEventParser cepEventParser, List<Condition> conditions) {
		this.cepEventParser = cepEventParser;
		this.conditions = conditions;
	}

	@Override
	public boolean filter(IN event, Context<IN> ctx) throws Exception {
		for (Condition condition : conditions) {
			if (condition.getOp().equals(Condition.OpType.EQUAL)) {
				final String value = cepEventParser.get(condition.getKey(), (CepEvent) event);
				if (value == null || !value.equals(condition.getValue())) {
					return false;
				}
			} else {
				throw new UnsupportedOperationException(String.format("Op %s is not supported.", condition.getOp().toString()));
			}
		}
		return true;
	}
}
