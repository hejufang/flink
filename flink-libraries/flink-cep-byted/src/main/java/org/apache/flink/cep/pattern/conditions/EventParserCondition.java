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

/**
 * New condition for cep2.0.
 * @param <IN>
 */
public class EventParserCondition<IN> extends RichIterativeCondition<IN> {

	private final CepEventParser cepEventParser;

	private final Condition condition;

	public EventParserCondition(CepEventParser cepEventParser, Condition condition) {
		this.cepEventParser = cepEventParser;
		this.condition = condition;
	}

	@Override
	public boolean filter(IN value, Context<IN> ctx) throws Exception {
		if (condition.getOp().equals(Condition.OpType.EQUAL)) {
			// TODO consider null values
			return cepEventParser.get(condition.getKey(), (CepEvent) value).equals(condition.getValue());
		} else {
			throw new UnsupportedOperationException(String.format("Op %s is not supported.", condition.getOp().toString()));
		}
	}
}
