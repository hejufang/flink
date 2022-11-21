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

import org.apache.flink.cep.pattern.conditions.RichIterativeCondition;
import org.apache.flink.cep.pattern.pojo.Event;
import org.apache.flink.cep.pattern.v2.EventV2;

import java.io.Serializable;

/**
 * Parser used to infer values specified in {@link org.apache.flink.cep.pattern.conditions.EventParserCondition}.
 */
public abstract class CepEventParser implements Serializable {

	public abstract Object get(String key, CepEvent data);

	public <T> RichIterativeCondition<T> buildCondition(Event event) {
		return null;
	}

	public <T> RichIterativeCondition<T> buildConditionV2(EventV2 event) {
		return null;
	}

	public abstract CepEventParser duplicate();
}