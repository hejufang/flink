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

package org.apache.flink.connector.jdbc.predicate;

import org.apache.flink.util.Preconditions;

/**
 * Converter for general postfix operators, such as {@code col NOT NULL}.
 */
public class PostfixConverter implements Converter {

	private final String op;

	public PostfixConverter(String op) {
		this.op = op;
	}

	@Override
	public Predicate apply(Predicate... predicates) {
		Preconditions.checkArgument(predicates.length == 1, "");
		final Predicate p0 = predicates[0];

		final String pred = String.format("(%s %s)", p0.getPred(), op);
		return new Predicate(pred, p0.getParams());
	}
}
