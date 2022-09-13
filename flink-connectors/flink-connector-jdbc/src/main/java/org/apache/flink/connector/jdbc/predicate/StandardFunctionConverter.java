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

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Converter for general function call, such as @{func(e1, e2, e3)}.
 */
public class StandardFunctionConverter implements Converter {

	private final String functionName;

	public StandardFunctionConverter(String functionName) {
		this.functionName = functionName;
	}

	@Override
	public Predicate apply(Predicate... predicates) {
		final String pred = String.format("%s(%s)",
			functionName,
			Arrays.stream(predicates).map(Predicate::getPred).collect(Collectors.joining(", ")));
		final Object[] params = predicates[0].plus(Arrays.copyOfRange(predicates, 1, predicates.length));
		return new Predicate(pred, params);
	}
}
