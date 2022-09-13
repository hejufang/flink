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

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Converter for {@code col IN (e1, e2, e3...)}.
 */
public class InConverter implements Converter {

	public InConverter() {
	}

	@Override
	public Predicate apply(Predicate... predicates) {
		Preconditions.checkArgument(predicates.length >= 2, "");
		final Predicate p0 = predicates[0];
		final Object[] params = p0.plus(Arrays.copyOfRange(predicates, 1, predicates.length));
		final String inPred = Arrays.stream(Arrays.copyOfRange(predicates, 1, predicates.length))
			.map(Predicate::getPred)
			.collect(Collectors.joining(", "));
		final String pred = String.format("(%s IN(%s))", p0.getPred(), inPred);
		return new Predicate(pred, params);
	}
}
