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
 * Converter for {@code col BETWEEN e1 AND e2}.
 */
public class BetweenConverter implements Converter {

	public BetweenConverter() {
	}

	@Override
	public Predicate apply(Predicate... predicates) {
		Preconditions.checkArgument(predicates.length == 3, "");
		final Predicate p0 = predicates[0];
		final Predicate p1 = predicates[1];
		final Predicate p2 = predicates[2];
		final String pred = String.format("(%s BETWEEN %s AND %s)", p0.getPred(), p1.getPred(), p2.getPred());
		final Object[] params = p0.plus(p1, p2);
		return new Predicate(pred, params);
	}
}
