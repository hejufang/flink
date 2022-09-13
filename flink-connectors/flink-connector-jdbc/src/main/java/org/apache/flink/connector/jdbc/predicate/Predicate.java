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

import java.io.Serializable;
import java.util.Arrays;

/**
 * The predicate abstraction for JDBC.
 *
 * <p>It's designed to use {@link java.sql.PreparedStatement} to construct the
 * final query to the JDBC vendor, to ease the escaping of string literals and
 * representation of other literals.
 *
 * <p><pre>
 * For the following SQL:
 * {@code
 * SELECT *
 * FROM T
 * WHERE -- the predicate which will be pushed into source
 *   col1 IS NOT NULL
 *   OR (col2 IN ('aa', 'bb', 'cc') AND col3 BETWEEN 10 AND 20)
 * }
 *
 * We will present it as:
 * pred = "((`col1` IS NOT NULL) OR ((`col2` IN (?, ?, ?)) AND (`col3` BETWEEN ? AND ?)))"
 * params = ['aa', 'bb', 'cc', 10, 20]
 * </pre>
 */
public class Predicate implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String pred;
	private final Object[] params;

	public Predicate(String pred) {
		this(pred, new String[0]);
	}

	public Predicate(String pred, Object[] params) {
		this.pred = pred;
		this.params = params;
	}

	public String getPred() {
		return pred;
	}

	public Object[] getParams() {
		return params;
	}

	public Object[] plus(Predicate... predicates) {
		if (predicates.length == 0) {
			return params;
		}

		final int paramsLength = Arrays.stream(predicates).mapToInt(p -> p.getParams().length).sum();
		final Object[] result = new Object[params.length + paramsLength];
		int idx = params.length;
		System.arraycopy(params, 0, result, 0, params.length);
		for (int i = 0; i < predicates.length; ++i) {
			System.arraycopy(predicates[i].getParams(), 0, result, idx, predicates[i].getParams().length);
			idx += predicates[i].getParams().length;
		}
		return result;
	}

	@Override
	public String toString() {
		return String.format(pred.replace("?", "%s"), params);
	}
}
