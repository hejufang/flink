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

package org.apache.flink.cep.pattern.conditions.comparators;

import javax.annotation.Nullable;

/**
 *
 */
public class Comparators {

	/**
	 *
	 */
	public static class DoubleComparator extends ConditionComparator<Double> {

		@Override
		public boolean isEqual(Double o1, Double o2) {
			return o1.equals(o2);
		}

		@Override
		public boolean isGreater(Double o1, Double o2) {
			return o1 > o2;
		}

		@Override
		public boolean isLess(Double o1, Double o2) {
			return o1 < o2;
		}

		@Override
		public Double castValue(Object obj) {
			if (obj instanceof String) {
				return Double.valueOf((String) obj);
			} else if (obj instanceof Float) {
				return Double.valueOf((Float) obj);
			} else if (obj instanceof Double) {
				return (Double) obj;
			} else if (obj instanceof Integer) {
				return Double.valueOf((Integer) obj);
			} else if (obj instanceof Long) {
				return Double.valueOf((Long) obj);
			} else {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public Double plus(@Nullable Double base, Object obj) {
			return (base == null ? defaultValue() : base) + castValue(obj);
		}

		@Override
		public Double defaultValue() {
			return 0.0;
		}
	}

	/**
	 *
	 */
	public static class LongComparator extends ConditionComparator<Long> {

		@Override
		public boolean isEqual(Long o1, Long o2) {
			return o1.equals(o2);
		}

		@Override
		public boolean isGreater(Long o1, Long o2) {
			return o1 > o2;
		}

		@Override
		public boolean isLess(Long o1, Long o2) {
			return o1 < o2;
		}

		@Override
		public Long castValue(Object obj) {
			if (obj instanceof String) {
				return Long.valueOf((String) obj);
			} else if (obj instanceof Integer) {
				return Long.valueOf((Integer) obj);
			} else if (obj instanceof Long) {
				return (Long) obj;
			} else {
				throw new UnsupportedOperationException();
			}
		}

		@Override
		public Long plus(Long base, Object obj) {
			return (base == null ? defaultValue() : base) + castValue(obj);
		}

		@Override
		public Long defaultValue() {
			return 0L;
		}
	}

	/**
	 *
	 */
	public static class StringComparator extends ConditionComparator<String> {

		@Override
		public boolean isEqual(String o1, String o2) {
			return o1.equals(o2);
		}

		@Override
		public boolean isGreater(String o1, String o2) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isLess(String o1, String o2) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String castValue(Object obj) {
			return obj.toString();
		}
	}
}
