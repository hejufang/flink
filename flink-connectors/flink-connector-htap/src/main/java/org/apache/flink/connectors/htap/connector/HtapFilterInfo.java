/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.htap.connector;

import org.apache.flink.annotation.PublicEvolving;

import com.bytedance.htap.HtapPredicate;
import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.Schema;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

/**
 * HtapFilterInfo.
 */
@PublicEvolving
public class HtapFilterInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private String column;
	private FilterType type;
	private Object value;

	private HtapFilterInfo() {
	}

	public HtapPredicate toPredicate(final Schema schema) {
		return toPredicate(schema.getColumn(this.column));
	}

	public HtapPredicate toPredicate(final ColumnSchema column) {
		HtapPredicate predicate;
		switch (this.type) {
			case IS_IN:
				predicate = HtapPredicate.newInListPredicate(column, (List<?>) this.value);
				break;
			case IS_NULL:
				predicate = HtapPredicate.newIsNullPredicate(column);
				break;
			case IS_NOT_NULL:
				predicate = HtapPredicate.newIsNotNullPredicate(column);
				break;
			default:
				predicate = predicateComparator(column);
				break;
		}
		return predicate;
	}

	private HtapPredicate predicateComparator(final ColumnSchema column) {
		final HtapPredicate.ComparisonOp comparison = this.type.comparator;

		HtapPredicate predicate;

		switch (column.getType()) {
			case STRING:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (String) this.value);
				break;
			case FLOAT:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (float) this.value);
				break;
			case INT8:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (byte) this.value);
				break;
			case INT16:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (short) this.value);
				break;
			case INT32:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (int) this.value);
				break;
			case INT64:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (long) this.value);
				break;
			case DOUBLE:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (double) this.value);
				break;
			case BOOL:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (boolean) this.value);
				break;
			case DATE:
				final LocalDate date = (LocalDate) this.value;
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, Date.valueOf(date));
				break;
			case VARCHAR:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, this.value);
				break;
			case UNIXTIME_MICROS:
				final Long time = (Long) this.value;
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, time * 1000);
				break;
			case BINARY:
				predicate = HtapPredicate.newComparisonPredicate(column, comparison, (byte[]) this.value);
				break;
			default:
				throw new IllegalArgumentException("Illegal var type: " + column.getType());
		}
		return predicate;
	}

	/**
	 * FilterType.
	 */
	public enum FilterType {
		GREATER(HtapPredicate.ComparisonOp.GREATER),
		GREATER_EQUAL(HtapPredicate.ComparisonOp.GREATER_EQUAL),
		EQUAL(HtapPredicate.ComparisonOp.EQUAL),
		LESS(HtapPredicate.ComparisonOp.LESS),
		LESS_EQUAL(HtapPredicate.ComparisonOp.LESS_EQUAL),
		IS_NOT_NULL(null),
		IS_NULL(null),
		IS_IN(null);

		final HtapPredicate.ComparisonOp comparator;

		FilterType(final HtapPredicate.ComparisonOp comparator) {
			this.comparator = comparator;
		}
	}

	@Override
	public String toString() {
		return "HtapFilterInfo[Column=" + (column == null ? "null" : column) +
			", FilterType=" + (type == null ? "null" : type.toString()) +
			", Value=" + (value == null ? "null" : value.toString()) + "]";
	}

	/**
	 * Builder for {@link HtapFilterInfo}.
	 */
	public static class Builder {
		private final HtapFilterInfo filter;

		private Builder(final String column) {
			this.filter = new HtapFilterInfo();
			this.filter.column = column;
		}

		public static Builder create(final String column) {
			return new Builder(column);
		}

		public Builder greaterThan(final Object value) {
			return filter(FilterType.GREATER, value);
		}

		public Builder lessThan(final Object value) {
			return filter(FilterType.LESS, value);
		}

		public Builder equalTo(final Object value) {
			return filter(FilterType.EQUAL, value);
		}

		public Builder greaterOrEqualTo(final Object value) {
			return filter(FilterType.GREATER_EQUAL, value);
		}

		public Builder lessOrEqualTo(final Object value) {
			return filter(FilterType.LESS_EQUAL, value);
		}

		public Builder isNotNull() {
			return filter(FilterType.IS_NOT_NULL, null);
		}

		public Builder isNull() {
			return filter(FilterType.IS_NULL, null);
		}

		public Builder isIn(final List<?> values) {
			return filter(FilterType.IS_IN, values);
		}

		public Builder filter(final FilterType type, final Object value) {
			this.filter.type = type;
			this.filter.value = value;
			return this;
		}

		public HtapFilterInfo build() {
			return filter;
		}
	}
}
