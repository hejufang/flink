/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.validate;

import org.apache.calcite.sql.validate.SqlConformance;

/** Sql conformance used for flink to set specific sql dialect parser. **/
public enum FlinkSqlConformance implements SqlConformance {
	/** Calcite's default SQL behavior. */
	DEFAULT,

	/** Conformance value that instructs Calcite to use SQL semantics
	 * consistent with the Apache HIVE, but ignoring its more
	 * inconvenient or controversial dicta. */
	HIVE,

	/**
	 * Conformance special for apaas.
	 */
	APAAS;

	@Override
	public boolean isLiberal() {
		return false;
	}

	@Override
	public boolean isGroupByAlias() {
		if (this == APAAS) {
			return true;
		}
		return false;
	}

	@Override
	public boolean isGroupByOrdinal() {
		return false;
	}

	@Override
	public boolean isHavingAlias() {
		if (this == APAAS) {
			return true;
		}
		return false;
	}

	@Override
	public boolean isSortByOrdinal() {
		switch (this) {
			case DEFAULT:
			case HIVE:
				return true;
			default:
				return false;
		}
	}

	@Override
	public boolean isSortByAlias() {
		switch (this) {
			case DEFAULT:
			case HIVE:
			case APAAS:
				return true;
			default:
				return false;
		}
	}

	@Override
	public boolean isSortByAliasObscures() {
		return false;
	}

	@Override
	public boolean isFromRequired() {
		return false;
	}

	@Override
	public boolean isBangEqualAllowed() {
		return false;
	}

	@Override
	public boolean isPercentRemainderAllowed() {
		return false;
	}

	@Override
	public boolean isMinusAllowed() {
		return false;
	}

	@Override
	public boolean isApplyAllowed() {
		return false;
	}

	@Override
	public boolean isInsertSubsetColumnsAllowed() {
		return false;
	}

	@Override
	public boolean allowNiladicParentheses() {
		return false;
	}

	@Override
	public boolean allowExplicitRowValueConstructor() {
		switch (this) {
		case DEFAULT:
			return true;
		}
		return false;
	}

	@Override
	public boolean allowExtend() {
		return false;
	}

	@Override
	public boolean isLimitStartCountAllowed() {
		return true;
	}

	@Override
	public boolean allowGeometry() {
		return false;
	}

	@Override
	public boolean shouldConvertRaggedUnionTypesToVarying() {
		return false;
	}

	@Override
	public boolean allowExtendedTrim() {
		return false;
	}

	@Override
	public boolean allowPluralTimeUnits() {
		return false;
	}

	@Override
	public boolean allowQualifyingCommonColumn() {
		return true;
	}
}
