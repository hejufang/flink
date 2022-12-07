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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.TemporalTableFunctionImpl;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Static;

import java.math.BigDecimal;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;

/**
 * Extends Calcite's {@link SqlValidator} by Flink-specific behavior.
 */
@Internal
public final class FlinkCalciteSqlValidator extends SqlValidatorImpl {

	public static boolean enableApaasSqlConformance = GlobalConfiguration
		.loadConfiguration()
		.get(ExecutionConfigOptions.ENABLE_APAAS_SQL_CONFORMANCE);

	public FlinkCalciteSqlValidator(
			SqlOperatorTable opTab,
			SqlValidatorCatalogReader catalogReader,
			RelDataTypeFactory typeFactory) {
		super(opTab, catalogReader, typeFactory,
			enableApaasSqlConformance ? FlinkSqlConformance.APAAS : SqlConformanceEnum.DEFAULT);
		if (enableApaasSqlConformance) {
			setCallRewrite(false);
		}
	}

	@Override
	public void validateLiteral(SqlLiteral literal) {
		if (literal.getTypeName() == DECIMAL) {
			final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
			if (decimal.precision() > DecimalType.MAX_PRECISION) {
				throw newValidationError(
					literal,
					Static.RESOURCE.numberLiteralOutOfRange(decimal.toString()));
			}
		}
		super.validateLiteral(literal);
	}

	@Override
	protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
		// Due to the improper translation of lateral table left outer join in Calcite, we need to
		// temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
		if (join.getJoinType() == JoinType.LEFT &&
				SqlUtil.stripAs(join.getRight()).getKind() == SqlKind.COLLECTION_TABLE &&
				!isTemporalTableFunction(join.getRight())) {
			final SqlNode condition = join.getCondition();
			if (condition != null &&
					(!SqlUtil.isLiteral(condition) || ((SqlLiteral) condition).getValueAs(Boolean.class) != Boolean.TRUE)) {
				throw new ValidationException(
					String.format(
						"Left outer joins with a table function do not accept a predicate such as %s. " +
						"Only literal TRUE is accepted.",
						condition));
			}
		}
		super.validateJoin(join, scope);
	}

	private boolean isTemporalTableFunction(SqlNode node) {
		// we enabled temporal table function left outer join in
		// LogicalCorrelateWithFilterToJoinFromTemporalTableFunctionRule,
		// currently only left outer join on table function is not supported.
		if (!(node instanceof SqlCall)) {
			return false;
		}

		SqlNode operand0 = ((SqlCall) node).operand(0);
		if (operand0 instanceof SqlCall) {
			operand0 = ((SqlCall) operand0).operand(0);
		} else {
			return false;
		}
		if (operand0 instanceof SqlCall) {
			SqlOperator operator = ((SqlCall) operand0).getOperator();
			if (operator instanceof TableSqlFunction) {
				return ((TableSqlFunction) operator).udtf() instanceof TemporalTableFunctionImpl;
			} else if (operator instanceof BridgingSqlFunction) {
				return ((BridgingSqlFunction) operator).getDefinition() instanceof TemporalTableFunctionImpl;
			}
		}

		return false;
	}
}
