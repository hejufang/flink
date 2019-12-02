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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.WindowFunction;
import org.apache.flink.table.functions.WindowFunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.utils.HiveAggSqlFunction;
import org.apache.flink.table.planner.functions.utils.HiveScalarSqlFunction;
import org.apache.flink.table.planner.functions.utils.HiveTableSqlFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.planner.functions.utils.WindowSqlFunction;
import org.apache.flink.table.planner.plan.schema.DeferredTypeFlinkTableFunction;
import org.apache.flink.table.planner.utils.UserDefinedWindowUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlGroupedWindowFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.isHiveFunc;
import static org.apache.flink.table.planner.utils.UserDefinedWindowUtils.ASSIGN_WINDOWS_METHOD;
import static org.apache.flink.table.planner.utils.UserDefinedWindowUtils.END;
import static org.apache.flink.table.planner.utils.UserDefinedWindowUtils.MERGE_WINDOW_METHOD;
import static org.apache.flink.table.planner.utils.UserDefinedWindowUtils.PROCTIME;
import static org.apache.flink.table.planner.utils.UserDefinedWindowUtils.ROWTIME;
import static org.apache.flink.table.planner.utils.UserDefinedWindowUtils.START;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Thin adapter between {@link SqlOperatorTable} and {@link FunctionCatalog}.
 */
@Internal
public class FunctionCatalogOperatorTable implements SqlOperatorTable {

	private final FunctionCatalog functionCatalog;
	private final FlinkTypeFactory typeFactory;
	private final Map<String, SqlGroupedWindowFunction> registeredSqlGroupedWindowFunction = new HashMap<>();

	public FunctionCatalogOperatorTable(
			FunctionCatalog functionCatalog,
			FlinkTypeFactory typeFactory) {
		this.functionCatalog = functionCatalog;
		this.typeFactory = typeFactory;
	}

	@Override
	public void lookupOperatorOverloads(
			SqlIdentifier opName,
			SqlFunctionCategory category,
			SqlSyntax syntax,
			List<SqlOperator> operatorList,
			SqlNameMatcher nameMatcher) {
		if (!opName.isSimple()) {
			return;
		}

		if (registeredSqlGroupedWindowFunction.containsKey(opName.getSimple())) {
			operatorList.add(registeredSqlGroupedWindowFunction.get(opName.getSimple()));
			return;
		}

		// We lookup only user functions via CatalogOperatorTable. Built in functions should
		// go through BasicOperatorTable
		if (isNotUserFunction(category)) {
			return;
		}

		String name = opName.getSimple();
		Optional<FunctionLookup.Result> candidateFunction = functionCatalog.lookupFunction(name);

		candidateFunction.flatMap(lookupResult ->
			convertToSqlFunction(category, name, lookupResult.getFunctionDefinition())
		).ifPresent(operatorList::add);
	}

	private boolean isNotUserFunction(SqlFunctionCategory category) {
		return category != null && !category.isUserDefinedNotSpecificFunction();
	}

	private Optional<SqlFunction> convertToSqlFunction(
			SqlFunctionCategory category,
			String name,
			FunctionDefinition functionDefinition) {
		if (functionDefinition instanceof AggregateFunctionDefinition) {
			AggregateFunctionDefinition def = (AggregateFunctionDefinition) functionDefinition;
			if (isHiveFunc(def.getAggregateFunction())) {
				return Optional.of(new HiveAggSqlFunction(
						name,
						name,
						def.getAggregateFunction(),
						typeFactory));
			} else {
				return convertAggregateFunction(name, (AggregateFunctionDefinition) functionDefinition);
			}
		} else if (functionDefinition instanceof ScalarFunctionDefinition) {
			ScalarFunctionDefinition def = (ScalarFunctionDefinition) functionDefinition;
			if (isHiveFunc(def.getScalarFunction())) {
				return Optional.of(new HiveScalarSqlFunction(
						name,
						name,
						def.getScalarFunction(),
						typeFactory));
			} else {
				return convertScalarFunction(name, def);
			}
		} else if (functionDefinition instanceof WindowFunctionDefinition) {
			WindowFunctionDefinition def = (WindowFunctionDefinition) functionDefinition;
			return convertWindowFunction(name, def);
		} else if (functionDefinition instanceof TableFunctionDefinition &&
				category != null &&
				category.isTableFunction()) {
			TableFunctionDefinition def = (TableFunctionDefinition) functionDefinition;
			if (isHiveFunc(def.getTableFunction())) {
				DataType returnType = fromLegacyInfoToDataType(new GenericTypeInfo<>(Row.class));
				return Optional.of(new HiveTableSqlFunction(
						name,
						name,
						def.getTableFunction(),
						returnType,
						typeFactory,
						new DeferredTypeFlinkTableFunction(def.getTableFunction(), returnType),
						HiveTableSqlFunction.operandTypeChecker(name, def.getTableFunction())));
			} else {
				return convertTableFunction(name, (TableFunctionDefinition) functionDefinition);
			}
		}

		return Optional.empty();
	}

	private Optional<SqlFunction> convertAggregateFunction(
			String name,
			AggregateFunctionDefinition functionDefinition) {
		SqlFunction aggregateFunction = UserDefinedFunctionUtils.createAggregateSqlFunction(
			name,
			name,
			functionDefinition.getAggregateFunction(),
			TypeConversions.fromLegacyInfoToDataType(functionDefinition.getResultTypeInfo()),
			TypeConversions.fromLegacyInfoToDataType(functionDefinition.getAccumulatorTypeInfo()),
			typeFactory
		);
		return Optional.of(aggregateFunction);
	}

	private Optional<SqlFunction> convertScalarFunction(String name, ScalarFunctionDefinition functionDefinition) {
		SqlFunction scalarFunction = UserDefinedFunctionUtils.createScalarSqlFunction(
			name,
			name,
			functionDefinition.getScalarFunction(),
			typeFactory
		);
		return Optional.of(scalarFunction);
	}

	private Optional<SqlFunction> convertWindowFunction(String name, WindowFunctionDefinition functionDefinition) {
		WindowFunction windowFunction = functionDefinition.getWindowFunction();

		UserDefinedWindowUtils.validateWindowFunction(windowFunction);

		Method[] assignWindowMethod = UserDefinedFunctionUtils.checkAndExtractMethods(windowFunction, ASSIGN_WINDOWS_METHOD);

		LogicalType[] logicalTypes = UserDefinedFunctionUtils.getParameterTypes(
			windowFunction, assignWindowMethod[0].getParameterTypes());

		SqlTypeFamily[] sqlTypeFamilies = Stream.of(logicalTypes)
			.map(typeFactory::createFieldTypeFromLogicalType)
			.map(RelDataType::getSqlTypeName)
			.map(SqlTypeName::getFamily)
			.toArray(SqlTypeFamily[]::new);
		sqlTypeFamilies[0] = SqlTypeFamily.DATETIME; // need to specify first argument's type is timestamp

		List<SqlGroupedWindowFunction> auxiliaries = new ArrayList<>();
		WindowSqlFunction groupedWindowFunction =  new WindowSqlFunction(
			name,
			SqlKind.OTHER_FUNCTION,
			null,
			OperandTypes.family(sqlTypeFamilies)) {
			@Override
			public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
				return auxiliaries;
			}
		};
		this.registeredSqlGroupedWindowFunction.put(name, groupedWindowFunction);

		SqlGroupedWindowFunction windowStart = groupedWindowFunction.auxiliary(name + START, SqlKind.OTHER_FUNCTION);
		this.registeredSqlGroupedWindowFunction.put(name + START, windowStart);
		auxiliaries.add(windowStart);

		SqlGroupedWindowFunction windowEnd = groupedWindowFunction.auxiliary(name + END, SqlKind.OTHER_FUNCTION);
		this.registeredSqlGroupedWindowFunction.put(name + END, windowEnd);
		auxiliaries.add(windowEnd);

		SqlGroupedWindowFunction windowProctime = groupedWindowFunction.auxiliary(name + PROCTIME, SqlKind.OTHER_FUNCTION);
		this.registeredSqlGroupedWindowFunction.put(name + PROCTIME, windowProctime);
		auxiliaries.add(windowProctime);

		SqlGroupedWindowFunction windowRowtime = groupedWindowFunction.auxiliary(name + ROWTIME, SqlKind.OTHER_FUNCTION);
		this.registeredSqlGroupedWindowFunction.put(name + ROWTIME, windowRowtime);
		auxiliaries.add(windowRowtime);

		groupedWindowFunction.setWindowFunction(windowFunction);
		groupedWindowFunction.setAssignWindowMethodParamTypes(logicalTypes);
		boolean hasMergeMethod = UserDefinedFunctionUtils.ifMethodExistInFunction(MERGE_WINDOW_METHOD, windowFunction);
		groupedWindowFunction.setHasMergeMethod(hasMergeMethod);

		return Optional.of(groupedWindowFunction);
	}

	private Optional<SqlFunction> convertTableFunction(String name, TableFunctionDefinition functionDefinition) {
		SqlFunction tableFunction = UserDefinedFunctionUtils.createTableSqlFunction(
			name,
			name,
			functionDefinition.getTableFunction(),
			TypeConversions.fromLegacyInfoToDataType(functionDefinition.getResultType()),
			typeFactory
		);
		return Optional.of(tableFunction);
	}

	@Override
	public List<SqlOperator> getOperatorList() {
		throw new UnsupportedOperationException("This should never be called");
	}
}
