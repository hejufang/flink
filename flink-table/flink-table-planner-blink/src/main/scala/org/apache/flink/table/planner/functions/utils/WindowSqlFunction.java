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

package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.table.functions.WindowFunction;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlGroupedWindowFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * Calcite's wrapper for user defined window function.
 */
public class WindowSqlFunction extends SqlGroupedWindowFunction {
	private WindowFunction windowFunction;
	private LogicalType[] assignWindowMethodParamTypes;
	private boolean hasMergeMethod;

	public WindowSqlFunction(
			String name,
			SqlKind kind,
			SqlGroupedWindowFunction groupFunction,
			SqlReturnTypeInference returnTypeInference,
			SqlOperandTypeInference operandTypeInference,
			SqlOperandTypeChecker operandTypeChecker,
			SqlFunctionCategory category) {
		super(name, kind, groupFunction, returnTypeInference, operandTypeInference, operandTypeChecker, category);
	}

	public WindowSqlFunction(
			String name,
			SqlKind kind,
			SqlGroupedWindowFunction groupFunction,
			SqlOperandTypeChecker operandTypeChecker) {
		super(name, kind, groupFunction, operandTypeChecker);
	}

	public WindowSqlFunction(
			SqlKind kind,
			SqlGroupedWindowFunction groupFunction,
			SqlOperandTypeChecker operandTypeChecker) {
		super(kind, groupFunction, operandTypeChecker);
	}

	public void setWindowFunction(WindowFunction windowFunction) {
		this.windowFunction = windowFunction;
	}

	public WindowFunction getWindowFunction() {
		return windowFunction;
	}

	public void setAssignWindowMethodParamTypes(LogicalType[] paramTypes) {
		this.assignWindowMethodParamTypes = paramTypes;
	}

	public LogicalType[] getAssignWindowMethodParamTypes() {
		return assignWindowMethodParamTypes;
	}

	public boolean isHasMergeMethod() {
		return hasMergeMethod;
	}

	public void setHasMergeMethod(boolean hasMergeMethod) {
		this.hasMergeMethod = hasMergeMethod;
	}
}
