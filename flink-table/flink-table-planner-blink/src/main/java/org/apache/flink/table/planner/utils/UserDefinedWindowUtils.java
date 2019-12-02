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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.WindowFunction;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

/**
 * Utils for user defined window.
 */
public class UserDefinedWindowUtils {
	public static final String START = "_START";
	public static final String END = "_END";
	public static final String PROCTIME = "_PROCTIME";
	public static final String ROWTIME = "_ROWTIME";

	public static final String ASSIGN_WINDOWS_METHOD = "assignWindows";
	public static final String MERGE_WINDOW_METHOD = "mergeWindow";

	public static int[] getFieldIndices(List<RexNode> operands, LogicalProject project, RelDataType rowType) {
		int[] result = operands.stream()
			.mapToInt(node -> {
				if (node instanceof RexInputRef) {
					RexInputRef rexInputRef = (RexInputRef) node;
					String columnName = rowType.getFieldList().get(rexInputRef.getIndex()).getName();
					for (RelDataTypeField relDataTypeField : project.getRowType().getFieldList()) {
						if (relDataTypeField.getName().equalsIgnoreCase(columnName)) {
							return relDataTypeField.getIndex();
						}
					}
				}
				return -1;
			})
			.toArray();
		result[0] = 0; // for now, timestamp's index is always 0.
		return result;
	}

	public static void validateWindowFunction(WindowFunction windowFunction) {
		Method[] assignWindowMethod = UserDefinedFunctionUtils.checkAndExtractMethods(windowFunction, ASSIGN_WINDOWS_METHOD);
		if (assignWindowMethod.length > 1) {
			throw new TableException("assignWindows should be implemented iff once.");
		}
		if (!assignWindowMethod[0].getParameterTypes()[0].isAssignableFrom(long.class)) {
			throw new TableException("first argument of assignWindows should be long");
		}
		if (!assignWindowMethod[0].getReturnType().isAssignableFrom(Collection.class)) {
			// TODO: verify generic type of Collection is TimeWindow.
			throw new TableException("assignWindows result type must be Collection<TimeWindow>");
		}

		Method[] mergeWindowMethods = new Method[0];
		if (UserDefinedFunctionUtils.ifMethodExistInFunction(MERGE_WINDOW_METHOD, windowFunction)) {
			mergeWindowMethods = UserDefinedFunctionUtils.checkAndExtractMethods(windowFunction, MERGE_WINDOW_METHOD);
		}
		if (mergeWindowMethods.length > 1) {
			throw new TableException("mergeWindow should be implemented no more than once.");
		}
		if (mergeWindowMethods.length > 0) {
			Method mergeMethod = mergeWindowMethods[0];
			if (!mergeMethod.getReturnType().isAssignableFrom(TimeWindow.class)) {
				throw new TableException("mergeWindow return type should be TimeWindow");
			}
			Class<?>[] parameterTypes = mergeMethod.getParameterTypes();
			if (parameterTypes.length != 2) {
				throw new TableException("mergeWindow should have two parameter exactly");
			}
			for (Class<?> clazz : parameterTypes) {
				if (!clazz.isAssignableFrom(TimeWindow.class)) {
					throw new TableException("mergeWindow parameter's type should be TimeWindow");
				}
			}
		}
	}

	// Make sure that we cannot instantiate this class
	private UserDefinedWindowUtils() {
	}
}
