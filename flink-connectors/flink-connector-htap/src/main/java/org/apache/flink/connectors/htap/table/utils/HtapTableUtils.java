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

package org.apache.flink.connectors.htap.table.utils;

import org.apache.flink.connectors.htap.connector.HtapFilterInfo;
import org.apache.flink.connectors.htap.connector.HtapTableInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;

import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * HtapTableUtils.
 */
public class HtapTableUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HtapTableUtils.class);

	// TODO: schema and props can be put into HtapTableInfo for extension
	public static HtapTableInfo createTableInfo(
			String tableName,
			TableSchema schema,
			Map<String, String> props) {
		HtapTableInfo tableInfo = HtapTableInfo.forTable(tableName);
		return tableInfo;
	}

	public static TableSchema htapToFlinkSchema(Schema schema) {
		TableSchema.Builder builder = TableSchema.builder();

		for (ColumnSchema column : schema.getColumns()) {
			DataType flinkType =
				HtapTypeUtils.toFlinkType(column.getType(), column.getMysqlType(),
					column.getTypeAttributes()).nullable();
			builder.field(column.getName(), flinkType);
		}

		return builder.build();
	}

	/**
	 * Converts Flink Expression to HtapFilterInfo.
	 */
	public static Optional<HtapFilterInfo> toHtapFilterInfo(Expression predicate) {
		LOG.debug("predicate summary: [{}], class: [{}], children: [{}]",
				predicate.asSummaryString(), predicate.getClass(), predicate.getChildren());
		if (predicate instanceof CallExpression) {
			CallExpression callExpression = (CallExpression) predicate;
			FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
			List<Expression> children = callExpression.getChildren();
			if (children.size() == 1) {
				return convertUnaryIsNullExpression(functionDefinition, children);
			} else if (children.size() == 2 &&
					!functionDefinition.equals(BuiltInFunctionDefinitions.OR)) {
				return convertBinaryComparison(functionDefinition, children);
			} else if (children.size() > 0 &&
					functionDefinition.equals(BuiltInFunctionDefinitions.OR)) {
				return convertIsInExpression(children);
			}
		}
		return Optional.empty();
	}

	private static boolean isFieldReferenceExpression(Expression exp) {
		return exp instanceof FieldReferenceExpression;
	}

	private static boolean isValueLiteralExpression(Expression exp) {
		return exp instanceof ValueLiteralExpression;
	}

	private static boolean isCallExpression(Expression exp) {
		return exp instanceof CallExpression;
	}

	private static Optional<HtapFilterInfo> convertUnaryIsNullExpression(
			FunctionDefinition functionDefinition,
			List<Expression> children) {
		FieldReferenceExpression fieldReferenceExpression;
		if (isFieldReferenceExpression(children.get(0))) {
			fieldReferenceExpression = (FieldReferenceExpression) children.get(0);
		} else {
			return Optional.empty();
		}
		// IS_NULL IS_NOT_NULL
		String columnName = fieldReferenceExpression.getName();
		HtapFilterInfo.Builder builder = HtapFilterInfo.Builder.create(columnName);
		if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NULL)) {
			return Optional.of(builder.isNull().build());
		} else if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NOT_NULL)) {
			return Optional.of(builder.isNotNull().build());
		}
		return Optional.empty();
	}

	private static Optional<HtapFilterInfo> convertBinaryComparison(
			FunctionDefinition functionDefinition,
			List<Expression> children) {
		FieldReferenceExpression fieldReferenceExpression;
		ValueLiteralExpression valueLiteralExpression;
		if (isValueLiteralExpression(children.get(0))) {
			fieldReferenceExpression = getFieldReferenceExpression(children.get(1)).orElse(null);
			valueLiteralExpression = (ValueLiteralExpression) children.get(0);
		} else if (isValueLiteralExpression(children.get(1))) {
			fieldReferenceExpression = getFieldReferenceExpression(children.get(0)).orElse(null);
			valueLiteralExpression = (ValueLiteralExpression) children.get(1);
		} else {
			return Optional.empty();
		}
		if (fieldReferenceExpression == null) {
			return Optional.empty();
		}
		String columnName = fieldReferenceExpression.getName();
		Object value = extractValueLiteral(fieldReferenceExpression, valueLiteralExpression);
		if (value == null) {
			return Optional.empty();
		}
		HtapFilterInfo.Builder builder = HtapFilterInfo.Builder.create(columnName);
		// GREATER GREATER_EQUAL EQUAL LESS LESS_EQUAL
		if (functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN)) {
			return Optional.of(builder.greaterThan(value).build());
		} else if (functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)) {
			return Optional.of(builder.greaterOrEqualTo(value).build());
		} else if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)) {
			return Optional.of(builder.equalTo(value).build());
		} else if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN)) {
			return Optional.of(builder.lessThan(value).build());
		} else if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL)) {
			return Optional.of(builder.lessOrEqualTo(value).build());
		}
		return Optional.empty();
	}

	private static Optional<HtapFilterInfo> convertIsInExpression(List<Expression> children) {
		// IN operation will be: or(equals(field, value1), or(equals(field, value2), ...)) in blink
		// For FilterType IS_IN, all internal CallExpression's function need to be equals and
		// fields need to be same
		Deque<Expression> expressions = new ArrayDeque<>(children);
		List<Object> values = new ArrayList<>(16);
		String columnName = null;
		// build values recursively
		while (!expressions.isEmpty()) {
			Expression expression = expressions.pop();
			if (isCallExpression(expression)) {
				CallExpression callExpression = (CallExpression) expression;
				FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
				List<Expression> subChildren = callExpression.getChildren();
				if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)
					&& subChildren.size() == 2) {
					FieldReferenceExpression fieldReferenceExpression;
					ValueLiteralExpression valueLiteralExpression;
					if (isValueLiteralExpression(subChildren.get(0))){
						fieldReferenceExpression =
							getFieldReferenceExpression(subChildren.get(1)).orElse(null);
						valueLiteralExpression = (ValueLiteralExpression) subChildren.get(0);
					} else if (isValueLiteralExpression(subChildren.get(1))) {
						fieldReferenceExpression =
							getFieldReferenceExpression(subChildren.get(0)).orElse(null);
						valueLiteralExpression = (ValueLiteralExpression) subChildren.get(1);
					} else {
						return Optional.empty();
					}
					if (fieldReferenceExpression != null) {
						String fieldName = fieldReferenceExpression.getName();
						if (columnName == null || columnName.equals(fieldName)) {
							columnName = fieldName;
						} else {
							return Optional.empty();
						}
						Object value =
							extractValueLiteral(fieldReferenceExpression, valueLiteralExpression);
						if (value == null) {
							return Optional.empty();
						}
						values.add(value);
					} else {
						return Optional.empty();
					}
				} else if (functionDefinition.equals(BuiltInFunctionDefinitions.OR)) {
					subChildren.forEach(expressions::push);
				} else {
					return Optional.empty();
				}
			} else {
				return Optional.empty();
			}
		}
		HtapFilterInfo.Builder builder = HtapFilterInfo.Builder.create(columnName);
		return Optional.of(builder.isIn(values).build());
	}

	private static Object extractValueLiteral(
			FieldReferenceExpression fieldReferenceExpression,
			ValueLiteralExpression valueLiteralExpression) {
		DataType fieldType = fieldReferenceExpression.getOutputDataType();
		return HtapTypeUtils.getLiteralValue(valueLiteralExpression, fieldType);
	}

	private static Optional<FieldReferenceExpression> getFieldReferenceExpression(Expression exp) {
		if (isFieldReferenceExpression(exp)) {
			return Optional.of((FieldReferenceExpression) exp);
		}
		if (isCallExpression(exp)) {
			CallExpression callExpression = (CallExpression) exp;
			if (callExpression.getFunctionDefinition().equals(BuiltInFunctionDefinitions.CAST)) {
				return Optional.of((FieldReferenceExpression) callExpression.getChildren().get(0));
			}
		}
		return Optional.empty();
	}
}
