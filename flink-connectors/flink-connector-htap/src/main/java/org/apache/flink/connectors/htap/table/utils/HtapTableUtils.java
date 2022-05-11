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

import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.Schema;
import com.bytedance.htap.metaclient.partition.PartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * HtapTableUtils.
 */
public class HtapTableUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HtapTableUtils.class);
	private static final String SEPARATOR = "/";

	// TODO: schema and props can be put into HtapTableInfo for extension
	public static HtapTableInfo createTableInfo(
			String dbName,
			String tableName,
			TableSchema schema,
			Map<String, String> props) {
		HtapTableInfo tableInfo = HtapTableInfo.forTable(dbName, tableName);
		return tableInfo;
	}

	/**
	 * Extract database names from Htap tableNames, Htap tableNames have particular
	 * format which like `databaseName/tableName`.
	 */
	public static Set<String> extractDatabaseName(List<String> htapTableNames) {
		Set<String> databases = new HashSet<>();
		for (String tableName : htapTableNames) {
			if (tableName.contains(SEPARATOR)) {
				databases.add(tableName.split(SEPARATOR)[0]);
			}
		}
		return databases;
	}

	public static TableSchema htapToFlinkSchema(Schema schema) {
		TableSchema.Builder builder = TableSchema.builder();
		List<ColumnSchema> primaryKeys = schema.getPrimaryKeyColumns();

		for (ColumnSchema column : schema.getColumns()) {
			DataType flinkType =
				HtapTypeUtils.toFlinkType(column.getType(), column.getMysqlType(),
					column.getTypeAttributes()).nullable();
			if (primaryKeys.contains(column)) {
				flinkType = flinkType.notNull();
			}
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
				return convertOptimizedIsInExpression(children);
			} else if (children.size() > 1 &&
					functionDefinition.equals(BuiltInFunctionDefinitions.IN)) {
				return convertIsInExpression(children);
			}
		}
		return Optional.empty();
	}

	public static PartitionID parsePartitionID(String partitionStr) {
		if (partitionStr == null) {
			throw new FlinkRuntimeException("partitionStr can not be null");
		}

		String[] items = partitionStr.trim().split("#");
		if (items.length < 3) {
			throw new FlinkRuntimeException("partitionStr must be in format" +
				" '{dbCluster}#{spaceNo}#{id}', but is " + partitionStr);
		}
		try {
			String dbCluster = items[0];
			int spaceNo = Integer.parseInt(items[1]);
			int id = Integer.parseInt(items[2]);
			return new PartitionID(dbCluster, spaceNo, id);
		} catch (NumberFormatException e) {
			throw new FlinkRuntimeException("partitionStr must be in format" +
				" '{dbCluster}#{spaceNo}#{id}', but is " + partitionStr);
		}
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

	private static boolean isCastExpression(Expression exp) {
		return exp instanceof CallExpression &&
				((CallExpression) exp).getFunctionDefinition().equals(BuiltInFunctionDefinitions.CAST);
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
		} else if (isFieldReferenceExpression(children.get(0)) && isFieldReferenceExpression(children.get(1))) {
			fieldReferenceExpression = getFieldReferenceExpression(children.get(0)).orElse(null);
			if (fieldReferenceExpression == null) {
				return Optional.empty();
			}
			String columnName = fieldReferenceExpression.getName();
			FieldReferenceExpression otherRefExp = getFieldReferenceExpression(children.get(1)).orElse(null);
			if (otherRefExp == null) {
				return Optional.empty();
			}
			String otherColumn = otherRefExp.getName();
			LOG.debug("Two columns: {}, {}", columnName, otherColumn);
			HtapFilterInfo.Builder builder = HtapFilterInfo.Builder.create(columnName, otherColumn);
			if (functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN)) {
				return Optional.of(builder.filter(HtapFilterInfo.FilterType.GREATER, null).build());
			} else if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN)) {
				return Optional.of(builder.filter(HtapFilterInfo.FilterType.LESS, null).build());
			} else if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)) {
				return Optional.of(builder.filter(HtapFilterInfo.FilterType.EQUAL, null).build());
			} else if (functionDefinition.equals(BuiltInFunctionDefinitions.NOT_EQUALS)) {
				return Optional.of(builder.filter(HtapFilterInfo.FilterType.NOT_EQUAL, null).build());
			} else if (functionDefinition.equals(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL)) {
				return Optional.of(builder.filter(HtapFilterInfo.FilterType.GREATER_EQUAL, null).build());
			} else if (functionDefinition.equals(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL)) {
				return Optional.of(builder.filter(HtapFilterInfo.FilterType.LESS_EQUAL, null).build());
			} else {
				return Optional.empty();
			}
		} else {
			return Optional.empty();
		}
		LOG.debug("fieldReferenceExpression: {}", fieldReferenceExpression);
		if (fieldReferenceExpression == null) {
			return Optional.empty();
		}
		String columnName = fieldReferenceExpression.getName();
		Object value = extractValueLiteral(fieldReferenceExpression, valueLiteralExpression);
		LOG.debug("Extracted value from literal: {}", value);
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

	// some `in` expression will optimized into `equal` expression connected with `or`
	// e.g. a in (1,2,3) => a = 1 or a = 2 or a = 3
	private static Optional<HtapFilterInfo> convertOptimizedIsInExpression(List<Expression> children) {
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

	private static Optional<HtapFilterInfo> convertIsInExpression(List<Expression> children) {
		if (isFieldReferenceExpression(children.get(0))) {
			List<Expression> valueExprs = children.subList(1, children.size());
			Optional<FieldReferenceExpression> optionalFieldExpr =
				getFieldReferenceExpression(children.get(0));

			if (optionalFieldExpr.isPresent()) {
				FieldReferenceExpression fieldReferenceExpression = optionalFieldExpr.get();
				String fieldName = fieldReferenceExpression.getName();
				List<Object> values = new ArrayList<>();
				for (Expression expr : valueExprs) {
					if (isValueLiteralExpression(expr)) {
						values.add(
							extractValueLiteral(fieldReferenceExpression, (ValueLiteralExpression) expr));
					} else {
						return Optional.empty();
					}
				}
				HtapFilterInfo.Builder builder = HtapFilterInfo.Builder.create(fieldName);
				return Optional.of(builder.isIn(values).build());
			}
		}
		return Optional.empty();
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
		return getFieldRefFromRecursiveCast(exp);
	}

	/**
	 * Return filed reference expression from recursive cast, or Optional.empty() for other cases.
	 * e.g. cast(cast(col1, CHAR(1)), VARCHAR(32)) => col1
	 * */
	private static Optional<FieldReferenceExpression> getFieldRefFromRecursiveCast(Expression exp) {
		if (isCastExpression(exp)) {
			CallExpression callExpression = (CallExpression) exp;
			Expression firstChild = callExpression.getChildren().get(0);
			if (isFieldReferenceExpression(firstChild)) {
				return Optional.of((FieldReferenceExpression) firstChild);
			} else if (isCastExpression(firstChild)) {
				return getFieldRefFromRecursiveCast(firstChild);
			}
		}
		return Optional.empty();
	}

	/**
	 * Parse partition predicates from partition expressions.
	 * We only support that each expression contains one partition key with 'or', 'in', 'equals'
	 * expression. Correspondingly, we only support some kind of calc：
	 * - different partition key expression linked by 'and'.
	 * - the expression for each partition key must be 'equals' or 'in' or linked then with 'or'.
	 *
	 * <p>There are some examples:
	 * - partition1 = xx and partition2 = xx and ...
	 * - partition1 in (xx, xx ...) and partition2 in (xx, xx ...) and ...
	 * - (partition1 = xx or partition1 = xx) and partition2 in (xx, xx ...) and ...
	 *
	 * @param expressions each expression in expressions is in And logical.
	 *
	 * @return partition predicates or empty list if extract failed.
	 * Struct of returned result(List&ltMap$ltString, Set&ltString&gt&t&gt):
	 * - Each map in List is in Or logical: map1 or map2 or map3. We only return single map for now.
	 * - Each key in map is field name of a partition.
	 * - Values in map represent value space of the corresponding key.
	 *
	 * For example [{"id": [1,2,3], "name": ["a"]}, {"id": [4]}] means
	 * (id in (1,2,3) and name = 'a') or (id = 4).
	 *
	 * */
	public static List<Map<String, Set<String>>> extractPartitionPredicates(
			List<Expression> expressions) {
		Map<String, Set<String>> multiPartitionPredicates = new HashMap<>();
		for (Expression expression : expressions) {
			Optional<Tuple2<String, Set<String>>> predicate =
				extractPartitionPredicate(expression);
			if (!predicate.isPresent()) {
				return Collections.emptyList();
			}
			String partitionName = predicate.get().f0;
			Set<String> values = predicate.get().f1;
			multiPartitionPredicates.put(partitionName, values);
		}
		return Collections.singletonList(multiPartitionPredicates);
	}

	/**
	 * Extract partition predicate of single partition key from an expression.
	 * */
	private static Optional<Tuple2<String, Set<String>>> extractPartitionPredicate(
			Expression expression) {
		if (isOrExpression(expression)) {
			return parsePartitionPredicatesFromOr(expression);
		} else if (isEqualsExpression(expression)) {
			return extractPartitionPredicateFromEquals(expression);
		} else if (isInExpression(expression)) {
			return extractPartitionPredicateFromIn(expression);
		}
		return Optional.empty();
	}

	/**
	 * Extract partition predicate of single partition key from 'equals' expression.
	 * */
	private static Optional<Tuple2<String, Set<String>>> extractPartitionPredicateFromEquals(
			Expression expression) {
		List<Expression> children = expression.getChildren();
		Preconditions.checkState(children.size() == 2,
			"Children size of Equals Expression must be equal to 2.");
		FieldReferenceExpression fieldReferenceExpression = null;
		ValueLiteralExpression valueLiteralExpression = null;
		if (isValueLiteralExpression(children.get(0))) {
			fieldReferenceExpression = getFieldReferenceExpression(children.get(1)).orElse(null);
			valueLiteralExpression = (ValueLiteralExpression) children.get(0);
		} else if (isValueLiteralExpression(children.get(1))) {
			fieldReferenceExpression = getFieldReferenceExpression(children.get(0)).orElse(null);
			valueLiteralExpression = (ValueLiteralExpression) children.get(1);
		}
		if (fieldReferenceExpression == null) {
			return Optional.empty();
		}
		String fieldName = fieldReferenceExpression.getName();
		Object value = extractValueLiteral(fieldReferenceExpression, valueLiteralExpression);
		return Optional.of(Tuple2.of(fieldName, Collections.singleton(value.toString())));
	}

	/**
	 * Extract partition predicate of single partition key from 'in' expression.
	 * */
	private static Optional<Tuple2<String, Set<String>>> extractPartitionPredicateFromIn(
			Expression expression) {
		List<Expression> children = expression.getChildren();
		Preconditions.checkState(children.size() >= 2,
			"Children size of IN Expression must be greater than or equal to 2.");

		Set<String> values = new HashSet<>();
		FieldReferenceExpression fieldReferenceExpression =
			(FieldReferenceExpression) children.get(0);
		String fieldName = fieldReferenceExpression.getName();
		for (int i = 1; i < children.size(); i++) {
			Expression expr = children.get(i);
			if (expr instanceof ValueLiteralExpression) {
				Object value =
					extractValueLiteral(fieldReferenceExpression, (ValueLiteralExpression) expr);
				values.add(value.toString());
			} else {
				return Optional.empty();
			}
		}
		return Optional.of(Tuple2.of(fieldName, values));
	}

	/**
	 * Extract partition predicate of single partition key from 'or' expression.
	 * */
	private static Optional<Tuple2<String, Set<String>>> parsePartitionPredicatesFromOr(
			Expression expression) {
		List<Expression> children = expression.getChildren();

		String fieldName = null;
		Set<String> values = new HashSet<>();
		for (Expression expr : children) {
			Optional<Tuple2<String, Set<String>>> optionalPredicates =
				parsePartitionPredicatesFromEqualsOr(expr);
			if (!optionalPredicates.isPresent()) {
				return Optional.empty();
			}
			Tuple2<String, Set<String>> predicates = optionalPredicates.get();
			if (fieldName == null) {
				fieldName = predicates.f0;
			} else if (!fieldName.equals(predicates.f0)) {
				// we only handle predicates with the same partition key in this method.
				return Optional.empty();
			}
			values.addAll(predicates.f1);
		}
		return Optional.of(Tuple2.of(fieldName, values));
	}

	/**
	 * Extract partition predicate of single partition key from 'equals'/'or' expression.
	 * */
	private static Optional<Tuple2<String, Set<String>>> parsePartitionPredicatesFromEqualsOr(
			Expression expression) {
		if (isEqualsExpression(expression)) {
			return extractPartitionPredicateFromEquals(expression);
		} else if (isOrExpression(expression)) {
			return parsePartitionPredicatesFromOr(expression);
		} else {
			return Optional.empty();
		}
	}

	private static boolean isOrExpression(Expression expression) {
		if (expression instanceof CallExpression) {
			FunctionDefinition definition = ((CallExpression) expression).getFunctionDefinition();
			return BuiltInFunctionDefinitions.OR.equals(definition);
		}
		return false;
	}

	private static boolean isEqualsExpression(Expression expression) {
		if (expression instanceof CallExpression) {
			FunctionDefinition definition = ((CallExpression) expression).getFunctionDefinition();
			return BuiltInFunctionDefinitions.EQUALS.equals(definition);
		}
		return false;
	}

	private static boolean isInExpression(Expression expression) {
		if (expression instanceof CallExpression) {
			FunctionDefinition definition = ((CallExpression) expression).getFunctionDefinition();
			return BuiltInFunctionDefinitions.IN.equals(definition);
		}
		return false;
	}
}
