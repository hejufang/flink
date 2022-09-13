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

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link Predicate} builder, who traverses a {@link Expression} tree, and produces
 * the {@code Predicate}.
 *
 * <p>This is designed to be extensible, so dialects could build specific
 * {@code PredicateBuilder}.
 */
public class PredicateBuilder implements ExpressionVisitor<Optional<Predicate>> {

	private final Map<BuiltInFunctionDefinition, Converter> converterMap;
	private final Function<String, String> quoting;

	public PredicateBuilder(
			Map<BuiltInFunctionDefinition, Converter> converterMap,
			Function<String, String> quoting) {
		this.converterMap = converterMap;
		this.quoting = quoting;
	}

	@Override
	public Optional<Predicate> visit(CallExpression call) {
		if (!converterMap.containsKey(call.getFunctionDefinition())) {
			return Optional.empty();
		}
		final Converter converter = converterMap.get(call.getFunctionDefinition());
		Predicate[] operands = new Predicate[call.getChildren().size()];
		for (int i = 0; i < operands.length; ++i) {
			Optional<Predicate> operand = call.getChildren().get(i).accept(this);
			if (!operand.isPresent()) {
				return Optional.empty();
			}
			operands[i] = operand.get();
		}
		return Optional.of(converter.apply(operands));
	}

	@Override
	public Optional<Predicate> visit(ValueLiteralExpression valueLiteral) {
		Object object = null;
		switch (valueLiteral.getOutputDataType().getLogicalType().getTypeRoot()) {
			case INTEGER:
				object = valueLiteral.getValueAs(Integer.class).get();
				break;
			case BIGINT:
				object = valueLiteral.getValueAs(Long.class).get();
				break;
			case SMALLINT:
				object = valueLiteral.getValueAs(Short.class).get();
				break;
			case TINYINT:
				object = valueLiteral.getValueAs(Byte.class).get();
				break;
			case FLOAT:
				object = valueLiteral.getValueAs(Float.class).get();
				break;
			case DOUBLE:
				object = valueLiteral.getValueAs(Double.class).get();
				break;
			case DECIMAL:
				object = valueLiteral.getValueAs(BigDecimal.class).get();
				break;
			case BOOLEAN:
				object = valueLiteral.getValueAs(Boolean.class).get();
				break;
			case CHAR:
			case VARCHAR:
				object = valueLiteral.getValueAs(String.class).get();
				break;
			case BINARY:
			case VARBINARY:
				object = valueLiteral.getValueAs(byte[].class).get();
				break;
			case DATE:
				object = valueLiteral.getValueAs(LocalDate.class).get();
				break;
			case TIME_WITHOUT_TIME_ZONE:
				object = valueLiteral.getValueAs(LocalTime.class).get();
				break;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				object = valueLiteral.getValueAs(LocalDateTime.class).get();
				break;
			case NULL:
				object = null;
				break;
			default:
				return Optional.empty();
		}
		return Optional.of(new Predicate("?", new Object[]{object}));
	}

	@Override
	public Optional<Predicate> visit(FieldReferenceExpression fieldReference) {
		final String result = quoting.apply(fieldReference.getName());
		return Optional.of(new Predicate(result));
	}

	@Override
	public Optional<Predicate> visit(TypeLiteralExpression typeLiteral) {
		// TODO: support cast, extract
		return Optional.empty();
	}

	@Override
	public Optional<Predicate> visit(Expression other) {
		return Optional.empty();
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * The builder for {@link PredicateBuilder}.
	 */
	public static class Builder {

		private final Map<BuiltInFunctionDefinition, Converter> converterMap = new HashMap<>();
		private Function<String, String> quoting = null;

		private Builder() {
		}

		public Builder withStandardConverters() {
			converterMap.put(BuiltInFunctionDefinitions.EQUALS, new BinaryConverter("="));
			converterMap.put(BuiltInFunctionDefinitions.NOT_EQUALS, new BinaryConverter("<>"));
			converterMap.put(BuiltInFunctionDefinitions.LESS_THAN, new BinaryConverter("<"));
			converterMap.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, new BinaryConverter("<="));
			converterMap.put(BuiltInFunctionDefinitions.GREATER_THAN, new BinaryConverter(">"));
			converterMap.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, new BinaryConverter(">="));
			converterMap.put(BuiltInFunctionDefinitions.AND, new BinaryConverter("AND"));
			converterMap.put(BuiltInFunctionDefinitions.OR, new BinaryConverter("OR"));

			converterMap.put(BuiltInFunctionDefinitions.NOT, new PrefixConverter("NOT"));
			converterMap.put(BuiltInFunctionDefinitions.MINUS_PREFIX, new PrefixConverter("-"));

			converterMap.put(BuiltInFunctionDefinitions.IS_NOT_NULL, new PostfixConverter("IS NOT NULL"));
			converterMap.put(BuiltInFunctionDefinitions.IS_NULL, new PostfixConverter("IS NULL"));
			converterMap.put(BuiltInFunctionDefinitions.IS_TRUE, new PostfixConverter("IS TRUE"));
			converterMap.put(BuiltInFunctionDefinitions.IS_FALSE, new PostfixConverter("IS FALSE"));
			converterMap.put(BuiltInFunctionDefinitions.IS_NOT_TRUE, new PostfixConverter("IS NOT TRUE"));
			converterMap.put(BuiltInFunctionDefinitions.IS_NOT_FALSE, new PostfixConverter("IS NOT FALSE"));

			converterMap.put(BuiltInFunctionDefinitions.IN, new InConverter());
			converterMap.put(BuiltInFunctionDefinitions.BETWEEN, new BetweenConverter());
			return this;
		}

		public Builder addConverter(BuiltInFunctionDefinition function, Converter converter) {
			converterMap.put(function, converter);
			return this;
		}

		public Builder withQuoting(Function<String, String> quoting) {
			this.quoting = quoting;
			return this;
		}

		public PredicateBuilder build() {
			Preconditions.checkNotNull(quoting);
			return new PredicateBuilder(converterMap, quoting);
		}
	}
}
