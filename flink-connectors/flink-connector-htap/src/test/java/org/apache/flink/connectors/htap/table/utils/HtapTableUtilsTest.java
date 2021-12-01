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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.expressions.ApiExpressionUtils.typeLiteral;

/**
 * Unit Tests for {@link HtapTableUtils}.
 */
public class HtapTableUtilsTest {

	private static final ResolvedExpression INT_FIELD = new FieldReferenceExpression(
		"int_test", DataTypes.INT(), 0, 0);

	private static final ResolvedExpression INT_VALUE_1 = new ValueLiteralExpression(
		1, new AtomicDataType(new IntType(false)));

	private static final ResolvedExpression INT_VALUE_2 = new ValueLiteralExpression(
		2, new AtomicDataType(new IntType(false)));

	private static final ResolvedExpression STR_FIELD = new FieldReferenceExpression(
		"str_test", DataTypes.STRING(), 0, 0);

	private static final ResolvedExpression STR_VALUE_1 = new ValueLiteralExpression(
		"str1", new AtomicDataType(new VarCharType(false, 1)));

	private static final ResolvedExpression STR_VALUE_2 = new ValueLiteralExpression(
		"str2", new AtomicDataType(new VarCharType(false, 1)));

	private static final ResolvedExpression STR_VALUE_3 = new ValueLiteralExpression(
		"str3", new AtomicDataType(new VarCharType(false, 1)));

	// `int_test` = 1
	private static final CallExpression INT_EQUAL_EXPRESSION_1 = new CallExpression(
		BuiltInFunctionDefinitions.EQUALS,
		Arrays.asList(INT_FIELD, INT_VALUE_1),
		DataTypes.BOOLEAN());

	// `int_test` = 2
	private static final CallExpression INT_EQUAL_EXPRESSION_2 = new CallExpression(
		BuiltInFunctionDefinitions.EQUALS,
		Arrays.asList(INT_FIELD, INT_VALUE_2),
		DataTypes.BOOLEAN());

	// `str_test` = 'str1'
	private static final CallExpression STR_EQUAL_EXPRESSION_1 = new CallExpression(
		BuiltInFunctionDefinitions.EQUALS,
		Arrays.asList(STR_FIELD, STR_VALUE_1),
		DataTypes.BOOLEAN());

	// `str_test` = 'str2'
	private static final CallExpression STR_EQUAL_EXPRESSION_2 = new CallExpression(
		BuiltInFunctionDefinitions.EQUALS,
		Arrays.asList(STR_FIELD, STR_VALUE_2),
		DataTypes.BOOLEAN());

	// `str_test` = 'str3'
	private static final CallExpression STR_EQUAL_EXPRESSION_3 = new CallExpression(
		BuiltInFunctionDefinitions.EQUALS,
		Arrays.asList(STR_FIELD, STR_VALUE_3),
		DataTypes.BOOLEAN());

	// `int_test` in (1, 2)
	private static final CallExpression INT_IN_EXPRESSION = new CallExpression(
		BuiltInFunctionDefinitions.IN,
		Arrays.asList(INT_FIELD, INT_VALUE_1, INT_VALUE_2),
		DataTypes.BOOLEAN());

	// `str_test` in ('str1', 'str2', 'str3')
	private static final CallExpression STR_IN_EXPRESSION = new CallExpression(
		BuiltInFunctionDefinitions.IN,
		Arrays.asList(STR_FIELD, STR_VALUE_1, STR_VALUE_2, STR_VALUE_3),
		DataTypes.BOOLEAN());

	// `int_test` = 1 or `int_test` = 2
	private static final CallExpression INT_OR_EXPRESSION = new CallExpression(
		BuiltInFunctionDefinitions.OR,
		Arrays.asList(INT_EQUAL_EXPRESSION_1, INT_EQUAL_EXPRESSION_2),
		DataTypes.BOOLEAN());

	// `str_test` = 'str1' or `str_test` = 'str2' or `str_test` = 'str3'
	private static final CallExpression STR_OR_EXPRESSION_1 = new CallExpression(
		BuiltInFunctionDefinitions.OR,
		Arrays.asList(STR_EQUAL_EXPRESSION_1, STR_EQUAL_EXPRESSION_2, STR_EQUAL_EXPRESSION_3),
		DataTypes.BOOLEAN());

	// `str_test` = 'str1' or `str_test` = 'str2' or `str_test` = 'str3'
	private static final CallExpression STR_OR_EXPRESSION_2 = new CallExpression(
		BuiltInFunctionDefinitions.OR,
		Arrays.asList(STR_EQUAL_EXPRESSION_2, STR_EQUAL_EXPRESSION_3),
		DataTypes.BOOLEAN());

	// `str_test` = 'str1' or (`str_test` = 'str2' or `str_test` = 'str3')
	private static final CallExpression STR_OR_EXPRESSION_3 = new CallExpression(
		BuiltInFunctionDefinitions.OR,
		Arrays.asList(STR_EQUAL_EXPRESSION_1, STR_OR_EXPRESSION_2),
		DataTypes.BOOLEAN());

	// `int_test` = 1 or `str_test` = 'str1'
	private static final CallExpression INT_STR_OR_EXPRESSION = new CallExpression(
		BuiltInFunctionDefinitions.OR,
		Arrays.asList(INT_EQUAL_EXPRESSION_1, STR_EQUAL_EXPRESSION_1),
		DataTypes.BOOLEAN());

	// `int_test` = 2 and `str_test` = 'str1'
	private static final CallExpression INT_STR_AND_EXPRESSION = new CallExpression(
		BuiltInFunctionDefinitions.AND,
		Arrays.asList(INT_EQUAL_EXPRESSION_2, STR_EQUAL_EXPRESSION_1),
		DataTypes.BOOLEAN());

	// `int_test` = 1 or (`int_test` = 2 and `str_test` = 'str1')
	private static final CallExpression INT_STR_MIXED_EXPRESSION = new CallExpression(
		BuiltInFunctionDefinitions.OR,
		Arrays.asList(INT_EQUAL_EXPRESSION_1, INT_STR_AND_EXPRESSION),
		DataTypes.BOOLEAN());

	HtapFilterInfo info;
	HtapFilterInfo expected;

	@Test
	public void testConvertUnaryIsNullExpression() {
		// IS_NULL(`field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.IS_NULL,
			Collections.singletonList(INT_FIELD),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// IS_NOT_NULL(`field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.IS_NOT_NULL,
			Collections.singletonList(INT_FIELD),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// IS_NULL(`literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.IS_NULL,
			Collections.singletonList(INT_VALUE_1),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// IS_NOT_NULL(`literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.IS_NOT_NULL,
			Collections.singletonList(INT_VALUE_1),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);
	}

	@Test
	public void testConvertBinaryComparison() {
		List<ResolvedExpression> childrenBeginWithField = Arrays.asList(INT_FIELD, INT_VALUE_1);
		List<ResolvedExpression> childrenBeginWithLiteral = Arrays.asList(INT_VALUE_1, INT_FIELD);
		List<ResolvedExpression> childrenFields = Arrays.asList(INT_FIELD, INT_FIELD);
		List<ResolvedExpression> childrenLiterals = Arrays.asList(INT_VALUE_1, INT_VALUE_1);

		CallExpression castExpression = new CallExpression(
			BuiltInFunctionDefinitions.CAST,
			Arrays.asList(
				new FieldReferenceExpression("int_test", DataTypes.BIGINT(), 0, 0),
				typeLiteral(DataTypes.INT())),
			DataTypes.INT()
		);

		CallExpression castCastExpression = new CallExpression(
			BuiltInFunctionDefinitions.CAST,
			Arrays.asList(
				castExpression,
				typeLiteral(DataTypes.DOUBLE())),
			DataTypes.DOUBLE()
		);

		// >(`field`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN,
			childrenBeginWithField,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// >(`literal`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// >(`field`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN,
			childrenFields,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// >(`literal`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN,
			childrenLiterals,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// <(`field`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN,
			childrenBeginWithField,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// <(`literal`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// <(`field`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN,
			childrenFields,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// <(`literal`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN,
			childrenLiterals,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// =(`field`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			childrenBeginWithField,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// =(`literal`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// =(`field`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			childrenFields,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// =(`literal`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			childrenLiterals,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

//		// =(cast(`int_test` as int), 1)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			Arrays.asList(castExpression, INT_VALUE_1),
			DataTypes.BOOLEAN())).orElse(null);
		expected = HtapFilterInfo.Builder.create("int_test")
			.equalTo(1L)
			.build();

		Assert.assertEquals(info, expected);

		// =(cast(cast(`int_test` as int) as double), 1)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			Arrays.asList(castCastExpression, INT_VALUE_1),
			DataTypes.BOOLEAN())).orElse(null);
		expected = HtapFilterInfo.Builder.create("int_test")
			.equalTo(1L)
			.build();

		Assert.assertEquals(info, expected);

		// >=(`field`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
			childrenBeginWithField,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// >=(`literal`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// >=(`field`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
			childrenFields,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// >=(`literal`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
			childrenLiterals,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// <=(`field`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
			childrenBeginWithField,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// <=(`literal`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// <=(`field`, `field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
			childrenFields,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// <=(`literal`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
			childrenLiterals,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);
	}

	@Test
	public void testConvertIsInExpression() {
		CallExpression equalExpression = new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			Arrays.asList(INT_FIELD, INT_VALUE_1),
			DataTypes.BOOLEAN());

		CallExpression greaterExpression = new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN,
			Arrays.asList(INT_FIELD, INT_VALUE_1),
			DataTypes.BOOLEAN());

		CallExpression lessExpression = new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN,
			Arrays.asList(INT_FIELD, INT_VALUE_1),
			DataTypes.BOOLEAN());

		CallExpression orExpression = new CallExpression(
			BuiltInFunctionDefinitions.OR,
			Arrays.asList(equalExpression, equalExpression),
			DataTypes.BOOLEAN());

		CallExpression andExpression = new CallExpression(
			BuiltInFunctionDefinitions.AND,
			Arrays.asList(equalExpression, equalExpression),
			DataTypes.BOOLEAN());

		// OR(`filed`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.OR,
			Arrays.asList(INT_FIELD, INT_VALUE_1),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// OR(=(`field`, `literal`), =(`field`, `literal`))
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.OR,
			Arrays.asList(equalExpression, equalExpression),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// OR(=(`field`, `literal`), >(`field`, `literal`))
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.OR,
			Arrays.asList(equalExpression, greaterExpression),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// OR(=(`field`, `literal`), <(`field`, `literal`))
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.OR,
			Arrays.asList(equalExpression, lessExpression),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// OR(=(`field`, `literal`), OR(=(`field`, `literal`), =(`field`, `literal`)))
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.OR,
			Arrays.asList(equalExpression, orExpression),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// OR(=(`field`, `literal`), AND(=(`field`, `literal`), =(`field`, `literal`)))
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.OR,
			Arrays.asList(equalExpression, andExpression),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);
	}

	@Test
	public void testExtractPartitionPredicates() {
		// `int_test` = 1
		List<Map<String, Set<String>>> result1 =
			HtapTableUtils.extractPartitionPredicates(Collections.singletonList(INT_EQUAL_EXPRESSION_1));
		List<Map<String, Set<String>>> expected1 =
			Collections.singletonList(Collections.singletonMap("int_test", Collections.singleton("1")));
		Assert.assertEquals(expected1, result1);

		// `int_test` in (1, 2)
		List<Map<String, Set<String>>> result2 =
			HtapTableUtils.extractPartitionPredicates(Collections.singletonList(INT_IN_EXPRESSION));
		Set<String> values2 = new HashSet<>(Arrays.asList("1", "2"));
		List<Map<String, Set<String>>> expected2 =
			Collections.singletonList(Collections.singletonMap("int_test", values2));
		Assert.assertEquals(expected2, result2);

		// `str_test` = 'str1' or `str_test` = 'str2' or `str_test` = 'str3'
		List<Map<String, Set<String>>> result3 =
			HtapTableUtils.extractPartitionPredicates(Collections.singletonList(STR_OR_EXPRESSION_1));
		Set<String> values3 = new HashSet<>(Arrays.asList("str1", "str2", "str3"));
		List<Map<String, Set<String>>> expected3 =
			Collections.singletonList(Collections.singletonMap("str_test", values3));
		Assert.assertEquals(expected3, result3);

		// `str_test` = 'str1' or (`str_test` = 'str2' or `str_test` = 'str3')
		List<Map<String, Set<String>>> result4 =
			HtapTableUtils.extractPartitionPredicates(Collections.singletonList(STR_OR_EXPRESSION_3));
		Set<String> values4 = new HashSet<>(Arrays.asList("str1", "str2", "str3"));
		List<Map<String, Set<String>>> expected4 =
			Collections.singletonList(Collections.singletonMap("str_test", values4));
		Assert.assertEquals(expected4, result4);

		// `int_test` = 1 and `str_test` = 'str1'
		List<Map<String, Set<String>>> result5 =
			HtapTableUtils.extractPartitionPredicates(
				Arrays.asList(INT_EQUAL_EXPRESSION_1, STR_EQUAL_EXPRESSION_1));
		Map<String, Set<String>> map5 = new HashMap<>();
		map5.put("int_test", Collections.singleton("1"));
		map5.put("str_test", Collections.singleton("str1"));
		List<Map<String, Set<String>>> expected5 = Collections.singletonList(map5);
		Assert.assertEquals(expected5, result5);

		// (`int_test` = 1 or `int_test` = 2) and `str_test` in ('str1', 'str2', 'str3')
		List<Map<String, Set<String>>> result6 = HtapTableUtils.extractPartitionPredicates(
			Arrays.asList(INT_OR_EXPRESSION, STR_IN_EXPRESSION));
		Map<String, Set<String>> map6 = new HashMap<>();
		map6.put("int_test", new HashSet<>(Arrays.asList("1", "2")));
		map6.put("str_test", new HashSet<>(Arrays.asList("str1", "str2", "str3")));
		List<Map<String, Set<String>>> expected6 = Collections.singletonList(map6);
		Assert.assertEquals(expected6, result6);

		// `int_test` = 1 or `str_test` = 'str1'
		// This is not supported for now, return an empty list as result.
		List<Map<String, Set<String>>> result7 =
			HtapTableUtils.extractPartitionPredicates(Collections.singletonList(INT_STR_OR_EXPRESSION));
		List<Map<String, Set<String>>> expected7 = Collections.emptyList();
		Assert.assertEquals(expected7, result7);

		// `int_test` = 1 or (`int_test` = 2 and `str_test` = 'str1')
		// This is not supported for now, return an empty list as result.
		List<Map<String, Set<String>>> result8 =
			HtapTableUtils.extractPartitionPredicates(Collections.singletonList(INT_STR_MIXED_EXPRESSION));
		List<Map<String, Set<String>>> expected8 = Collections.emptyList();
		Assert.assertEquals(expected8, result8);
	}
}
