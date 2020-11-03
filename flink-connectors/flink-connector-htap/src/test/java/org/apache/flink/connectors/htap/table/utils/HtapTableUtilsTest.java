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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * HtapTableUtils Unit Tests.
 */
public class HtapTableUtilsTest {

	ResolvedExpression testFiled = new FieldReferenceExpression(
		"test", DataTypes.INT(), 0, 0);

	ResolvedExpression testValue = new ValueLiteralExpression(
		1, new AtomicDataType(new IntType(false)));

	HtapFilterInfo info;

	@Test
	public void testConvertUnaryIsNullExpression() {
		// IS_NULL(`field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.IS_NULL,
			Collections.singletonList(testFiled),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// IS_NOT_NULL(`field`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.IS_NOT_NULL,
			Collections.singletonList(testFiled),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// IS_NULL(`literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.IS_NULL,
			Collections.singletonList(testValue),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);

		// IS_NOT_NULL(`literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.IS_NOT_NULL,
			Collections.singletonList(testValue),
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNull(info);
	}

	@Test
	public void testConvertBinaryComparison() {
		List<ResolvedExpression> childrenBeginWithFiled = Arrays.asList(testFiled, testValue);
		List<ResolvedExpression> childrenBeginWithLiteral = Arrays.asList(testValue, testFiled);
		List<ResolvedExpression> childrenFields = Arrays.asList(testFiled, testFiled);
		List<ResolvedExpression> childrenLiterals = Arrays.asList(testValue, testValue);

		// >(`filed`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN,
			childrenBeginWithFiled,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// >(`literal`, `filed`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// >(`filed`, `filed`)
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

		// <(`filed`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN,
			childrenBeginWithFiled,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// <(`literal`, `filed`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// <(`filed`, `filed`)
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

		// =(`filed`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			childrenBeginWithFiled,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// =(`literal`, `filed`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.EQUALS,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// =(`filed`, `filed`)
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

		// >=(`filed`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
			childrenBeginWithFiled,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// >=(`literal`, `filed`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// >=(`filed`, `filed`)
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

		// <=(`filed`, `literal`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
			childrenBeginWithFiled,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// <=(`literal`, `filed`)
		info = HtapTableUtils.toHtapFilterInfo(new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
			childrenBeginWithLiteral,
			DataTypes.BOOLEAN()))
			.orElse(null);
		Assert.assertNotNull(info);

		// <=(`filed`, `filed`)
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
			Arrays.asList(testFiled, testValue),
			DataTypes.BOOLEAN());

		CallExpression greaterExpression = new CallExpression(
			BuiltInFunctionDefinitions.GREATER_THAN,
			Arrays.asList(testFiled, testValue),
			DataTypes.BOOLEAN());

		CallExpression lessExpression = new CallExpression(
			BuiltInFunctionDefinitions.LESS_THAN,
			Arrays.asList(testFiled, testValue),
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
			Arrays.asList(testFiled, testValue),
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
}
