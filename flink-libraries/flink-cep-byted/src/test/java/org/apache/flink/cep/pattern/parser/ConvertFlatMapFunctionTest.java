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

package org.apache.flink.cep.pattern.parser;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.pojo.AbstractPatternPojo;
import org.apache.flink.cep.pattern.pojo.Condition;
import org.apache.flink.cep.pattern.pojo.Event;
import org.apache.flink.cep.pattern.pojo.PatternBody;
import org.apache.flink.cep.pattern.pojo.PatternPojo;
import org.apache.flink.cep.pattern.v2.PatternPojoV2;
import org.apache.flink.cep.test.TestData;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for conversion from pojo to pattern.
 */
public class ConvertFlatMapFunctionTest {

	@Test
	public void testConditionGroup() throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojoV2 pojo = (PatternPojoV2) objectMapper.readValue(TestData.CONDITION_GROUP_PATTERN, AbstractPatternPojo.class);
		Pattern<?, ?> result = PatternConverter.buildPattern(pojo, new TestCepEventParserFactory().create());
		Assert.assertNotNull(result.getPatternId());
	}

	@Test
	public void testDisabledPattern() throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojo pojo = objectMapper.readValue(TestData.disablePattern("test_agg"), PatternPojo.class);
		Pattern<?, ?> result = PatternConverter.buildPattern(pojo, new TestCepEventParserFactory().create());
		Assert.assertNotNull(result.getPatternId());
		Assert.assertTrue(result.isDisabled());
	}

	@Test
	public void testAggregationPattern() {
		Event begin = new Event("begin", null, null, Collections.singletonList(
				new Condition("a", Condition.OpType.EQUAL, "2", Condition.ValueType.LONG, Condition.AggregationType.SUM, new ArrayList<>())));

		PatternBody body = new PatternBody(Collections.singletonList(begin), new HashMap<>());
		PatternPojo pojo = new PatternPojo("test_pattern", body, null);
		Pattern<?, ?> result = PatternConverter.buildPattern(pojo, new TestCepEventParserFactory().create());

		Assert.assertEquals("test_pattern", result.getPatternId());
	}

	@Test
	public void testFollowedByPattern() {
		Event begin = new Event("begin", null, null, Collections.singletonList(new Condition("a", Condition.OpType.EQUAL, "a1")));
		Event middle = new Event("middle", Event.ConnectionType.FOLLOWED_BY, "begin", Collections.singletonList(new Condition("b", Condition.OpType.EQUAL, "b1")));
		Event end = new Event("end", Event.ConnectionType.FOLLOWED_BY, "middle", Collections.singletonList(new Condition("c", Condition.OpType.EQUAL, "c1")));

		PatternBody body = new PatternBody(Arrays.asList(end, middle, begin), new HashMap<>());
		PatternPojo pojo = new PatternPojo("test_pattern", body, null);
		Pattern<?, ?> result = PatternConverter.buildPattern(pojo, new TestCepEventParserFactory().create());

		Assert.assertEquals("test_pattern", result.getPatternId());
	}

	@Test
	public void testNotFollowedByPattern() {
		Event begin = new Event("begin", null, null, Collections.singletonList(new Condition("a", Condition.OpType.EQUAL, "a1")));
		Event middle = new Event("middle", Event.ConnectionType.NOT_FOLLOWED_BY, "begin", Collections.singletonList(new Condition("b", Condition.OpType.EQUAL, "b1")));

		Map<PatternBody.AttributeType, String> attrs = new HashMap<>();
		attrs.put(PatternBody.AttributeType.WINDOW, "1000");

		PatternBody body = new PatternBody(Arrays.asList(middle, begin), attrs);
		PatternPojo pojo = new PatternPojo("test_pattern", body, null);
		Pattern<?, ?> result = PatternConverter.buildPattern(pojo, new TestCepEventParserFactory().create());

		Assert.assertEquals("test_pattern", result.getPatternId());
		Assert.assertEquals(1000, result.getWindowTime().toMilliseconds());
	}

	@Test
	public void testAllowSinglePartialMatchPerKeyPattern() {
		Event begin = new Event("begin", null, null, Collections.singletonList(new Condition("a", Condition.OpType.EQUAL, "a1")));
		Map<PatternBody.AttributeType, String> attrs = new HashMap<>();
		attrs.put(PatternBody.AttributeType.ALLOW_SINGLE_PARTIAL_MATCH_PER_KEY, "true");

		PatternBody body = new PatternBody(Collections.singletonList(begin), attrs);
		PatternPojo pojo = new PatternPojo("test_pattern", body, null);
		Pattern<?, ?> result = PatternConverter.buildPattern(pojo, new TestCepEventParserFactory().create());

		Assert.assertEquals("test_pattern", result.getPatternId());
		Assert.assertTrue(result.isAllowSinglePartialMatchPerKey());
	}

	private static class TestCepEventParserFactory implements CepEventParserFactory {

		@Override
		public CepEventParser create() {
			return new TestCepEventParser();
		}
	}

	private static class TestCepEventParser extends CepEventParser {

		@Override
		public String get(String key, CepEvent data) {
			return key + "1";
		}

		@Override
		public CepEventParser duplicate() {
			return new TestCepEventParser();
		}
	}
}
