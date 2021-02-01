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

package org.apache.flink.cep.pattern.pojo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.test.TestData;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests for hash of {@link PatternPojo}.
 */
public class HashTest {

	@Test
	public void testHash1() {
		int hash1 = generateObject1().hashCode();
		int hash2 = generateObject1().hashCode();
		Assert.assertEquals(hash1, hash2);
	}

	@Test
	public void testHash2() throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		final PatternPojo pojo1 = (PatternPojo) objectMapper.readValue(TestData.COUNT_PATTERN_1, AbstractPatternPojo.class);
		final PatternPojo pojo2 = (PatternPojo) objectMapper.readValue(TestData.COUNT_PATTERN_1, AbstractPatternPojo.class);
		System.out.println(pojo1.hashCode());
		Assert.assertEquals(pojo1.hashCode(), pojo2.hashCode());

	}

	PatternPojo generateObject1() {
		Condition c1 = new Condition("a", Condition.OpType.EQUAL, "234");
		Condition c2 = new Condition("b", Condition.OpType.GREATER, "235", Condition.ValueType.DOUBLE, Condition.AggregationType.SUM, Arrays.asList(
				new Condition("c", Condition.OpType.EQUAL, "4"), new Condition("d", Condition.OpType.EQUAL, "5")));
		Event event1 = new Event("event1", null, null, Arrays.asList(c1, c2));
		Event event2 = new Event("event2", Event.ConnectionType.FOLLOWED_BY, "event1", Collections.singletonList(new Condition("x", Condition.OpType.GREATER, "5")));
		PatternBody patternBody = new PatternBody(Arrays.asList(event1), Stream.of(
				Tuple2.of(PatternBody.AttributeType.WINDOW, "10000"),
				Tuple2.of(PatternBody.AttributeType.ALLOW_SINGLE_PARTIAL_MATCH_PER_KEY, "true")).collect(Collectors.toMap(t -> t.f0, t -> t.f1)));
		return new PatternPojo("test", patternBody, PatternPojo.StatusType.ENABLED);
	}
}
