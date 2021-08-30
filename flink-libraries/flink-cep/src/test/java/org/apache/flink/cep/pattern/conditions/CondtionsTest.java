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

package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.pattern.conditions.v2.EventParserConditionV2;
import org.apache.flink.cep.pattern.parser.TestCepEventParser;
import org.apache.flink.cep.pattern.pojo.AbstractCondition;
import org.apache.flink.cep.pattern.pojo.Condition;
import org.apache.flink.cep.pattern.v2.ConditionGroup;
import org.apache.flink.cep.pattern.v2.LeafCondition;
import org.apache.flink.configuration.Configuration;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Test for conditions.
 */
public class CondtionsTest {

	@Test
	public void testOrCondition() throws Exception {
		LeafCondition c1 = new LeafCondition("name", LeafCondition.OpType.EQUAL, "v1");
		LeafCondition c2 = new LeafCondition("name", LeafCondition.OpType.EQUAL, "v2");
		LeafCondition c3 = new LeafCondition("id", LeafCondition.OpType.EQUAL, "1");
		LeafCondition c4 = new LeafCondition("price", LeafCondition.OpType.EQUAL, "2", AbstractCondition.ValueType.DOUBLE);

		ConditionGroup g1 = new ConditionGroup(LeafCondition.OpType.OR, new ArrayList<>(), Arrays.asList(c1, c2));
		ConditionGroup g2 = new ConditionGroup(LeafCondition.OpType.AND, new ArrayList<>(), Arrays.asList(c3, c4));
		ConditionGroup g = new ConditionGroup(LeafCondition.OpType.OR, Arrays.asList(g1, g2), new ArrayList<>());
		final EventParserConditionV2<Event> condition = new EventParserConditionV2<>(new TestCepEventParser(), g, "-1");
		condition.open(new Configuration());
		Assert.assertTrue(condition.filter(new Event(3, "v1", 1.0), new TestContext()));
		Assert.assertTrue(condition.filter(new Event(3, "v2", 1.0), new TestContext()));
		Assert.assertTrue(condition.filter(new Event(1, "v3", 2), new TestContext()));
		Assert.assertFalse(condition.filter(new Event(2, "v4", 2.0), new TestContext()));
	}

	@Test
	public void testNotEqual() throws Exception {
		Condition c1 = new Condition("id", Condition.OpType.NOT_EQUAL, "1", Condition.ValueType.STRING, null, null);
		final EventParserCondition<Event> condition = new EventParserCondition<>(new TestCepEventParser(), Arrays.asList(c1), "-1");
		Assert.assertFalse(condition.filter(new Event(1, "x", 1.0), new TestContext()));
		Assert.assertTrue(condition.filter(new Event(2, "x", 1.0), new TestContext()));
	}

	@Test
	public void testInCondition() throws Exception {
		Condition c1 = new Condition("id", Condition.OpType.IN, "1,3,5", Condition.ValueType.DOUBLE, null, null);
		final EventParserCondition<Event> condition = new EventParserCondition<>(new TestCepEventParser(), Arrays.asList(c1), "-1");
		Assert.assertFalse(condition.filter(new Event(2, "x", 1.0), new TestContext()));
		Assert.assertTrue(condition.filter(new Event(5, "x", 1.0), new TestContext()));
	}

	@Test
	public void testLessAndEqualCondition() throws Exception {
		Condition c2 = new Condition("price", Condition.OpType.GREATER_EQUAL, "1", Condition.ValueType.DOUBLE, null, null);
		final EventParserCondition<Event> condition = new EventParserCondition<>(new TestCepEventParser(), Arrays.asList(c2), "-1");
		Assert.assertTrue(condition.filter(new Event(2, "x", 1.0), new TestContext()));
		Assert.assertFalse(condition.filter(new Event(2, "x", 0.9), new TestContext()));
	}

	@Test
	public void testGreaterCondition() throws Exception {
		Condition c1 = new Condition("id", Condition.OpType.GREATER, "1", Condition.ValueType.DOUBLE, null, null);
		Condition c2 = new Condition("price", Condition.OpType.GREATER, "1", Condition.ValueType.DOUBLE, null, null);
		final EventParserCondition<Event> condition = new EventParserCondition<>(new TestCepEventParser(), Arrays.asList(c1, c2), "-1");

		Assert.assertTrue(condition.filter(new Event(2, "x", 1.5), new TestContext()));
		Assert.assertFalse(condition.filter(new Event(0, "x", 2.0), new TestContext()));
		Assert.assertFalse(condition.filter(new Event(2, "x", 0.5), new TestContext()));
	}

	@Test
	public void testMultipleConditions() throws Exception {
		Condition c1 = new Condition("id", Condition.OpType.EQUAL, "1");
		Condition c2 = new Condition("name", Condition.OpType.EQUAL, "2");
		final EventParserCondition<Event> condition = new EventParserCondition<>(new TestCepEventParser(), Arrays.asList(c1, c2), "-1");

		Assert.assertTrue(condition.filter(new Event(1, "2", 1.0), new TestContext()));
		Assert.assertFalse(condition.filter(new Event(1, "3", 1.0), new TestContext()));
	}

	private static class TestContext implements IterativeCondition.Context<Event> {

		@Override
		public <ACC> ACC getAccumulator(String stateKey, TypeSerializer<ACC> serializer) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public <ACC> void putAccumulator(String stateKey, ACC accumulator, TypeSerializer<ACC> serializer) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterable<Event> getEventsForPattern(String name) throws Exception {
			return null;
		}

		@Override
		public long timestamp() {
			return 0;
		}

		@Override
		public long currentProcessingTime() {
			return 0;
		}
	}
}
