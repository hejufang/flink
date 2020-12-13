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

import org.apache.flink.cep.Event;
import org.apache.flink.cep.pattern.parser.TestCepEventParser;
import org.apache.flink.cep.pattern.pojo.Condition;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test for conditions.
 */
public class CondtionsTest {

	@Test
	public void testGreaterCondition() throws Exception {
		Condition c1 = new Condition("id", Condition.OpType.GREATER, "1", Condition.ValueType.DOUBLE, null);
		Condition c2 = new Condition("price", Condition.OpType.GREATER, "1", Condition.ValueType.DOUBLE, null);
		final EventParserCondition<Event> condition = new EventParserCondition<>(new TestCepEventParser(), Arrays.asList(c1, c2));

		Assert.assertTrue(condition.filter(new Event(2, "x", 1.5), new TestContext()));
		Assert.assertFalse(condition.filter(new Event(0, "x", 2.0), new TestContext()));
		Assert.assertFalse(condition.filter(new Event(2, "x", 0.5), new TestContext()));
	}

	@Test
	public void testMultipleConditions() throws Exception {
		Condition c1 = new Condition("id", Condition.OpType.EQUAL, "1");
		Condition c2 = new Condition("name", Condition.OpType.EQUAL, "2");
		final EventParserCondition<Event> condition = new EventParserCondition<>(new TestCepEventParser(), Arrays.asList(c1, c2));

		Assert.assertTrue(condition.filter(new Event(1, "2", 1.0), new TestContext()));
		Assert.assertFalse(condition.filter(new Event(1, "3", 1.0), new TestContext()));
	}

	private static class TestContext implements IterativeCondition.Context<Event> {

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
