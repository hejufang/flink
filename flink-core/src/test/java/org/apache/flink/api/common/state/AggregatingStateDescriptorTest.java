/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the {@link  AggregatingStateDescriptor}.
 */
public class AggregatingStateDescriptorTest {

	@Test
	public void testAggregatingStateDescriptor() throws Exception {

		AggregateFunction sumAggr = new SumAggr();
		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> descr =
					new AggregatingStateDescriptor<>("testName", sumAggr, StringSerializer.INSTANCE);

		assertEquals("testName", descr.getName());
		assertNotNull(descr.getSerializer());
		assertEquals(sumAggr, descr.getAggregateFunction());

		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> copy = CommonTestUtils.createCopySerializable(descr);

		assertEquals("testName", copy.getName());
		assertNotNull(copy.getSerializer());
		assertEquals(StringSerializer.INSTANCE, copy.getSerializer());
	}

	@Test
	public void testHashCodeEquals() throws Exception {
		final String name = "testName";
		AggregateFunction sumAggr = new SumAggr();

		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> original = new AggregatingStateDescriptor<>(name, sumAggr, String.class);
		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> same = new AggregatingStateDescriptor<>(name, sumAggr, String.class);
		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> sameBySerializer = new AggregatingStateDescriptor<>(name, sumAggr, StringSerializer.INSTANCE);
		// test that hashCode() works on state descriptors with initialized and uninitialized serializers
		assertEquals(original.hashCode(), same.hashCode());
		assertEquals(original.hashCode(), sameBySerializer.hashCode());

		assertEquals(original, same);
		assertEquals(original, sameBySerializer);

		// equality with a clone
		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> clone = CommonTestUtils.createCopySerializable(original);
		assertEquals(original, clone);

		// equality with an initialized
		clone.initializeSerializerUnlessSet(new ExecutionConfig());
		assertEquals(original, clone);

		original.initializeSerializerUnlessSet(new ExecutionConfig());
		assertEquals(original, same);
		// equality with an initialized
		clone.initializeSerializerUnlessSet(new ExecutionConfig());
		assertEquals(original, clone);

		original.initializeSerializerUnlessSet(new ExecutionConfig());
		assertEquals(original, same);
	}

	/**
	 * FLINK-6775.
	 *
	 * <p>Tests that the returned serializer is duplicated. This allows to
	 * share the state descriptor.
	 */
	@Test
	public void testStateDescriptorDuplication() {
		final String name = "testName";
		final AggregateFunction sumAggr = new SumAggr();

		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> original = new AggregatingStateDescriptor<>(name, sumAggr, String.class);
		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> same = original.duplicate();
		AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> sameBySerializer = new AggregatingStateDescriptor<>(name, sumAggr, StringSerializer.INSTANCE);

		// test that hashCode() works on state descriptors with initialized and uninitialized serializers
		assertEquals(original.hashCode(), same.hashCode());
		assertEquals(same.hashCode(), sameBySerializer.hashCode());

		same.initializeSerializerUnlessSet(new ExecutionConfig());
		original.initializeSerializerUnlessSet(new ExecutionConfig());

		assertEquals(original.getSerializer(), same.getSerializer());
		assertEquals(same.getSerializer(), sameBySerializer.getSerializer());
	}

		/**
	 * Test {@link AggregateFunction} concatenating the already stored string with the long passed as argument.
	 */
	private static class SumAggr implements AggregateFunction<Tuple2<Integer, Long>, String, String> {

		private static final long serialVersionUID = -6249227626701264599L;

		@Override
		public String createAccumulator() {
			return "0";
		}

		@Override
		public String add(Tuple2<Integer, Long> value, String accumulator) {
			long acc = Long.valueOf(accumulator);
			acc += value.f1;
			return Long.toString(acc);
		}

		@Override
		public String getResult(String accumulator) {
			return accumulator;
		}

		@Override
		public String merge(String a, String b) {
			return Long.toString(Long.valueOf(a) + Long.valueOf(b));
		}
	}
}
