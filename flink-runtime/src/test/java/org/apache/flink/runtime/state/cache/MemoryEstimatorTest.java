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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.cache.memory.MapStateMemoryEstimator;
import org.apache.flink.runtime.state.cache.memory.ValueStateSerializerEstimator;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Objects;

/**
 * Tests for the {@link org.apache.flink.runtime.state.cache.memory.MemoryEstimator}.
 */
public class MemoryEstimatorTest {
	private static final String TEST_KEY = "test_key";
	private static final String TEST_USER_KEY = "test_user_key";
	private static final Long TEST_VALUE = 12345678L;

	@Test
	public void testValueStateSerializerEstimator() {
		ValueStateSerializerEstimator<String, VoidNamespace, Long> estimator =
			new ValueStateSerializerEstimator<>(StringSerializer.INSTANCE, VoidNamespaceSerializer.INSTANCE, LongSerializer.INSTANCE);
		try {
			estimator.updateEstimatedSize(new CacheEntryKey<>(TEST_KEY, VoidNamespace.INSTANCE), TEST_VALUE);
			long size = estimator.getEstimatedSize();
			Assert.assertEquals(18, size); // key: 8 + 1, namespace: 0, value: 8 + 1
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testMapStateSerializerEstimator() {
		MapStateMemoryEstimator<String, TimeWindow, String, Long> estimator =
			new MapStateMemoryEstimator<>(StringSerializer.INSTANCE, TimeWindow.Serializer.INSTANCE, new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE));
		try {
			estimator.updateEstimatedSize(new CacheEntryKey<>(TEST_KEY, new TimeWindow(TEST_KEY, 0L, 1L), TEST_USER_KEY), TEST_VALUE);
			long size = estimator.getEstimatedSize();
			Assert.assertEquals(58, size); // key: 8 + 13 + 2, namespace: 10 + 8 + 8, value: 8 + 1
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * Time window for namespace.
	 */
	public static class TimeWindow {
		private final String name;
		private final long start;
		private final long end;

		public TimeWindow(@Nonnull String name, long start, long end) {
			this.name = name;
			this.start = start;
			this.end = end;
		}

		public long getStart() {
			return start;
		}

		public long getEnd() {
			return end;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TimeWindow window = (TimeWindow) o;

			return Objects.equals(name, window.name) && end == window.end && start == window.start;
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, start, end);
		}

		@Override
		public String toString() {
			return "TimeWindow{" +
				"name='" + name + '\'' +
				", start=" + start +
				", end=" + end +
				'}';
		}

		// ------------------------------------------------------------------------
		// Serializer
		// ------------------------------------------------------------------------

		/**
		 * The serializer used to write the TimeWindow type.
		 */
		public static class Serializer extends TypeSerializerSingleton<TimeWindow> {
			public static final Serializer INSTANCE = new Serializer();
			private static final long serialVersionUID = 1L;

			@Override
			public boolean isImmutableType() {
				return true;
			}

			@Override
			public TimeWindow createInstance() {
				return null;
			}

			@Override
			public TimeWindow copy(TimeWindow from) {
				return from;
			}

			@Override
			public TimeWindow copy(TimeWindow from, TimeWindow reuse) {
				return from;
			}

			@Override
			public int getLength() {
				return -1;
			}

			@Override
			public void serialize(TimeWindow record, DataOutputView target) throws IOException {
				target.writeUTF(record.name);
				target.writeLong(record.start);
				target.writeLong(record.end);
			}

			@Override
			public TimeWindow deserialize(DataInputView source) throws IOException {
				String name = source.readUTF();
				long start = source.readLong();
				long end = source.readLong();
				return new TimeWindow(name, start, end);
			}

			@Override
			public TimeWindow deserialize(TimeWindow reuse, DataInputView source) throws IOException {
				return deserialize(source);
			}

			@Override
			public void copy(DataInputView source, DataOutputView target) throws IOException {
				target.writeUTF(source.readUTF());
				target.writeLong(source.readLong());
				target.writeLong(source.readLong());
			}

			// ------------------------------------------------------------------------

			@Override
			public TypeSerializerSnapshot<TimeWindow> snapshotConfiguration() {
				return new TimeWindowSerializerSnapshot();
			}

			/**
			 * Serializer configuration snapshot for compatibility and format evolution.
			 */
			@SuppressWarnings("WeakerAccess")
			public static final class TimeWindowSerializerSnapshot extends SimpleTypeSerializerSnapshot<TimeWindow> {

				public TimeWindowSerializerSnapshot() {
					super(Serializer::new);
				}
			}
		}
	}
}
