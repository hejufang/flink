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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link RoundRobinOperatorStateRepartitioner}.
 */
public class RoundRobinOperatorStateRepartitionerTest {

	@Test
	public void testMaxSizeHashMap() {
		final RoundRobinOperatorStateRepartitioner partitioner = (RoundRobinOperatorStateRepartitioner) RoundRobinOperatorStateRepartitioner.INSTANCE;
		int times = 100;
		for (int i = 0; i < times; i++) {
			partitioner.repartitionState(
					Collections.singletonList(Collections.singletonList(new PartitionerOperatorStateHandle())),
					1,
					1);
			Assert.assertEquals(Math.min(i + 1, RoundRobinOperatorStateRepartitioner.CACHE_MAX_SIZE), partitioner.getUnionStatesCache().size());
		}
	}

	private static class PartitionerOperatorStateHandle implements OperatorStateHandle {

		@Override
		public Map<String, StateMetaInfo> getStateNameToPartitionOffsets() {
			final Map<String, StateMetaInfo> m = new HashMap<>(1);
			m.put("test-state", new StateMetaInfo(new long[]{1L}, Mode.UNION));
			return m;
		}

		@Override
		public FSDataInputStream openInputStream() throws IOException {
			return new TestStream();
		}

		@Override
		public StreamStateHandle getDelegateStateHandle() {
			return new StreamStateHandle() {
				@Override
				public FSDataInputStream openInputStream() throws IOException {
					return new TestStream();
				}

				@Override
				public void discardState() throws Exception {}

				@Override
				public long getStateSize() {
					return 0;
				}
			};
		}

		@Override
		public void discardState() throws Exception {}

		@Override
		public long getStateSize() {
			return 0;
		}
	}

	private static class TestStream extends FSDataInputStream {

		@Override
		public void seek(long desired) throws IOException {}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public int read() throws IOException {
			return 0;
		}
	}
}
