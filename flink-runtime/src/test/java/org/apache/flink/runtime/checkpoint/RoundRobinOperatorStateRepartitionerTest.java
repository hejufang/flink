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

import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link RoundRobinOperatorStateRepartitioner}.
 */
public class RoundRobinOperatorStateRepartitionerTest {
	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	public final OperatorStateRepartitioner<OperatorStateHandle> repartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

	@Test
	public void testRepartitionUnionState() throws IOException {
		List<List<OperatorStateHandle>> operatorStates = createOperatorStates();
		FileUnionStateAggregator fileUnionStateAggregator = new FileUnionStateAggregator(tempFolder.newFolder("union-state-folder").getCanonicalPath());
		List<List<OperatorStateHandle>> firstResult = repartitioner.repartitionState(operatorStates, 2, 2, fileUnionStateAggregator);
		Assert.assertEquals(firstResult.size(), 2);
		Assert.assertTrue(firstResult.stream().allMatch(list -> list.size() == 3));

		List<List<OperatorStateHandle>> secondResult = repartitioner.repartitionState(operatorStates, 2, 2, fileUnionStateAggregator);
		Assert.assertTrue(firstResult == secondResult);

		List<List<OperatorStateHandle>> thirdResult = repartitioner.repartitionState(operatorStates, 2, 3, fileUnionStateAggregator);
		Assert.assertTrue(firstResult != thirdResult);
		Assert.assertEquals(thirdResult.size(), 3);
		Assert.assertTrue(thirdResult.stream().allMatch(list -> list.size() == 3));
	}

	private List<List<OperatorStateHandle>> createOperatorStates() {
		List<List<OperatorStateHandle>> operatorStates = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			Map<String, OperatorStateHandle.StateMetaInfo> stateNameToPartitionOffsets = new HashMap<>();
			stateNameToPartitionOffsets.put("broadcast-state", new OperatorStateHandle.StateMetaInfo(new long[]{0}, OperatorStateHandle.Mode.BROADCAST));
			stateNameToPartitionOffsets.put("union-state1", new OperatorStateHandle.StateMetaInfo(new long[]{32}, OperatorStateHandle.Mode.UNION));
			stateNameToPartitionOffsets.put("union-state2", new OperatorStateHandle.StateMetaInfo(new long[]{48}, OperatorStateHandle.Mode.UNION));
			stateNameToPartitionOffsets.put("spilt-state", new OperatorStateHandle.StateMetaInfo(new long[]{64}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
			OperatorStateHandle operatorStateHandle = new OperatorStreamStateHandle(stateNameToPartitionOffsets, new ByteStreamStateHandle("operator-state", new byte[128]));
			operatorStates.add(Collections.singletonList(operatorStateHandle));
		}
		return operatorStates;
	}
}
