/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.StatefulFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateRegistry;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.client.cli.CheckpointVerifyResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.Assert;
import org.junit.Test;

/**
 * CheckpointVerifyITCase.
 */
public class CheckpointVerifyITCase extends CheckpointVerifyTestBase {
	private static final String JOB_NAME = "savepoint-job";

	private KeySelector defaultKeySelector = (KeySelector<Integer, String>) value -> value.toString();

	private ValueStateDescriptor defaultValueStateDescriptor = new ValueStateDescriptor("state", StringSerializer.INSTANCE);

	private JobGraph createJobGraph(KeySelector keySelector, ValueStateDescriptor stateDescriptor) {
		StatefulMap statefulMap = new StatefulMap(stateDescriptor);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.addSource(new InfiniteTestSource()).uid("source")
			.keyBy(keySelector)
			.map(statefulMap).uid("map");
		return env.getStreamGraph(JOB_NAME).getJobGraph();
	}

	@Test
	public void testStateTypeChangedIncompatible() throws Exception {
		ValueStateDescriptor after = new ValueStateDescriptor("state", IntSerializer.INSTANCE);
		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, createJobGraph(defaultKeySelector, defaultValueStateDescriptor));
		CheckpointVerifyResult checkpointVerifyResult = verifyCheckpoint(savepointPath, createJobGraph(defaultKeySelector, after));
		Assert.assertEquals(checkpointVerifyResult, CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE);
	}

	@Test
	public void testKeyedSerializerIncompatible() throws Exception {
		KeySelector keySelector = (KeySelector<Integer, Integer>) value -> value;

		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, createJobGraph(defaultKeySelector, defaultValueStateDescriptor));
		CheckpointVerifyResult checkpointVerifyResult = verifyCheckpoint(savepointPath, createJobGraph(keySelector, defaultValueStateDescriptor));
		Assert.assertEquals(checkpointVerifyResult, CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE);
	}

	private static class InfiniteTestSource implements SourceFunction<Integer> {

		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(1);
				}
				Thread.sleep(10);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class StatefulMap extends RichMapFunction<Integer, Integer>
		implements CheckpointedFunction, StatefulFunction {

		private ValueStateDescriptor stateDescriptor;

		StatefulMap(ValueStateDescriptor stateDescriptor){
			this.stateDescriptor = stateDescriptor;
		}

		State state;

		@Override
		public void open(Configuration parameters) throws Exception {}

		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
		}

		@Override
		public void registerState(StateRegistry stateRegistry) throws Exception {
			state = stateRegistry.getState(stateDescriptor);
		}
	}
}
