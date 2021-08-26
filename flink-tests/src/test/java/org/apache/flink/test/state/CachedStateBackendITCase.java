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

package org.apache.flink.test.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.cache.CacheConfigurableOptions;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;
import static org.apache.flink.configuration.CheckpointingOptions.STATE_BACKEND;
import static org.junit.Assert.assertEquals;

/**
 * Test for CachedStateBackend.
 */
public class CachedStateBackendITCase {

	private static final OperatorID OPERATOR_ID = new OperatorID(42L, 42L);
	private static final int RECORD_NUM = 128;

	private static int outerLoopIndex; // We use outer/innerLoopIndex to record the amount of data that has been sent.
	private static int innerLoopIndex;

	private TemporaryFolder temporaryFolder;

	@Before
	public void setup() throws IOException {
		temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();
		outerLoopIndex = 0;
		innerLoopIndex = 0;
	}

	/**
	 * Test state access is correct in non-ttl mode.
	 */
	@Test
	public void testStateAccessCorrectlyWithoutTtl() throws Exception {
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = new CounterOperator("cache-", null);
		StreamTaskMailboxTestHarness<Tuple2<String, Integer>> testHarness = createTestHarness(OPERATOR_ID, operator, Optional.empty());

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		for (int i = 0; i < RECORD_NUM; i++) {
			expectedOutput.add(new StreamRecord<>(Tuple2.of("test" + i, 3)));
		}

		processRecords(testHarness, false);

		testHarness.process();
		testHarness.waitForTaskCompletion();
		testHarness.close();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * Test that ttlState access is correct under the semantics of NeverReturnExpired.
	 */
	@Test
	public void testTTlStateNeverReturnExpired() throws Exception {
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = new CounterOperator("cache-", StateTtlConfig.StateVisibility.NeverReturnExpired);
		StreamTaskMailboxTestHarness<Tuple2<String, Integer>> testHarness = createTestHarness(OPERATOR_ID, operator, Optional.empty());

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		for (int i = 0; i < RECORD_NUM; i++) {
			expectedOutput.add(new StreamRecord<>(Tuple2.of("test" + i, 2)));
		}

		processRecords(testHarness, false);

		testHarness.process();
		testHarness.waitForTaskCompletion();
		testHarness.close();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * Test that ttlState access is correct under the semantics of ReturnExpiredIfNotCleanedUp.
	 */
	@Test
	public void testTTlStateReturnExpiredIfNotCleanedUp() throws Exception {
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = new CounterOperator("cache-", StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp);
		StreamTaskMailboxTestHarness<Tuple2<String, Integer>> testHarness = createTestHarness(OPERATOR_ID, operator, Optional.empty());

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		for (int i = 0; i < RECORD_NUM; i++) {
			expectedOutput.add(new StreamRecord<>(Tuple2.of("test" + i, 3)));
		}

		processRecords(testHarness, false);

		testHarness.process();
		testHarness.waitForTaskCompletion();
		testHarness.close();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * The state access is correct after the task is restored from the checkpoint.
	 */
	@Test
	public void testCheckpointAndRestore() throws Exception {
		OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = new CounterOperator("cache-", null);
		StreamTaskMailboxTestHarness<Tuple2<String, Integer>> testHarness = createTestHarness(OPERATOR_ID, operator, Optional.empty());

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		for (int i = 0; i < RECORD_NUM; i++) {
			expectedOutput.add(new StreamRecord<>(Tuple2.of("test" + i, 3)));
		}

		processRecords(testHarness, true);

		testHarness.process();
		testHarness.waitForTaskCompletion();
		testHarness.close();

		testHarness.getOutput().removeIf(o -> o instanceof CheckpointBarrier);
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		long reportedCheckpointId = testHarness.getTaskStateManager().getReportedCheckpointId();
		assertEquals(1L, reportedCheckpointId);
		JobManagerTaskRestore jobManagerTaskRestore = new JobManagerTaskRestore(
			testHarness.getTaskStateManager().getReportedCheckpointId(),
			testHarness.getTaskStateManager().getLastJobManagerTaskStateSnapshot());

		// restore from checkpoint
		testHarness = createTestHarness(OPERATOR_ID, operator, Optional.of(jobManagerTaskRestore));
		processRecords(testHarness, false);

		testHarness.process();
		testHarness.waitForTaskCompletion();
		testHarness.close();
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	/**
	 * Create a task test environment.
	 */
	private StreamTaskMailboxTestHarness<Tuple2<String, Integer>> createTestHarness(
			OperatorID operatorID,
			OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator,
			Optional<JobManagerTaskRestore> restore) throws Exception {

		Configuration configuration = new Configuration();
		configuration.setString(STATE_BACKEND.key(), "rocksdb");
		configuration.setString("state.backend.rocksdb.memory.managed", "false"); // Avoid rocksdb compaction.

		configuration.set(CacheConfigurableOptions.CACHE_ENABLED, Boolean.TRUE);
		configuration.set(CacheConfigurableOptions.CACHE_MAX_HEAP_SIZE, MemorySize.parse("1m"));
		configuration.set(CacheConfigurableOptions.CACHE_INITIAL_SIZE, MemorySize.parse("128b"));
		configuration.set(CacheConfigurableOptions.CACHE_BLOCK_SIZE, MemorySize.parse("32b"));

		File file = temporaryFolder.newFolder();
		configuration.setString(CHECKPOINTS_DIRECTORY.key(), file.toURI().toString());
		configuration.setString(INCREMENTAL_CHECKPOINTS.key(), "true");

		StreamTaskMailboxTestHarnessBuilder<Tuple2<String, Integer>> builder =
			new StreamTaskMailboxTestHarnessBuilder<>(OneInputStreamTask::new, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
				.addInput(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
				.setupOutputForSingletonOperatorChain(SimpleOperatorFactory.of(operator), operatorID)
				.setTaskManagerRuntimeInfo(new TestingTaskManagerRuntimeInfo(configuration,
					System.getProperty("java.io.tmpdir").split(",|" + File.pathSeparator)))
				.setKeyType(BasicTypeInfo.STRING_TYPE_INFO);

		if (restore.isPresent()) {
			JobManagerTaskRestore taskRestore = restore.get();
			builder.setTaskStateSnapshot(
				taskRestore.getRestoreCheckpointId(),
				taskRestore.getTaskStateSnapshot());
		}

		final StreamTaskMailboxTestHarness<Tuple2<String, Integer>> testHarness = builder.build();
		return testHarness;
	}

	/**
	 * Simulate our task received the data.
	 */
	private void processRecords(StreamTaskMailboxTestHarness<Tuple2<String, Integer>> testHarness, boolean triggerCheckpoint) throws Exception {

		for (int i = outerLoopIndex; i < 2; i++) { // loop twice to make sure cache miss
			Thread.sleep(1000L);
			for (int j = innerLoopIndex; j < RECORD_NUM; j++) {
				testHarness.processElement(new StreamRecord<>(Tuple2.of("test" + j, i + 1)), 0, 0);
			}
			if (triggerCheckpoint) {
				long checkpointId = 1L;
				testHarness.processEvent(new CheckpointBarrier(checkpointId, System.currentTimeMillis(), CheckpointOptions.forCheckpointWithDefaultLocation()));
			}
		}

		// collect state
		for (int i = 0; i < RECORD_NUM; i++) {
			testHarness.processElement(new StreamRecord<>(Tuple2.of("test" + i, Integer.MIN_VALUE)), 0, 0);
		}
		testHarness.endInput();
	}

	/**
	 * Operator that counts processed messages and keeps result on state.
	 */
	private static class CounterOperator extends AbstractStreamOperator<Tuple2<String, Integer>>
		implements OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> {

		private static final long serialVersionUID = -1L;

		private ValueState<Integer> counterState;
		private ListState<Integer> totalCount;

		private String prefix;
		private StateTtlConfig.StateVisibility visibility;
		private int count;

		CounterOperator(String prefix, StateTtlConfig.StateVisibility visibility) {
			this.prefix = prefix;
			this.visibility = visibility;
		}

		@Override
		public void processElement(StreamRecord<Tuple2<String, Integer>> element) throws Exception {
			Tuple2<String, Integer> record = element.getValue();
			setCurrentKey(record.f0);
			if (record.f1 == Integer.MIN_VALUE) {
				output.collect(new StreamRecord<>(Tuple2.of(record.f0, counterState.value())));
			} else {
				count++;
				Integer oriValue = counterState.value();
				if (oriValue == null) {
					counterState.update(record.f1);
				} else {
					counterState.update(oriValue + record.f1);
				}
			}
		}

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {
			super.initializeState(context);
			ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>(prefix + "counter-state", IntSerializer.INSTANCE);
			if (visibility != null) {
				StateTtlConfig.Builder builder = StateTtlConfig.newBuilder(Time.milliseconds(500L));
				if (visibility == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) {
					builder.returnExpiredIfNotCleanedUp();
				} else {
					builder.neverReturnExpired();
				}
				builder.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite);
				valueStateDescriptor.enableTimeToLive(builder.build());
			}
			counterState = context
				.getKeyedStateStore()
				.getState(valueStateDescriptor);

			ListStateDescriptor<Integer> listStateDescriptor =
				new ListStateDescriptor<>(
					"total-count",
					Integer.class);
			totalCount = context.getOperatorStateStore().getListState(listStateDescriptor);

			if (context.isRestored()) {
				count = totalCount.get().iterator().next();
			}
		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {
			totalCount.clear();
			totalCount.add(count);
			outerLoopIndex = count  / RECORD_NUM;
			innerLoopIndex = count % RECORD_NUM;
		}
	}
}
