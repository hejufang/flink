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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ITCase for region checkpoint.
 */
public class RegionCheckpointITCase extends TestLogger implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(RegionCheckpointITCase.class);

	private static final int CHECKPOINT_EXPIRE_PERIOD = 3000;
	private static final int CHECKPOINT_INTERVAL = 2000;

	/** Checkpoint Id greater than this will fail if using global checkpoint handler. */
	private static final int COMPLETE_CHECKPOINTS_BEFORE_TESTING = 1;

	private static final AtomicLong latestSuccessfulChckpointId = new AtomicLong(0);
	private static final AtomicLong latestCheckpointId = new AtomicLong(0);

	@Before
	public void setup() {
		latestSuccessfulChckpointId.set(0);
		latestCheckpointId.set(0);
	}

	@Test
	public void testExpiredCheckpoint() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setInteger(CheckpointingOptions.MAX_RETAINED_REGION_SNAPSHOTS, 1000);
		configuration.setBoolean(CheckpointingOptions.REGION_CHECKPOINT_ENABLED, true);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4, configuration);
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
		env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_EXPIRE_PERIOD);

		env.addSource(new TestSource(3)).addSink(new ExpireSink(3));
		env.execute();

		Assert.assertEquals(3, latestSuccessfulChckpointId.get());
		Assert.assertEquals(1L, ExpireSink.notifications.get());
	}

	@Test
	public void testTaskFailedCheckpoint() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setInteger(CheckpointingOptions.MAX_RETAINED_REGION_SNAPSHOTS, 1000);
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
		configuration.setBoolean(CheckpointingOptions.REGION_CHECKPOINT_ENABLED, true);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4, configuration);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
		env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
		env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_EXPIRE_PERIOD);
		env.setParallelism(4);

		env.addSource(new TestSource(3)).addSink(new FailedSink());

		env.execute();

		Assert.assertTrue(latestSuccessfulChckpointId.get() == 3 && FailedSink.exceptionThrown.get());
	}

	@Test
	public void testMaxPercentageRecovery() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setDouble(CheckpointingOptions.MAX_PERCENTAGE_RECOVERY, 0.1);
		configuration.setBoolean(CheckpointingOptions.REGION_CHECKPOINT_ENABLED, true);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4, configuration);
		env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
		env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_EXPIRE_PERIOD);
		env.setParallelism(4);

		env.addSource(new TestSource(3)).addSink(new ExpireSink(3));

		env.execute();

		// only the first checkpoint can be successful because 0.1 * 4 is less than the number of bad regions
		Assert.assertEquals(1, latestSuccessfulChckpointId.get());
	}

	@Test
	public void testMaxRetainedParameter1() throws Exception {
		testMaxRetainedParameterWithExpiration(3, 1, 2);
	}

	@Test
	public void testMaxRetainedParameter2() throws Exception {
		testMaxRetainedParameterWithExpiration(4, 2, 3);
	}

	public void testMaxRetainedParameterWithExpiration(int totalCheckpoints, int retainedSnapshots, int lastSuccessfulCheckpointId) throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setInteger(CheckpointingOptions.MAX_RETAINED_REGION_SNAPSHOTS, retainedSnapshots);
		configuration.setBoolean(CheckpointingOptions.REGION_CHECKPOINT_ENABLED, true);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		env.getCheckpointConfig().setCheckpointInterval(CHECKPOINT_INTERVAL);
		env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_EXPIRE_PERIOD);
		env.setParallelism(4);

		env.addSource(new TestSource(totalCheckpoints)).addSink(new ExpireSink(totalCheckpoints));

		env.execute();

		Assert.assertEquals(lastSuccessfulCheckpointId, latestSuccessfulChckpointId.get());
	}

	private static class FailedSink extends RichSinkFunction<Integer> implements CheckpointedFunction {
		static AtomicBoolean exceptionThrown = new AtomicBoolean(false);
		static AtomicBoolean enterSnapshotProcess = new AtomicBoolean(false);
		static AtomicBoolean failoverFinished = new AtomicBoolean(false);

		FailedSink() {
			exceptionThrown.compareAndSet(true, false);
			enterSnapshotProcess.compareAndSet(true, false);
			failoverFinished.compareAndSet(false, true);
		}

		@Override
		public void open(Configuration parameters) throws Exception{
			if (getRuntimeContext().getIndexOfThisSubtask() == 0 && getRuntimeContext().getAttemptNumber() == 1) {
				failoverFinished.compareAndSet(false, true);
			}
		}

		@Override
		public void invoke(Integer value, Context context) throws Exception {
			if (latestSuccessfulChckpointId.get() >= COMPLETE_CHECKPOINTS_BEFORE_TESTING && getRuntimeContext().getIndexOfThisSubtask() == 0 && !exceptionThrown.get()) {
				while (!enterSnapshotProcess.get()) {
					Thread.sleep(100);
				}
				exceptionThrown.compareAndSet(false, true);
				throw new RuntimeException("Expected exception.");
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (latestSuccessfulChckpointId.get() >= COMPLETE_CHECKPOINTS_BEFORE_TESTING && getRuntimeContext().getIndexOfThisSubtask() == 1) {
				enterSnapshotProcess.compareAndSet(false, true);
				while (!exceptionThrown.get() || !failoverFinished.get()) {
					Thread.sleep(100);
				}
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {}
	}

	private static class ExpireSink extends RichSinkFunction<Integer> implements CheckpointListener, CheckpointedFunction {
		static AtomicBoolean expired = new AtomicBoolean(false);
		static AtomicLong notifications = new AtomicLong(0);

		int totalCheckpoints;

		ExpireSink(int totalCheckpoints) {
			this.totalCheckpoints = totalCheckpoints;
			expired.compareAndSet(true, false);
			notifications.set(0);
		}

		@Override
		public void invoke(Integer value, Context context) throws Exception {}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0 && checkpointId <= totalCheckpoints) {
				notifications.incrementAndGet();
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0
					&& latestSuccessfulChckpointId.get() >= COMPLETE_CHECKPOINTS_BEFORE_TESTING) {
				// let checkpoint expire
				LOG.info("Sleep to expire the checkpoint {}.", context.getCheckpointId());
				expired.compareAndSet(false, true);
				Thread.sleep(CHECKPOINT_EXPIRE_PERIOD * 2);
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {}
	}

	private static class TestSource extends RichParallelSourceFunction<Integer> implements CheckpointedFunction, CheckpointListener {

		boolean loop = true;
		int totalCheckpoints;

		TestSource(int totalCheckpoints) {
			this.totalCheckpoints = totalCheckpoints;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (loop && latestCheckpointId.get() <= totalCheckpoints) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(new Random().nextInt(100));
				}
				Thread.sleep(100);
			}
			LOG.info("Stop the source... latest checkpoint id={}, latest successful checkpoint id={}",
					latestCheckpointId, latestSuccessfulChckpointId.get());
		}

		@Override
		public void cancel() {
			loop = false;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (checkpointId <= totalCheckpoints && latestSuccessfulChckpointId.get() < checkpointId) {
				latestSuccessfulChckpointId.set(checkpointId);
				LOG.info("New checkpoint {} is completed.", checkpointId);
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (latestCheckpointId.get() < context.getCheckpointId()) {
				latestCheckpointId.set(context.getCheckpointId());
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {}
	}
}
