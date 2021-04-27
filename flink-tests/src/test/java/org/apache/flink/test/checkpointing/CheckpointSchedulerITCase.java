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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.checkpointstrategy.CheckpointSchedulingStrategies;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for checkpoint scheduler.
 */
public class CheckpointSchedulerITCase extends TestLogger implements Serializable {

	private static final long offsetMillis = 20 * 1000L; // 20 sec is ms
	private static final long checkpointInterval = 5 * 1000L; // 5 sec in ms
	private static final long checkpointTimeout = 1_000L; // 1 sec in ms

	// We expect one successful early checkpoint and two regular checkpoints to succeed
	RichSinkFunction<String> assertSink = new AssertSink(3);

	/**
	 * Expected behavior: early checkpoint succeeds and then regular checkpoint succeeds twice.
	 *
	 * @throws Exception Flink job exception
	 */
	@Test
	public void testHourlyCheckpoint() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// Configure checkpoint
		env.enableCheckpointing(checkpointInterval);
		env.getCheckpointConfig().setCheckpointSchedulingStrategy(
			CheckpointSchedulingStrategies.hourlyStrategy(offsetMillis));
		env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

		// No failure, runs 2 checkpointInterval time
		DataStream<String> inputStream = env.addSource(new SourceFunction<String>() {
			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				TimeUnit.MILLISECONDS.sleep(checkpointInterval * 2);
			}

			@Override
			public void cancel() {
			}
		}).setParallelism(1);

		inputStream
			.rebalance()
			.map((MapFunction<String, String>) value -> value)
			.addSink(assertSink)
			.setParallelism(1);

		env.execute();
	}

	/**
	 * Expected behavior: early checkpoint fails and then regular checkpoint succeeds twice.
	 *
	 * @throws Exception Flink job exception
	 */
	@Test
	public void testHourlyCheckpointWithFailedEarlyCheckpoint() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// Configure checkpoints
		env.enableCheckpointing(checkpointInterval);
		env.getCheckpointConfig().setCheckpointSchedulingStrategy(
			CheckpointSchedulingStrategies.hourlyStrategy(offsetMillis));
		env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

		// Configure restart strategies
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			1, // number of restart attempts
			Time.of(checkpointInterval + 1000L, TimeUnit.MILLISECONDS) // delay enough to fail early checkpoint
		));
		// set region fall over so the checkpoint scheduler will not be reset
		env.getConfiguration().setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");

		// We expect only two regular checkpoints to succeed
		RichSinkFunction<String> assertSink = new AssertSink(2);

		// No failure, runs 2 checkpointInterval time
		DataStream<String> inputStream = env.addSource(new RichSourceFunction<String>() {
			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				if (getRuntimeContext().getAttemptNumber() == 0) {
					throw new Exception("Source is intentionally failed for testing");
				}
				// For normal execution
				TimeUnit.MILLISECONDS.sleep(checkpointInterval * 2);
			}

			@Override
			public void cancel() {
			}
		}).setParallelism(1);

		inputStream
			.rebalance()
			.map(new RichMapFunction<String, String>() {
				@Override
				public String map(String value) {
					return value;
				}
			})
			.addSink(assertSink)
			.setParallelism(1);

		env.execute();
	}

	/**
	 * The sink asserted that checkpoint succeeded at least once before the job exits.
	 */
	private static class AssertSink extends RichSinkFunction<String> implements CheckpointListener {

		/**
		 * Expected last checkpoint ID. Always equals number of the checkpoint which is successfully triggered.
		 */
		private final long expectedLastCheckpointID;

		/**
		 * The actual last checkpoint ID.
		 */
		private long lastCheckpointID = -1;

		public AssertSink(long expectedLastCheckpointID) {
			this.expectedLastCheckpointID = expectedLastCheckpointID;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			this.lastCheckpointID = checkpointId;
		}

		@Override
		public void close() {
		}
	}
}
