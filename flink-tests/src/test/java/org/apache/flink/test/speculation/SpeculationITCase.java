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

package org.apache.flink.test.speculation;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.speculation.SpeculationOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ITCase for speculation.
 */
public class SpeculationITCase extends TestLogger implements Serializable {


	transient Configuration config;

	@Before
	public void setup() {
		config = new Configuration();
		config.setBoolean(SpeculationOptions.SPECULATION_ENABLED, true);
		config.setLong(SpeculationOptions.SPECULATION_INTERVAL, 5 * 1000L);
	}

	@Test
	public void testNormalSpeculationMainExecutionSucceed() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(config);
		env.getConfig().setExecutionMode(ExecutionMode.BATCH);
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.setParallelism(10);

		final DataSet<String> source = env.fromCollection(generateData()).setParallelism(1);
		final DataSet<String> rebalance = source.rebalance().setParallelism(10);

		rebalance.map(new RichMapFunction<String, String>() {

			boolean stop = true;

			@Override
			public String map(String value) throws Exception {
				final int index = getRuntimeContext().getIndexOfThisSubtask();
				final int attempt = getRuntimeContext().getAttemptNumber();

				// let subtask-3-attempt-0 sleep for a while to make sure copy execution is scheduled
				if (index == 3 && attempt == 0 && stop) {
					Thread.sleep(60000);
					stop = false;
				}

				// let subtask-3-attempt-1 sleep forever
				if (index == 3 && attempt == 1) {
					Thread.sleep(10000000);
				}
				return value;
			}
		}).output(new NoOpOutputFormat());

		env.execute();
	}

	@Test
	public void testNormalSpeculationCopyExecutionSucceed() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(config);
		env.getConfig().setExecutionMode(ExecutionMode.BATCH);
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.setParallelism(10);

		final DataSet<String> source = env.fromCollection(generateData()).setParallelism(1);
		final DataSet<String> rebalance = source.rebalance().setParallelism(10);

		rebalance.map(new RichMapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				final int index = getRuntimeContext().getIndexOfThisSubtask();
				final int attempt = getRuntimeContext().getAttemptNumber();

				// let subtask-3-attempt-0 sleep forever
				if (index == 3 && attempt == 0) {
					Thread.sleep(1000000);
				}
				return value;
			}
		}).output(new NoOpOutputFormat());

		env.execute();
	}

	@Test
	public void testFailureSpeculation() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(config);
		env.getConfig().setExecutionMode(ExecutionMode.BATCH);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.seconds(15)));
		env.setParallelism(10);

		final DataSet<String> source = env.fromCollection(generateData()).setParallelism(1);
		final DataSet<String> rebalance = source.rebalance().setParallelism(10);

		rebalance.map(new RichMapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				final int index = getRuntimeContext().getIndexOfThisSubtask();
				final int attempt = getRuntimeContext().getAttemptNumber();

				// let subtask-3-attempt-0 sleep forever
				if (index == 3 && attempt == 0) {
					Thread.sleep(10000000);
				}

				if (index == 3 && attempt == 1) {
					throw new RuntimeException("Shut down");
				}
				return value;
			}
		}).output(new NoOpOutputFormat());

		env.execute()
		;
	}

	private class NoOpOutputFormat extends RichOutputFormat<String> {

		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
		}

		@Override
		public void writeRecord(String record) throws IOException {
		}

		@Override
		public void close() throws IOException {
		}
	}

	private List<String> generateData() {
		final List<String> list = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			list.add(String.valueOf(i));
		}
		return list;
	}
}
