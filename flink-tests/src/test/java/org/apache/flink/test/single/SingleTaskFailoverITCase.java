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

package org.apache.flink.test.single;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for Single Task Failover feature.
 */
public class SingleTaskFailoverITCase {

	private static boolean stop;

	@Before
	public void setUp() {
		stop = false;
	}

	@Test
	public void testSingleTaskFailoverOnSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setBoolean("taskmanager.network.partition.force-partition-recoverable", true);
		env.setParallelism(4);

		DataStream<String> source1 = env.addSource(new RichParallelSourceFunction<String>() {

			int count = 0;
			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				while (!stop) {
					ctx.collect(String.valueOf(new java.util.Random().nextInt(Integer.MAX_VALUE)));
					Thread.sleep(100);
					count++;
					if (count > 5 && getRuntimeContext().getAttemptNumber() == 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
						throw new RuntimeException("Expected Exception");
					}
					if (getRuntimeContext().getAttemptNumber() == 1) {
						stop = true;
					}
				}

			}

			@Override
			public void cancel() {}
		}).rebalance().map(new TestMap());

		env.execute("Single Task Failover Map Job");
	}

	@Test
	public void testSingleTaskFailoverOnJoin() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setBoolean("taskmanager.network.partition.force-partition-recoverable", true);
		env.setParallelism(4);

		DataStream<String> source1 = env.addSource(new TestSource());
		DataStream<String> source2 = env.addSource(new TestSource());

		source1.keyBy(new TestKeySelector()).connect(source2.keyBy(new TestKeySelector())).map(new RichCoMapFunction<String, String, String>() {

			int count = 0;

			@Override
			public String map1(String value) throws Exception {
				if (getRuntimeContext().getAttemptNumber() == 1) {
					stop = true;
				}

				if (count > 5 && getRuntimeContext().getIndexOfThisSubtask() == 2 && getRuntimeContext().getAttemptNumber() == 0) {
					throw new RuntimeException("Expected Exception");
				}

				count++;
				return value + "----1";
			}

			@Override
			public String map2(String value) throws Exception {
				return value + "----2";
			}
		}).addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value) throws Exception {}
		});

		env.execute("Single Task Failover Join Job");
	}

	private static class TestKeySelector implements KeySelector<String, Integer> {

		@Override
		public Integer getKey(String value) throws Exception {
			return value.hashCode();
		}
	}

	private static class TestSource extends RichParallelSourceFunction<String> {
		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (!stop) {
				if (getRuntimeContext().getAttemptNumber() > 0) {
					throw new IllegalStateException("Test Fail !");
				}
				ctx.collect(String.valueOf(new java.util.Random().nextInt(Integer.MAX_VALUE)));
				Thread.sleep(100);
			}
		}

		@Override
		public void cancel() {}
	}

	private static class TestMap extends RichMapFunction<String, String> {

		@Override
		public String map(String value) throws Exception {
			if (getRuntimeContext().getAttemptNumber() > 0) {
				throw new IllegalStateException("Test Fail !");
			}
			return value;
		}
	}
}
