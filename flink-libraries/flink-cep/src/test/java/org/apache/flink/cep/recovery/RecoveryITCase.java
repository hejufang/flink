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

package org.apache.flink.cep.recovery;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.pattern.parser.TestCepEventParser;
import org.apache.flink.cep.test.TestData;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Test savepoint rescaling.
 */
@RunWith(Parameterized.class)
public class RecoveryITCase extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(RecoveryITCase.class);

	private static final int numTaskManagers = 2;
	private static final int slotsPerTaskManager = 2;
	private static final int numSlots = numTaskManagers * slotsPerTaskManager;

	@Parameterized.Parameters(name = "backend = {0}")
	public static Object[] data() {
		return new Object[]{"filesystem", "rocksdb"};
	}

	@Parameterized.Parameter
	public String backend;

	private String currentBackend = null;

	enum OperatorCheckpointMethod {
		NON_PARTITIONED, CHECKPOINTED_FUNCTION, CHECKPOINTED_FUNCTION_BROADCAST, LIST_CHECKPOINTED
	}

	private static MiniClusterWithClientResource cluster;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setup() throws Exception {
		// detect parameter change
		if (currentBackend != backend) {
			shutDownExistingCluster();

			currentBackend = backend;

			Configuration config = new Configuration();

			final File checkpointDir = temporaryFolder.newFolder();
			final File savepointDir = temporaryFolder.newFolder();

			config.setString(CheckpointingOptions.STATE_BACKEND, currentBackend);
			config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
			config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

			cluster = new MiniClusterWithClientResource(
					new MiniClusterResourceConfiguration.Builder()
							.setConfiguration(config)
							.setNumberTaskManagers(numTaskManagers)
							.setNumberSlotsPerTaskManager(numSlots)
							.build());
			cluster.before();
		}
	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}
	}

	@Test
	public void testSavepointRescalingInKeyedState() throws Exception {
		testSavepointRescalingKeyedState();
	}


	/**
	 * Tests that a job with purely keyed state can be restarted from a savepoint
	 * with a different parallelism.
	 */
	public void testSavepointRescalingKeyedState() throws Exception {

		Duration timeout = Duration.ofMinutes(3);
		Deadline deadline = Deadline.now().plus(timeout);

		ClusterClient<?> client = cluster.getClusterClient();

		try {
			JobGraph jobGraph = createJobGraph(true);

			final JobID jobID = jobGraph.getJobID();

			ClientUtils.submitJob(client, jobGraph);

			TestSink.workCompletedLatch.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			CompletableFuture<String> savepointPathFuture = client.triggerSavepoint(jobID, null);

			final String savepointPath = savepointPathFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			client.cancel(jobID);

			while (!getRunningJobs(client).isEmpty()) {
				Thread.sleep(50);
			}
			JobGraph scaledJobGraph = createJobGraph(false);

			scaledJobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

			ClientUtils.submitJob(client, scaledJobGraph);

		} finally {
		}
	}

	JobGraph createJobGraph(boolean firstGraph) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(60000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setBufferTimeout(10);

		TestSink.workCompletedLatch = new CountDownLatch(1);
		DataStream<String> patternJsonStream = env.addSource(new PatternJsonStream(firstGraph));
		DataStream<Event> input = env.addSource(new EventStream(firstGraph))
				.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

					@Override
					public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
						return element.f1;
					}

					@Override
					public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
						return new Watermark(lastElement.f1 - 5);
					}

				})
				.map((MapFunction<Tuple2<Event, Long>, Event>) value -> value.f0)
				.keyBy((KeySelector<Event, Integer>) Event::getId);

		DataStream<String> result = CEP.pattern(input, patternJsonStream, TestCepEventParser::new).process(new MultiplePatternProcessFunction<Event, String>() {
			@Override
			public void processMatch(Tuple2<String, Map<String, List<Event>>> match, Context ctx, Object key, Collector<String> out) throws Exception {
				out.collect(match.f0 + "," + key);
			}
		});

		result.addSink(new TestSink());

		return env.getStreamGraph().getJobGraph();
	}

	private static class PatternJsonStream implements SourceFunction<String> {

		boolean firstGraph;
		PatternJsonStream(boolean firstGraph) {
			this.firstGraph = firstGraph;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			if (firstGraph) {
				ctx.collect(TestData.COUNT_PATTERN_1);
				while (true) {
					// sleep until client cancels this job
					Thread.sleep(100);
				}
			} else {
				// output an empty string
				ctx.collect("");
			}
			LOG.info("PatternStream exits.");
		}

		@Override
		public void cancel() {}
	}

	static class EventStream implements SourceFunction<Tuple2<Event, Long>> {

		boolean firstGraph;

		EventStream(boolean firstGraph) {
			this.firstGraph = firstGraph;
		}

		@Override
		public void run(SourceContext<Tuple2<Event, Long>> ctx) throws Exception {
			if (firstGraph) {
				while (true) {
					ctx.collect(Tuple2.of(new Event(1, "buy", 1.0), System.currentTimeMillis() - 5));
					Thread.sleep(100);
				}
			} else {
				ctx.collect(Tuple2.of(new Event(1, "buy", 1.0), System.currentTimeMillis() - 5));
			}
			LOG.info("EventStream exists.");
		}

		@Override
		public void cancel() {}
	}

	static class TestSink implements SinkFunction<String> {
		static volatile CountDownLatch workCompletedLatch = new CountDownLatch(1);

		@Override
		public void invoke(String value, Context context) throws Exception {
			LOG.info("Sink receives data, the job is running now.");
			workCompletedLatch.countDown();
		}
	}

	private static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
		Collection<JobStatusMessage> statusMessages = client.listJobs().get();
		return statusMessages.stream()
				.filter(status -> !status.getJobState().isGloballyTerminalState())
				.map(JobStatusMessage::getJobId)
				.collect(Collectors.toList());
	}
}
