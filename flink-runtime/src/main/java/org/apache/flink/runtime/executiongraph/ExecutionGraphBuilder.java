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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.checkpointstrategy.CheckpointSchedulingStrategies;
import org.apache.flink.api.common.checkpointstrategy.CheckpointTriggerStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.event.AbstractEventRecorder;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporter;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DiscardHistoricalCheckpointConfiguration;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.handler.CheckpointHandler;
import org.apache.flink.runtime.checkpoint.handler.GlobalCheckpointHandler;
import org.apache.flink.runtime.checkpoint.handler.RegionCheckpointHandler;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.checkpoint.trigger.CheckpointTriggerConfiguration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.ExecutionFailNumGauge;
import org.apache.flink.runtime.executiongraph.metrics.ExecutionStatusGauge;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.speculation.NoOpSpeculationStrategy;
import org.apache.flink.runtime.executiongraph.speculation.SpeculationStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleServiceOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.FORCE_PARTITION_RECOVERABLE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link ExecutionGraph} from a {@link JobGraph}.
 */
public class ExecutionGraphBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphBuilder.class);

	/**
	 * Builds the ExecutionGraph from the JobGraph.
	 * If a prior execution graph exists, the JobGraph will be attached. If no prior execution
	 * graph exists, then the JobGraph will become attach to a new empty execution graph.
	 */
	@VisibleForTesting
	public static ExecutionGraph buildGraph(
			@Nullable ExecutionGraph prior,
			JobGraph jobGraph,
			Configuration jobManagerConfig,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			SlotProvider slotProvider,
			ClassLoader classLoader,
			CheckpointRecoveryFactory recoveryFactory,
			Time rpcTimeout,
			RestartStrategy restartStrategy,
			MetricGroup metrics,
			BlobWriter blobWriter,
			Time allocationTimeout,
			Logger log,
			ShuffleMaster<?> shuffleMaster,
			JobMasterPartitionTracker partitionTracker) throws JobExecutionException, JobException {

		final FailoverStrategy.Factory failoverStrategy =
			FailoverStrategyLoader.loadFailoverStrategy(jobManagerConfig, log);

		return buildGraph(
			prior,
			jobGraph,
			jobManagerConfig,
			futureExecutor,
			ioExecutor,
			slotProvider,
			classLoader,
			recoveryFactory,
			rpcTimeout,
			restartStrategy,
			metrics,
			blobWriter,
			allocationTimeout,
			log,
			shuffleMaster,
			partitionTracker,
			failoverStrategy,
			new NoOpSpeculationStrategy(),
			BlacklistUtil.createNoOpRemoteBlacklistReporter(),
			null);
	}

	public static ExecutionGraph buildGraph(
		@Nullable ExecutionGraph prior,
		JobGraph jobGraph,
		Configuration jobManagerConfig,
		ScheduledExecutorService futureExecutor,
		Executor ioExecutor,
		SlotProvider slotProvider,
		ClassLoader classLoader,
		CheckpointRecoveryFactory recoveryFactory,
		Time rpcTimeout,
		RestartStrategy restartStrategy,
		MetricGroup metrics,
		BlobWriter blobWriter,
		Time allocationTimeout,
		Logger log,
		ShuffleMaster<?> shuffleMaster,
		JobMasterPartitionTracker partitionTracker,
		FailoverStrategy.Factory failoverStrategyFactory,
		SpeculationStrategy speculationStrategy,
		final RemoteBlacklistReporter remoteBlacklistReporter,
		final AbstractEventRecorder abstractEventRecorder) throws JobExecutionException, JobException {

		if (abstractEventRecorder != null) {
			abstractEventRecorder.buildExecutionGraphStart();
		}

		checkNotNull(jobGraph, "job graph cannot be null");
		final String jobName = jobGraph.getName();
		final String jobUID = jobGraph.getJobUID() == null ? jobName : jobGraph.getJobUID();
		final JobID jobId = jobGraph.getJobID();

		final JobInformation jobInformation = new JobInformation(
			jobUID,
			jobId,
			jobName,
			jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			jobGraph.getUserJarBlobKeys(),
			jobGraph.getClasspaths());

		final int maxPriorAttemptsHistoryLength =
				jobManagerConfig.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

		final boolean isRecoverable = jobManagerConfig.getBoolean(FORCE_PARTITION_RECOVERABLE);
		final boolean cloudShuffleMode = jobManagerConfig.getBoolean(ShuffleServiceOptions.SHUFFLE_CLOUD_SHUFFLE_MODE);
		final boolean jobLogDetailDisable = jobManagerConfig.getBoolean(CoreOptions.FLINK_JOB_LOG_DETAIL_DISABLE);

		final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory =
			PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(jobManagerConfig);

		// create a new execution graph, if none exists so far
		final ExecutionGraph executionGraph;
		try {
			executionGraph = (prior != null) ? prior :
				new ExecutionGraph(
					jobInformation,
					futureExecutor,
					ioExecutor,
					rpcTimeout,
					restartStrategy,
					maxPriorAttemptsHistoryLength,
					failoverStrategyFactory,
					slotProvider,
					classLoader,
					blobWriter,
					allocationTimeout,
					partitionReleaseStrategyFactory,
					shuffleMaster,
					partitionTracker,
					jobGraph.getScheduleMode(),
					speculationStrategy,
					remoteBlacklistReporter,
					new DefaultLogicalTopology(jobGraph),
					isRecoverable,
					cloudShuffleMode,
					jobLogDetailDisable);
					executionGraph.setExecutionStatusDuration(jobManagerConfig.getInteger(JobManagerOptions.EXECUTION_STATUS_DURATION_MS));
		} catch (IOException e) {
			throw new JobException("Could not create the ExecutionGraph.", e);
		}

		// set the basic properties

		try {
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
		}
		catch (Throwable t) {
			log.warn("Cannot create JSON plan for job", t);
			// give the graph an empty plan
			executionGraph.setJsonPlan("{}");
		}

		// initialize the vertices that have a master initialization hook
		// file output formats create directories here, input formats create splits

		final long initMasterStart = System.nanoTime();
		log.info("Running initialization on master for job {} ({}).", jobName, jobId);
		if (abstractEventRecorder != null) {
			abstractEventRecorder.buildExecutionGraphInitialization();
		}

		for (JobVertex vertex : jobGraph.getVertices()) {
			String executableClass = vertex.getInvokableClassName();
			if (executableClass == null || executableClass.isEmpty()) {
				throw new JobSubmissionException(jobId,
						"The vertex " + vertex.getID() + " (" + vertex.getName() + ") has no invokable class.");
			}

			try {
				vertex.initializeOnMaster(classLoader);
			}
			catch (Throwable t) {
					throw new JobExecutionException(jobId,
							"Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(), t);
			}
		}

		log.info("Successfully ran initialization on master in {} ms.",
				(System.nanoTime() - initMasterStart) / 1_000_000);

		// topologically sort the job vertices and attach the graph to the existing one
		List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
		if (log.isDebugEnabled()) {
			log.debug("Adding {} vertices from job graph {} ({}).", sortedTopology.size(), jobName, jobId);
		}
		executionGraph.attachJobGraph(sortedTopology, abstractEventRecorder);

		if (log.isDebugEnabled()) {
			log.debug("Successfully created execution graph from job graph {} ({}).", jobName, jobId);
		}

		// configure the state checkpointing
		JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();
		boolean checkpointForceClose = jobManagerConfig.getBoolean(JobManagerOptions.JOBMANAGER_CHECKPOINT_FORCE_CLOSE);
		if (!checkpointForceClose && snapshotSettings != null) {
			List<ExecutionJobVertex> triggerVertices =
					idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

			List<ExecutionJobVertex> ackVertices =
					idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

			List<ExecutionJobVertex> confirmVertices =
					idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);

			// configure region checkpoint
			final CheckpointHandler checkpointHandler;
			final boolean regionCheckpointEnabled = jobManagerConfig.getBoolean(CheckpointingOptions.REGION_CHECKPOINT_ENABLED);
			if (regionCheckpointEnabled) {
				final int maxNumberOfSnapshotsToRetain = jobManagerConfig.getInteger(
						CheckpointingOptions.MAX_RETAINED_REGION_SNAPSHOTS);
				final double maxPercentageOfRecovery = jobManagerConfig.getDouble(
						CheckpointingOptions.MAX_PERCENTAGE_RECOVERY);
				final boolean forceSingleTaskAsRegion = jobManagerConfig.getBoolean(
						CheckpointingOptions.FORCE_SINGLE_TASK_AS_REGION);
				checkpointHandler = new RegionCheckpointHandler(
						metrics,
						confirmVertices.stream().flatMap(jv -> Arrays.stream(jv.getTaskVertices())).toArray(ExecutionVertex[]::new),
						maxNumberOfSnapshotsToRetain,
						maxPercentageOfRecovery,
						forceSingleTaskAsRegion);
			} else {
				checkpointHandler = new GlobalCheckpointHandler();
			}

			CompletedCheckpointStore completedCheckpoints;
			CheckpointIDCounter checkpointIdCounter;
			try {
				int maxNumberOfCheckpointsToRetain = jobManagerConfig.getInteger(
						CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

				if (maxNumberOfCheckpointsToRetain <= 0) {
					// warning and use 1 as the default value if the setting in
					// state.checkpoints.max-retained-checkpoints is not greater than 0.
					log.warn("The setting for '{} : {}' is invalid. Using default value of {}",
							CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
							maxNumberOfCheckpointsToRetain,
							CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

					maxNumberOfCheckpointsToRetain = CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
				}

				if (regionCheckpointEnabled) {
					// override max retained checkpoints based on the region checkpoints
					maxNumberOfCheckpointsToRetain += (jobManagerConfig.getInteger(
							CheckpointingOptions.MAX_RETAINED_REGION_SNAPSHOTS) + 1);
					LOG.info("Override {} to {} according to {}={}.",
							CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
							maxNumberOfCheckpointsToRetain,
							CheckpointingOptions.MAX_RETAINED_REGION_SNAPSHOTS.key(),
							jobManagerConfig.getInteger(CheckpointingOptions.MAX_RETAINED_REGION_SNAPSHOTS));
				}

				completedCheckpoints = recoveryFactory.createCheckpointStore(jobId, jobUID, maxNumberOfCheckpointsToRetain, classLoader);
				checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId, jobUID);
			}
			catch (Exception e) {
				throw new JobExecutionException(jobId, "Failed to initialize high-availability checkpoint handler", e);
			}

			// Maximum number of remembered checkpoints
			int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);

			CheckpointStatsTracker checkpointStatsTracker = new CheckpointStatsTracker(
					historySize,
					ackVertices,
					snapshotSettings.getCheckpointCoordinatorConfiguration(),
					metrics);

			// load the state backend from the application settings
			final StateBackend applicationConfiguredBackend;
			final SerializedValue<StateBackend> serializedAppConfigured = snapshotSettings.getDefaultStateBackend();

			if (serializedAppConfigured == null) {
				applicationConfiguredBackend = null;
			}
			else {
				try {
					applicationConfiguredBackend = serializedAppConfigured.deserializeValue(classLoader);
				} catch (IOException | ClassNotFoundException e) {
					throw new JobExecutionException(jobId,
							"Could not deserialize application-defined state backend.", e);
				}
			}

			final StateBackend rootBackend;
			try {
				rootBackend = StateBackendLoader.fromApplicationOrConfigOrDefault(
						applicationConfiguredBackend, jobManagerConfig, classLoader, log);
			}
			catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
				throw new JobExecutionException(jobId, "Could not instantiate configured state backend", e);
			}

			// instantiate the user-defined checkpoint hooks

			final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks = snapshotSettings.getMasterHooks();
			final List<MasterTriggerRestoreHook<?>> hooks;

			if (serializedHooks == null) {
				hooks = Collections.emptyList();
			}
			else {
				final MasterTriggerRestoreHook.Factory[] hookFactories;
				try {
					hookFactories = serializedHooks.deserializeValue(classLoader);
				}
				catch (IOException | ClassNotFoundException e) {
					throw new JobExecutionException(jobId, "Could not instantiate user-defined checkpoint hooks", e);
				}

				final Thread thread = Thread.currentThread();
				final ClassLoader originalClassLoader = thread.getContextClassLoader();
				thread.setContextClassLoader(classLoader);

				try {
					hooks = new ArrayList<>(hookFactories.length);
					for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
						hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
					}
				}
				finally {
					thread.setContextClassLoader(originalClassLoader);
				}
			}

			// Override configuration if CLI specified otherwise.
			final String checkpointSchedulingStrategy = jobManagerConfig.getString(CheckpointingOptions.CHECKPOINT_SCHEDULING_STRATEGY);
			final String savepointSchedulingStrategy = jobManagerConfig.getString(CheckpointingOptions.SAVEPOINT_SCHEDULING_STRATEGY);
			CheckpointCoordinatorConfiguration chkConfig;

			if (checkpointSchedulingStrategy == null) {
				chkConfig = snapshotSettings.getCheckpointCoordinatorConfiguration();
			} else {
				log.info("Checkpoint-scheduler-related options found. CLI configuration will take priority.");
				final CheckpointSchedulingStrategies.CheckpointSchedulerConfiguration overrideSchedulingConfig;
				overrideSchedulingConfig = CheckpointSchedulingStrategies.resolveCliConfig(checkpointSchedulingStrategy, jobManagerConfig);

				final CheckpointCoordinatorConfiguration origin = snapshotSettings.getCheckpointCoordinatorConfiguration();

				chkConfig = new CheckpointCoordinatorConfiguration(
					origin.getCheckpointInterval(),
					origin.getCheckpointTimeout(),
					origin.getMinPauseBetweenCheckpoints(),
					origin.getMaxConcurrentCheckpoints(),
					origin.getCheckpointRetentionPolicy(),
					origin.isExactlyOnce(),
					origin.isPreferCheckpointForRecovery(),
					overrideSchedulingConfig,
					origin.getTolerableCheckpointFailureNumber(),
					origin.isUnalignedCheckpointsEnabled());
			}

			if (savepointSchedulingStrategy != null) {
				log.info("Savepoint-scheduler-related options found. CLI configuration will take priority.");
				final CheckpointSchedulingStrategies.SavepointSchedulerConfiguration overrideSavepointSchedulingConfig;
				overrideSavepointSchedulingConfig = CheckpointSchedulingStrategies.resolveSavepointCliConfig(savepointSchedulingStrategy, jobManagerConfig);

				final CheckpointCoordinatorConfiguration origin = chkConfig;

				chkConfig = new CheckpointCoordinatorConfiguration(
					origin.getCheckpointInterval(),
					origin.getCheckpointTimeout(),
					origin.getMinPauseBetweenCheckpoints(),
					origin.getMaxConcurrentCheckpoints(),
					origin.getCheckpointRetentionPolicy(),
					origin.isExactlyOnce(),
					origin.isPreferCheckpointForRecovery(),
					origin.getCheckpointSchedulerConfiguration(),
					overrideSavepointSchedulingConfig,
					origin.getTolerableCheckpointFailureNumber(),
					origin.isUnalignedCheckpointsEnabled());
			}

			String savepointLocationPrefix = jobManagerConfig.getString(CheckpointingOptions.SAVEPOINT_LOCATION_PREFIX);
			chkConfig.setSavepointLocationPrefix(savepointLocationPrefix);

			final boolean aggregateUnionState = jobManagerConfig.getBoolean(CheckpointingOptions.UNION_STATE_AGGREGATION_ENABLED);
			chkConfig.setAggregateUnionState(aggregateUnionState);

			int hdfsMaxRetryAttempts = jobManagerConfig.getInteger(CheckpointingOptions.DATA_TRANSFER_MAX_RETRY_ATTEMPTS);
			chkConfig.setTransferMaxRetryAttempts(hdfsMaxRetryAttempts);

			final boolean allowPersistStateMeta = jobManagerConfig.getBoolean(CheckpointingOptions.ALLOW_PERSIST_STATE_META);
			chkConfig.setAllowPersistStateMeta(allowPersistStateMeta);

			final CheckpointTriggerStrategy triggerStrategy = jobManagerConfig.getEnum(CheckpointTriggerStrategy.class, CheckpointingOptions.CHECKPOINT_TRIGGER_STRATEGY);
			CheckpointTriggerConfiguration triggerConfiguration = new CheckpointTriggerConfiguration(triggerStrategy, sortedTopology);
			chkConfig.setCheckpointTriggerConfiguration(triggerConfiguration);

			int numDiscardHistoricalOnce = jobManagerConfig.getInteger(CheckpointingOptions.NUM_DISCARD_HISTORICAL);
			long fixedDelayTimeDiscardHistorical = jobManagerConfig.getLong(CheckpointingOptions.FIXED_DELAY_TIME_DESCARD_HISTORICAL);
			String expiredCheckpointDirectoryPrefix = jobManagerConfig.getString(CheckpointingOptions.EXPIRED_CHECKPOINT_DIR);
			DiscardHistoricalCheckpointConfiguration discardHistoricalCheckpointConfiguration = new DiscardHistoricalCheckpointConfiguration(
				numDiscardHistoricalOnce,
				fixedDelayTimeDiscardHistorical,
				expiredCheckpointDirectoryPrefix
			);
			chkConfig.setDiscardHistoricalCheckpointConfiguration(discardHistoricalCheckpointConfiguration);

			String namespace = jobManagerConfig.getString(CheckpointingOptions.CHECKPOINTS_NAMESPACE);

			executionGraph.enableCheckpointing(
				namespace,
				chkConfig,
				triggerVertices,
				ackVertices,
				confirmVertices,
				hooks,
				checkpointIdCounter,
				completedCheckpoints,
				rootBackend,
				checkpointStatsTracker,
				checkpointHandler,
				metrics);
		}

		// create all the metrics for the Execution Graph

		metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
		metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
		metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));

		// register failover gauge metrics and rate metrics
		metrics.gauge(ExecutionFailNumGauge.METRIC_NAME, new ExecutionFailNumGauge(executionGraph));
		metrics.meter(ExecutionFailNumGauge.RATE_METRIC_NAME, new MeterView(executionGraph.getNumberOfExecutionFailCounter(), 60));
		metrics.gauge(ExecutionStatusGauge.METRIC_NAME, new ExecutionStatusGauge(executionGraph));

		executionGraph.getFailoverStrategy().registerMetrics(metrics);
		executionGraph.getSpeculationStrategy().registerMetrics(metrics);

		log.info("Successfully ran buildExecutionGraph on master in {} ms.",
				(System.nanoTime() - initMasterStart) / 1_000_000);

		if (abstractEventRecorder != null) {
			abstractEventRecorder.buildExecutionGraphFinish();
		}
		return executionGraph;
	}

	private static List<ExecutionJobVertex> idToVertex(
			List<JobVertexID> jobVertices, ExecutionGraph executionGraph) throws IllegalArgumentException {

		List<ExecutionJobVertex> result = new ArrayList<>(jobVertices.size());

		for (JobVertexID id : jobVertices) {
			ExecutionJobVertex vertex = executionGraph.getJobVertex(id);
			if (vertex != null) {
				result.add(vertex);
			} else {
				throw new IllegalArgumentException(
						"The snapshot checkpointing settings refer to non-existent vertex " + id);
			}
		}

		return result;
	}

	// ------------------------------------------------------------------------

	/** This class is not supposed to be instantiated. */
	private ExecutionGraphBuilder() {}
}
