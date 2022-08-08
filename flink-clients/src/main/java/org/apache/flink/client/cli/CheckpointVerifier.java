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

package org.apache.flink.client.cli;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.StateMetaCompatibility;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.CheckpointingOptions.ALLOW_NON_RESTORED_STATE;
import static org.apache.flink.configuration.CheckpointingOptions.CLIENT_CHECKPOINT_VERIFICATION_ENABLE;
import static org.apache.flink.configuration.CheckpointingOptions.MAX_RETAINED_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.Checkpoints.loadCheckpointMetadata;
import static org.apache.flink.runtime.checkpoint.StateAssignmentOperation.containKeyedState;

/**
 * Checkpoint verifier at client-end.
 */
public class CheckpointVerifier {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointVerifier.class);
	private static final int MISS_OPERATOR_ID_EXIT_CODE = 2;
	private static final int MISMATCH_PARALLELISM_EXIT_CODE = 3;
	private static final int INVALID_SAVEPOINT_PATH_EXIT_CODE = 4;

	// exit code after checkpoint verification
	public static volatile int verifyExitCode = -1;

	private static List<BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult>> verifyStrategies;

	private static volatile Map<Long, Tuple2<String, Map<OperatorID, OperatorState>>> checkpointsOnStorage;

	static {
		verifyStrategies = new ArrayList<>();
		// Strategy 1: all OperatorID with state in OperatorState must exist in new JobGraph
		verifyStrategies.add((tasks, operatorStateAndMeta) -> {

			// get all OperatorIDs form jobVertex
			Set<OperatorID> allOperatorIDs = new HashSet<>();
			Map<OperatorID, OperatorStateMeta> allOperatorAndStateMetas = new HashMap<>();

			for (JobVertex jobVertex : tasks.values()) {

				Map<OperatorID, OperatorStateMeta> chainedOperatorIdAndStateMeta = jobVertex.getChainedOperatorIdAndStateMeta();
				List<OperatorIDPair> operatorIDPairs = jobVertex.getOperatorIDs();
				operatorIDPairs.stream().forEach(operatorIDPair -> {
					// because chainedOperatorIdAndStateMeta is generated with GeneratedOperatorID
					// so if set UserDefinedOperator, use it instead of GeneratedOperatorID
					operatorIDPair.getUserDefinedOperatorID().ifPresent(
						userDefinedOperatorID -> {
							OperatorStateMeta operatorStateMeta = chainedOperatorIdAndStateMeta.get(operatorIDPair.getGeneratedOperatorID());
							chainedOperatorIdAndStateMeta.put(userDefinedOperatorID, operatorStateMeta);
						}
					);
				});
				allOperatorAndStateMetas.putAll(chainedOperatorIdAndStateMeta);

				allOperatorIDs.addAll(jobVertex.getOperatorIDs().stream().map(operatorIDPair -> {
					return operatorIDPair.getUserDefinedOperatorID()
						.filter(operatorStateAndMeta::containsKey)
						.orElse(operatorIDPair.getGeneratedOperatorID());
				}).collect(Collectors.toList()));
			}

			for (Map.Entry<OperatorID, Tuple2<OperatorState, OperatorStateMeta>> operatorGroupStateEntry : operatorStateAndMeta.entrySet()) {
				OperatorState operatorState = operatorGroupStateEntry.getValue().f0;
				OperatorStateMeta operatorStateMeta = operatorGroupStateEntry.getValue().f1;
				//----------------------------------------find operator for state---------------------------------------------

				if (!allOperatorIDs.contains(operatorGroupStateEntry.getKey()) && !Checkpoints.isEmptyState(operatorState)) {
					final String message = "There is no operator for the state " + operatorState.getOperatorID() +
						" If you see this, usually it means that the job's topology is changed. And" +
						" the state in previous checkpoint cannot be used in current job !!! \n" +
						" You need to revert your changes or change state.checkpoints.namespace to start a new checkpoint.";
					logAndSyserr(message);
					return CheckpointVerifyResult.FAIL_MISS_OPERATOR_ID;
				} else {
					if (operatorStateMeta == null) {
						continue;
					}
					OperatorStateMeta stateMetaFromJobGraph = allOperatorAndStateMetas.get(operatorGroupStateEntry.getKey());
					StateMetaCompatibility compatibility = operatorStateMeta.resolveCompatibility(stateMetaFromJobGraph);
					if (compatibility.isIncompatible()) {
						final String message = "The state schema in " + operatorState.getOperatorID() +
						"is incompatible because " + compatibility.getMessage() + " If you see this, usually it means that the state schema is changed with incompatible. And" +
						" the state in previous checkpoint cannot be used in current job !!! \n" +
						" You need to revert your changes or change state.checkpoints.namespace to start a new checkpoint.";
						logAndSyserr(message);
						return CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE;
					}

				}
			}

			return CheckpointVerifyResult.SUCCESS;
		});

		// Strategy 2: JobVertex's parallelism should not exceed OperatorState's maxParallelism
		verifyStrategies.add((tasks, operatorStates) ->  {
			for (JobVertex jobVertex : tasks.values()) {
				int parallelism = jobVertex.getParallelism();

				List<OperatorIDPair> operatorIDs = jobVertex.getOperatorIDs();

				for (OperatorIDPair operatorIDPair : operatorIDs) {
					Tuple2<OperatorState, OperatorStateMeta> operatorStateAndMeta = operatorStates.get(operatorIDPair.getGeneratedOperatorID());
					if (operatorStateAndMeta != null) {
						OperatorState operatorState = operatorStateAndMeta.f0;
						if (operatorState.getMaxParallelism() < parallelism && containKeyedState(operatorState)) {
							final String message = "Operator " + operatorState.getOperatorID() + " has " + parallelism +
								" in new JobGraph, while the max parallelism in OperatorState is " + operatorState.getMaxParallelism() +
								", could not match the parallelism between new JobGraph and previous state.";
							logAndSyserr(message);
							return CheckpointVerifyResult.FAIL_MISMATCH_PARALLELISM;
						}
					}
				}
			}
			return CheckpointVerifyResult.SUCCESS;
		});
	}

	/**
	 * Check if there exists any completed checkpoints on HDFS in advance,
	 * if none, skip checkpoint verification at once.
	 *
	 * @param classLoader Used to load plugins in a plug-in fashion.
	 * @param configuration Configuration of client.
	 * @return true if HDFS has completed checkpoint; false if none.
	 */
	public static boolean beforeVerify(ClassLoader classLoader, Configuration configuration) {
		String jobUID = configuration.getString(PipelineOptions.JOB_UID);
		if (jobUID == null) {
			return false;
		}
		if (configuration.contains(CheckpointingOptions.RESTORE_SAVEPOINT_PATH)) {
			LOG.info("checkpoint.restore-savepoint-path is {}, starting checkpoint verification.", configuration.getString(CheckpointingOptions.RESTORE_SAVEPOINT_PATH));
			return true;
		}
		// use a fake JobID, just for checkpoint verification
		checkpointsOnStorage = findAllCompletedCheckpointsOnStorage(configuration, classLoader, new JobID(), jobUID);
		LOG.info("Pre-check checkpoints {} on HDFS, if zero: {}, exit immediately", checkpointsOnStorage.keySet(), checkpointsOnStorage.isEmpty());
		return !checkpointsOnStorage.isEmpty();
	}

	public static void verify(JobGraph jobGraph, ClassLoader classLoader, Configuration configuration) throws Exception {
		// -----------------------------------------------------------------
		// Check whether client checkpoint verification is enabled.
		// -----------------------------------------------------------------
		if (configuration.getBoolean(ALLOW_NON_RESTORED_STATE)) {
			LOG.info("ALLOW_NON_RESTORED_STATE is set true, skip checkpoint verification");
			return;
		}

		// If "client-checkpoint-verification-enable=false" is set, skip.
		if (!configuration.getBoolean(CLIENT_CHECKPOINT_VERIFICATION_ENABLE)) {
			LOG.info("CLIENT_CHECKPOINT_VERIFICATION_ENABLE is set false, skip checkpoint verification");
			return;
		}

		// If user disable checkpointing (i.e., checkpoint interval is -1), skip .
		JobCheckpointingSettings checkpointingSettings = jobGraph.getCheckpointingSettings();
		boolean checkpointForceClose = configuration.getBoolean(JobManagerOptions.JOBMANAGER_CHECKPOINT_FORCE_CLOSE);
		if (checkpointForceClose || checkpointingSettings == null) {
			LOG.info("Checkpointing is disabled, skip checkpoint verification");
			return;
		} else {
			long interval = Preconditions.checkNotNull(checkpointingSettings.getCheckpointCoordinatorConfiguration()).getCheckpointInterval();
			if (interval == -1) {
				// sanity check
				LOG.info("Checkpointing is disabled, skip checkpoint verification");
				return;
			}
		}

		// If user does not use ZooKeeper HA, skip.
		// TODO @chenzhanghao.1997: Support native K8s HA as well
		if (!HighAvailabilityMode.fromConfig(configuration).equals(HighAvailabilityMode.ZOOKEEPER)) {
			LOG.info("Only ZooKeeper HA is supported, skip checkpoint verification");
			return;
		}

		// -----------------------------------------------------------------
		// Prepare CompletedCheckpointStore and verify.
		// -----------------------------------------------------------------
		CompletedCheckpointStore completedCheckpointStore = null;
		CheckpointVerifyResult verifyResult = CheckpointVerifyResult.SUCCESS;

		try (HighAvailabilityServices haService = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION)) {

			// construct CompletedCheckpointStore (ZooKeeper)
			try {
				completedCheckpointStore = haService.getCheckpointRecoveryFactory().createCheckpointStore(
					jobGraph.getJobID(), jobGraph.getJobUID(), configuration.getInteger(MAX_RETAINED_CHECKPOINTS), ClassLoader.getSystemClassLoader());

				verifyResult = CheckpointVerifier.doVerify(jobGraph, classLoader, completedCheckpointStore, configuration);
			} catch (Exception e) {
				LOG.warn("Fail to create CompletedCheckpointStore, skip checkpoint verification, {}", e);
				return;
			} finally {
				if (completedCheckpointStore != null) {
					try {
						completedCheckpointStore.shutdown(JobStatus.CREATED);
					} catch (Exception e) {
						LOG.warn("Fail to close CompletedCheckpointStore, {}", e);
					}
				}
			}
		} catch (Exception e) {
			LOG.warn("Fail to create HAService at client, skip checkpoint verification, {}", e);
			return;
		}

		// Both ZK & haService has already closed, check verification result and exit if the check fails.
		switch (verifyResult) {
			case FAIL_MISS_OPERATOR_ID:
				verifyExitCode = MISS_OPERATOR_ID_EXIT_CODE;
				throw new Exception("Could not submit job, miss operator ID");
			case FAIL_MISMATCH_PARALLELISM:
				verifyExitCode = MISMATCH_PARALLELISM_EXIT_CODE;
				throw new Exception("Could not submit job, mismatch parallelism");
			case INVALID_SAVEPOINT_PATH:
				verifyExitCode = INVALID_SAVEPOINT_PATH_EXIT_CODE;
				throw new Exception("Could not submit job, invalid savepoint path");
		}
	}

	public static CheckpointVerifyResult doVerify(
		JobGraph jobGraph,
		ClassLoader classLoader,
		CompletedCheckpointStore completedCheckpointStore,
		Configuration configuration) {
		JobID jobID = jobGraph.getJobID();
		String jobUID = jobGraph.getJobUID();
		Iterable<JobVertex> taskVertices = jobGraph.getVertices();
		Map<JobVertexID, JobVertex> tasks = new HashMap<>();

		// Gather all tasks in JobVertex (using JobGraph at client).
		for (JobVertex jobVertex: taskVertices) {
			tasks.put(jobVertex.getID(), jobVertex);
		}

		if (completedCheckpointStore == null) {
			LOG.warn("{} Failed to initialize high-availability checkpoint handler", jobID);
			return CheckpointVerifyResult.ZOOKEEPER_RETRIEVE_FAIL;
		}

		if (completedCheckpointStore instanceof DefaultCompletedCheckpointStore) {
			// -----------------------------------------------------------------
			// Load checkpoints on Zookeeper.
			// -----------------------------------------------------------------
			DefaultCompletedCheckpointStore zkCompeletedCheckpointStore = (DefaultCompletedCheckpointStore) completedCheckpointStore;
			try {
				zkCompeletedCheckpointStore.recover();
			} catch (Exception e) {
				LOG.warn("Cannot recover CompletedCheckpoint from ZooKeeper, fail to conduct checkpoint verification");
				return CheckpointVerifyResult.ZOOKEEPER_RETRIEVE_FAIL;
			}

			// -----------------------------------------------------------------
			// Load checkpoints on HDFS, merge with ZooKeeper, and verify.
			// -----------------------------------------------------------------
			try {
				// (1) get checkpoints on HDFS.
				if (checkpointsOnStorage == null) {
					checkpointsOnStorage = findAllCompletedCheckpointsOnStorage(configuration, classLoader, jobID, jobUID);
				}
				LOG.info("Find checkpoints {} on HDFS.", checkpointsOnStorage.keySet());
				Map<OperatorID, OperatorState> latestOperatorStates;
				Map<OperatorID, OperatorStateMeta> latestOperatorStateMetas = null;
				Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>> latestOperatorStateAndMetas;
				// No checkpoint found on HDFS, check savepoint restore settings and directly set latestOperatorStates once loaded correctly
				if (checkpointsOnStorage.size() == 0) {
					LOG.info("No checkpoint store on HDFS, check savepoint settings.");
					SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
					if (savepointRestoreSettings != null && savepointRestoreSettings.restoreSavepoint()) {
						latestOperatorStates = getOperatorStatesFromSavepointSettings(configuration, classLoader, jobID, jobUID, savepointRestoreSettings);
						latestOperatorStateMetas = resolveStateMetaFromCheckpointPath(savepointRestoreSettings.getRestorePath(), classLoader);
						if (latestOperatorStates.isEmpty()) {
							final String message = "Load from savepoint restore settings failed with restore path : " + savepointRestoreSettings.getRestorePath();
							logAndSyserr(message);
							return CheckpointVerifyResult.INVALID_SAVEPOINT_PATH;
						}
					} else {
						LOG.info("No checkpoint store on HDFS and no savepoint settings, skip checkpoint verification");
						return CheckpointVerifyResult.SKIP;
					}
				} else {
					// (2) get checkpoints on ZooKeeper.
					final Map<Long, Map<OperatorID, OperatorState>> checkpointsOnStore =
						(Map<Long, Map<OperatorID, OperatorState>>) zkCompeletedCheckpointStore.getAllCheckpoints().stream()
							.collect(Collectors.toMap(CompletedCheckpoint::getCheckpointID, CompletedCheckpoint::getOperatorStates));
					LOG.info("Find checkpoints {} on Zookeeper.", checkpointsOnStore.keySet());

					// (3) merge checkpoints on HDFS to checkpoints on ZooKeeper.
					Map<Long, Map<OperatorID, OperatorState>> extraCheckpoints = new HashMap<>();
					for (Map.Entry<Long, Tuple2<String, Map<OperatorID, OperatorState>>> checkpoint: checkpointsOnStorage.entrySet()) {
						if (!checkpointsOnStore.containsKey(checkpoint.getKey())) {
							extraCheckpoints.put(checkpoint.getKey(), checkpoint.getValue().f1);
						}
					}

					LOG.info("There are {} checkpoints are on HDFS but not on Zookeeper.", extraCheckpoints.size());
					if (extraCheckpoints.size() > 0) {
						checkpointsOnStore.putAll(extraCheckpoints);
					}

					if (checkpointsOnStore.size() == 0) {
						LOG.info("No checkpoints on either ZooKeeper or HDFS, skip checkpoint verification.");
						return CheckpointVerifyResult.SKIP;
					}

					// (4) find the latest checkpoint, based on checkpoint ID.
					// Note: isPreferCheckpointForRecovery is not considered.
					long maxCheckpointID = checkpointsOnStore.keySet().stream().max(Long::compare).get();
					latestOperatorStates = checkpointsOnStore.get(maxCheckpointID);
					LOG.info("Latest checkpoint id {}", maxCheckpointID);

					// get latest snapshot state meta
					// make sure that the last checkpoint on HDFS is valid
					if (checkpointsOnStorage.containsKey(maxCheckpointID)) {
						String lastCheckpointPath = checkpointsOnStorage.get(maxCheckpointID).f0;
						latestOperatorStateMetas = resolveStateMetaFromCheckpointPath(lastCheckpointPath, classLoader);
					}
				}

				// merge CheckpointMetaData and StateMeta
				latestOperatorStateAndMetas = mergeLatestStateAndMetas(latestOperatorStates, latestOperatorStateMetas);

				CheckpointVerifyResult checkpointVerifyResult = getCheckpointVerifyResult(tasks, latestOperatorStateAndMetas);
				if (checkpointVerifyResult == CheckpointVerifyResult.SUCCESS) {
					LOG.info("Checkpoint verification success");
				}
				return checkpointVerifyResult;

			} catch (Exception e) {
				LOG.warn("Retrieve checkpoints from HDFS fail, {}", e);
				return CheckpointVerifyResult.HDFS_RETRIEVE_FAIL;
			}

		} else {
			LOG.warn("CompletedCheckpointStore is not running in ZooKeeper, fail to conduct checkpoint verification");
			return CheckpointVerifyResult.ZOOKEEPER_RETRIEVE_FAIL;
		}
	}

	/**
	 * Retrieve CompletedCheckpoint on HDFS. It is not easy, because we need ExecutionGraph actually.
	 * But we are at client-end, we do not load CompletedCheckpoint, just its metadata. The core information
	 * collection of <code>OperatorState</code> is extracted and returned.
	 *
	 * @param configuration Configuration of client.
	 * @param classLoader User ClassLoader.
	 * @param jobID The job's ID.
	 * @param jobUID The job's UID.
	 * @return The map of checkpoint ID to collection of operator state on HDFS.
	 *
	 */
	public static Map<Long, Tuple2<String, Map<OperatorID, OperatorState>>> findAllCompletedCheckpointsOnStorage(
		Configuration configuration,
		ClassLoader classLoader,
		JobID jobID,
		String jobUID) {
		Preconditions.checkNotNull(jobID, "jobId");
		Preconditions.checkNotNull(classLoader, "classLoader");

		int maxRetainCheckpoints = configuration.getInteger(MAX_RETAINED_CHECKPOINTS);
		LOG.info("Maximum retained checkpoints {}", maxRetainCheckpoints);

		// map checkpointID -> operatorStates
		final Map<Long, Tuple2<String, Map<OperatorID, OperatorState>>> result = new HashMap<>();

		try {
			CheckpointStorageCoordinatorView checkpointStorage =
				StateBackendLoader.loadStateBackendFromConfig(configuration, classLoader, LOG)
					.createCheckpointStorage(jobID, jobUID);

			for (String completedCheckpointPointer : checkpointStorage.findCompletedCheckpointPointer()) {
				// just load MAX_RETAINED_CHECKPOINTS checkpoints from HDFS, more detail INFOI-18858.
				if (result.size() >= maxRetainCheckpoints) {
					LOG.info("Already loaded {} checkpoints, skip others.", result.size());
					break;
				}

				try {
					final CompletedCheckpointStorageLocation location = checkpointStorage.resolveCheckpoint(completedCheckpointPointer);
					Preconditions.checkNotNull(location, "location");

					final StreamStateHandle metadataHandle = location.getMetadataHandle();
					final CheckpointMetadata rawCheckpointMetadata;
					try (InputStream in = metadataHandle.openInputStream()) {
						DataInputStream dis = new DataInputStream(in);
						rawCheckpointMetadata = loadCheckpointMetadata(dis, classLoader, completedCheckpointPointer);
					}

					long checkpointID = rawCheckpointMetadata.getCheckpointId();
					Collection<OperatorState> operatorStates = rawCheckpointMetadata.getOperatorStates();

					result.put(
						checkpointID,
						Tuple2.of(completedCheckpointPointer, operatorStates.stream().collect(Collectors.toMap(OperatorState::getOperatorID, Function.identity()))));
				} catch (Exception e) {
					LOG.warn("Fail to load checkpoint on {}.", completedCheckpointPointer, e);
				}
			}
		}
		catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
			LOG.warn("{} Could not instantiate configured state backend", jobID, e);
		}
		return result;
	}

	public static Map<OperatorID, OperatorState> getOperatorStatesFromSavepointSettings (
		Configuration configuration,
		ClassLoader classLoader,
		JobID jobID,
		String jobUID,
		SavepointRestoreSettings savepointRestoreSettings
		) {
		Map<OperatorID, OperatorState> latestOperatorStates = new HashMap<>();
		try {
			CheckpointStorageCoordinatorView checkpointStorage =
				StateBackendLoader.loadStateBackendFromConfig(configuration, classLoader, LOG)
					.createCheckpointStorage(jobID, jobUID);
			String restoreSavepointPath = savepointRestoreSettings.getRestorePath();
			final CompletedCheckpointStorageLocation location = checkpointStorage.resolveCheckpoint(restoreSavepointPath);

			// Load the savepoint into the system
			final StreamStateHandle metadataHandle = location.getMetadataHandle();
			final String checkpointPointer = location.getExternalPointer();

			final CheckpointMetadata checkpointMetadata;
			try (InputStream in = metadataHandle.openInputStream()) {
				DataInputStream dis = new DataInputStream(in);
				checkpointMetadata = loadCheckpointMetadata(dis, classLoader, checkpointPointer);
			}
			HashMap<OperatorID, OperatorState> operatorStates = new HashMap<>(checkpointMetadata.getOperatorStates().size());
			for (OperatorState operatorState : checkpointMetadata.getOperatorStates()) {
				operatorStates.put(operatorState.getOperatorID(), operatorState);
			}
			latestOperatorStates = operatorStates;
			LOG.info("Load savepoint {} as latest operator states", checkpointMetadata.getCheckpointId());
		} catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
			LOG.warn("{} Could not instantiate configured state backend", jobID, e);
		}
		return latestOperatorStates;
	}

	public static Map<OperatorID, OperatorStateMeta> resolveStateMetaFromCheckpointPath(String checkpointPointer, ClassLoader classLoader) throws Exception {
		Map<OperatorID, OperatorStateMeta> result = new HashMap<>();
		FileStateHandle fileStateHandle;
		try {
			fileStateHandle = AbstractFsCheckpointStorage
				.resolveStateMetaFileHandle(checkpointPointer);
		} catch (FileNotFoundException exception) {
			LOG.warn("Could not found stateMeta file in dir : " + checkpointPointer);
			return result;
		}

		try (DataInputStream stream = new DataInputStream(fileStateHandle.openInputStream())) {
			CheckpointStateMetadata checkpointStateMetadata = Checkpoints.loadCheckpointStateMetadata(stream, classLoader, checkpointPointer);
			result = checkpointStateMetadata.getOperatorStateMetas().stream().collect(Collectors.toMap(OperatorStateMeta::getOperatorID, Function.identity()));
		}
		return result;
	}

	public static Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>> mergeLatestStateAndMetas(Map<OperatorID, OperatorState> operatorStates, Map<OperatorID, OperatorStateMeta> operatorStateMetas) {
		HashMap<OperatorID, Tuple2<OperatorState, OperatorStateMeta>> operatorStateAndMetas = new HashMap<>();
		operatorStates.forEach((operatorID, operatorState) -> {
			OperatorStateMeta operatorStateMeta = null;
			if (operatorStateMetas != null && operatorStateMetas.containsKey(operatorID)) {
				operatorStateMeta = operatorStateMetas.get(operatorID);
			}
			operatorStateAndMetas.put(operatorID, Tuple2.of(operatorState, operatorStateMeta));
		});
		return operatorStateAndMetas;
	}

	public static CheckpointVerifyResult getCheckpointVerifyResult(Map<JobVertexID, JobVertex> tasks, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>> latestOperatorStateAndMetas){
		for (BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult> strategy: verifyStrategies) {
			CheckpointVerifyResult result = strategy.apply(tasks, latestOperatorStateAndMetas);
			if (result != CheckpointVerifyResult.SUCCESS) {
				return result;
			}
		}
		return CheckpointVerifyResult.SUCCESS;
	}

	@VisibleForTesting
	public static List<BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, Tuple2<OperatorState, OperatorStateMeta>>, CheckpointVerifyResult>> getVerifyStrategies() {
		return verifyStrategies;
	}

	private static void logAndSyserr(String message) {
		LOG.error(message);
		System.err.println("------------------------------------------------------------");
		System.err.println(message);
	}
}
