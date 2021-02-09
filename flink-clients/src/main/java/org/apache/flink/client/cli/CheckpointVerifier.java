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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.ZooKeeperCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
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

/**
 * Checkpoint verifier at client-end.
 */
public class CheckpointVerifier {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointVerifier.class);

	private static List<BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, OperatorState>, Boolean>> verifyStrategies;

	static {
		verifyStrategies = new ArrayList<>();
		verifyStrategies.add((tasks, operatorStates) -> {
			Set<OperatorID> allOperatorIDs = new HashSet<>();
			for (JobVertex jobVertex : tasks.values()) {
				allOperatorIDs.addAll(jobVertex.getOperatorIDs());
			}

			for (Map.Entry<OperatorID, OperatorState> operatorGroupStateEntry : operatorStates.entrySet()) {
				OperatorState operatorState = operatorGroupStateEntry.getValue();
				//----------------------------------------find operator for state---------------------------------------------

				if (!allOperatorIDs.contains(operatorGroupStateEntry.getKey())) {
					final String message = "There is no operator for the state " + operatorState.getOperatorID() +
						". If you see this, usually it means that the job's topology is changed. And " +
						" the state in previous checkpoint cannot be used in current job !!! \n" +
						" You need to revert your changes or change state.checkpoints.namespace to start a new checkpoint.";
					LOG.error(message);
					return false;
				}
			}

			return true;
		});
	}

	public static void verify(JobGraph jobGraph, ClassLoader classLoader, HighAvailabilityServices haService, Configuration configuration) {
		// -----------------------------------------------------------------
		// Check whether client checkpoint verification is enabled.
		// -----------------------------------------------------------------
		// If "allow-non-restored-state=true" is set, skip.
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
		if (checkpointingSettings == null) {
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

		// -----------------------------------------------------------------
		// Prepare CompletedCheckpointStore and verify.
		// -----------------------------------------------------------------
		CompletedCheckpointStore completedCheckpointStore = null;
		boolean alreadyPreparedHaService = (haService != null);
		CheckpointVerifyResult verifyResult = CheckpointVerifyResult.SUCCESS;

		try {
			// construct HA service if client do not provide an off-the-shelf one.
			if (haService == null) {
				// construct HA service and CompletedCheckpointStore
				try {
					haService = HighAvailabilityServicesUtils.createHighAvailabilityServices(
						configuration,
						Executors.directExecutor(),
						HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);
				} catch (Exception e) {
					LOG.warn("Fail to create HAService at client, skip checkpoint verification, {}", e);
					return;
				}
			}

			// construct CompletedCheckpointStore (ZooKeeper)
			try {
				completedCheckpointStore = haService.getCheckpointRecoveryFactory().createCheckpointStore(
					jobGraph.getJobID(), jobGraph.getName(), configuration.getInteger(MAX_RETAINED_CHECKPOINTS), ClassLoader.getSystemClassLoader());
			} catch (Exception e) {
				LOG.warn("Fail to create CompletedCheckpointStore, skip checkpoint verification, {}", e);
				return;
			}

			verifyResult = CheckpointVerifier.doVerify(jobGraph, classLoader, completedCheckpointStore, configuration);

		} finally {
			if (completedCheckpointStore != null) {
				try {
					completedCheckpointStore.shutdown(JobStatus.CREATED);
				} catch (Exception e) {
					LOG.warn("Fail to close CompletedCheckpointStore, {}", e);
				}
			}

			if (!alreadyPreparedHaService && haService != null) {
				try {
					haService.close();
				} catch (Exception e) {
					LOG.warn("Fail to close HA services {}", e);
				}
			}

			Preconditions.checkState(verifyResult != CheckpointVerifyResult.FAIL,
				"Checkpoint verification at client fails, unable to submit job. " +
					"You can skip this by disabling checkpoint, or set allowNonRestoredState to true.");
		}
	}

	public static CheckpointVerifyResult doVerify(
			JobGraph jobGraph,
			ClassLoader classLoader,
			CompletedCheckpointStore completedCheckpointStore,
			Configuration configuration) {
		JobID jobID = jobGraph.getJobID();
		String jobName = jobGraph.getName();
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

		if (completedCheckpointStore instanceof ZooKeeperCompletedCheckpointStore) {
			// -----------------------------------------------------------------
			// Load checkpoints on Zookeeper.
			// -----------------------------------------------------------------
			ZooKeeperCompletedCheckpointStore zkCompeletedCheckpointStore = (ZooKeeperCompletedCheckpointStore) completedCheckpointStore;
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
				final Map<Long, Map<OperatorID, OperatorState>> checkpointsOnStorage =
					findAllCompletedCheckpointsOnStorage(configuration, classLoader, jobID, jobName);
				LOG.info("Find checkpoints {} on HDFS.", checkpointsOnStorage.keySet());

				// (2) get checkpoints on ZooKeeper.
				final Map<Long, Map<OperatorID, OperatorState>> checkpointsOnStore =
					zkCompeletedCheckpointStore.getAllCheckpoints().stream()
					.collect(Collectors.toMap(CompletedCheckpoint::getCheckpointID, CompletedCheckpoint::getOperatorStates));
				LOG.info("Find checkpoints {} on Zookeeper.", checkpointsOnStore.keySet());

				// (3) merge checkpoints on HDFS to checkpoints on ZooKeeper.
				Map<Long, Map<OperatorID, OperatorState>> extraCheckpoints = new HashMap<>();
				for (Map.Entry<Long, Map<OperatorID, OperatorState>> checkpoint: checkpointsOnStorage.entrySet()) {
					if (!checkpointsOnStore.containsKey(checkpoint.getKey())) {
						extraCheckpoints.put(checkpoint.getKey(), checkpoint.getValue());
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
				Map<OperatorID, OperatorState> latestOperatorStates = checkpointsOnStore.get(maxCheckpointID);
				LOG.info("Latest checkpoint id {}", maxCheckpointID);

				// (5) iterate verify strategies.
				for (BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, OperatorState>, Boolean> strategy: verifyStrategies) {
					if (!strategy.apply(tasks, latestOperatorStates)) {
						return CheckpointVerifyResult.FAIL;
					}
				}

			} catch (Exception e) {
				LOG.warn("Retrieve checkpoints from HDFS fail, {}", e);
				return CheckpointVerifyResult.HDFS_RETRIEVE_FAIL;
			}

		} else {
			LOG.warn("CompletedCheckpointStore is not running in ZooKeeper, fail to conduct checkpoint verification");
			return CheckpointVerifyResult.ZOOKEEPER_RETRIEVE_FAIL;
		}

		LOG.info("Checkpoint verification success");
		return CheckpointVerifyResult.SUCCESS;
	}

	/**
	 * Retrieve CompletedCheckpoint on HDFS. It is not easy, because we need ExecutionGraph actually.
	 * But we are at client-end, we do not load CompletedCheckpoint, just its metadata. The core information
	 * collection of <code>OperatorState</code> is extracted and returned.
	 *
	 * @param configuration Configuration of client.
	 * @param classLoader User ClassLoader.
	 * @param jobID The job's ID.
	 * @param jobName The job's name.
	 * @return The map of checkpoint ID to collection of operator state on HDFS.
	 *
	 */
	public static Map<Long, Map<OperatorID, OperatorState>> findAllCompletedCheckpointsOnStorage(
			Configuration configuration,
			ClassLoader classLoader,
			JobID jobID,
			String jobName) {
		Preconditions.checkNotNull(jobID, "jobId");
		Preconditions.checkNotNull(classLoader, "classLoader");

		int maxRetainCheckpoints = configuration.getInteger(MAX_RETAINED_CHECKPOINTS);
		LOG.info("Maximum retained checkpoints {}", maxRetainCheckpoints);

		// map checkpointID -> operatorStates
		final Map<Long, Map<OperatorID, OperatorState>> result = new HashMap<>();

		try {
			CheckpointStorageCoordinatorView checkpointStorage =
				StateBackendLoader.loadStateBackendFromConfig(configuration, classLoader, LOG)
				.createCheckpointStorage(jobID, jobName);

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
					final Savepoint rawCheckpointMetadata;
					try (InputStream in = metadataHandle.openInputStream()) {
						DataInputStream dis = new DataInputStream(in);
						rawCheckpointMetadata = loadCheckpointMetadata(dis, classLoader);
					}

					long checkpointID = rawCheckpointMetadata.getCheckpointId();
					Collection<OperatorState> operatorStates = rawCheckpointMetadata.getOperatorStates();

					result.put(
						checkpointID,
						operatorStates.stream().collect(Collectors.toMap(OperatorState::getOperatorID, Function.identity())));
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
}
