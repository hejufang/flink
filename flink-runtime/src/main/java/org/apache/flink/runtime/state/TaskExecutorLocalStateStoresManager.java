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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * This class holds the all {@link TaskLocalStateStoreImpl} objects for a task executor (manager).
 */
public class TaskExecutorLocalStateStoresManager implements TaskLocalStateListener {

	/** Logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorLocalStateStoresManager.class);

	/** Minimum time interval to try to fail tasks. */
	private static final long FAIL_TASK_MIN_INTERVAL = 60 * 1000L;

	/**
	 * This map holds all local state stores for tasks running on the task manager / executor that own the instance of
	 * this. Maps from allocation id to all the subtask's local state stores.
	 */
	@GuardedBy("lock")
	private final Map<AllocationID, Map<JobVertexSubtaskKey, OwnedTaskLocalStateStore>> taskStateStoresByAllocationID;

	/** This map is used to record all tasks allocated by a slot and the local state size of each task. */
	@GuardedBy("lock")
	private final Map<AllocationID, Map<JobVertexSubtaskKey, Tuple3<ExecutionAttemptID, TaskManagerActions, Long>>> taskLocalStateSize;

	/** This map is used to record the slots allocated by all jobs in taskManager and the local state size of each job. */
	@GuardedBy("lock")
	private final Map<JobID, Tuple2<Set<AllocationID>, Long>> jobIdToAllocationId;

	/** The configured mode for local recovery on this task manager. */
	private final boolean localRecoveryEnabled;

	/** This is the root directory for all local state of this task manager / executor. */
	private final File[] localStateRootDirectories;

	/** Executor that runs the discarding of released state objects. */
	private final Executor discardExecutor;

	/** Guarding lock for taskStateStoresByAllocationID and closed-flag. */
	private final Object lock;

	private final Thread shutdownHook;

	@GuardedBy("lock")
	private boolean closed;

	/** Configuration of local state management. */
	private LocalStateManageConfig localStateManageConfig;

	/** The total size of the local state in the current taskManager. */
	private long totalLocalStateSize = 0L;

	/** The time of the last attempt to fail the task. */
	private long lastFailTaskTime = 0L;

	/** Metrics related configuration. */
	private static final String WAREHOUSE_LOCAL_STATE_MESSAGE = "local_state_message";
	private static final String LOCAL_STATE_SIZE = "localStateSize";
	private static final String NUMBER_OF_TM_EXCEED_QUOTA = "numberOfTmExceedQuota";

	private static final MessageSet<WarehouseLocalStateMessage> localStateMessage = new MessageSet<>(MessageType.LOCAL_STATE);

	public TaskExecutorLocalStateStoresManager(
		boolean localRecoveryEnabled,
		@Nonnull File[] localStateRootDirectories,
		@Nonnull Executor discardExecutor) throws IOException {
		this(
			localRecoveryEnabled,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			localStateRootDirectories,
			new LocalStateManageConfig(Long.MAX_VALUE, Long.MAX_VALUE, false),
			discardExecutor);
	}

	public TaskExecutorLocalStateStoresManager(
		boolean localRecoveryEnabled,
		MetricGroup taskManagerMetricGroup,
		@Nonnull File[] localStateRootDirectories,
		LocalStateManageConfig localStateManageConfig,
		@Nonnull Executor discardExecutor) throws IOException {

		this.taskStateStoresByAllocationID = new HashMap<>();
		this.taskLocalStateSize = new HashMap<>();
		this.jobIdToAllocationId = new HashMap<>();
		this.localRecoveryEnabled = localRecoveryEnabled;
		this.localStateRootDirectories = localStateRootDirectories;
		this.discardExecutor = discardExecutor;
		this.lock = new Object();
		this.closed = false;
		this.localStateManageConfig = localStateManageConfig;

		for (File localStateRecoveryRootDir : localStateRootDirectories) {

			if (!localStateRecoveryRootDir.exists()
				&& !localStateRecoveryRootDir.mkdirs()
				// we double check for exists in case another task created the directory concurrently.
				&& !localStateRecoveryRootDir.exists()) {
				throw new IOException("Could not create root directory for local recovery: " +
					localStateRecoveryRootDir);
			}
		}

		// register a shutdown hook
		this.shutdownHook = ShutdownHookUtil.addShutdownHook(this::shutdown, getClass().getSimpleName(), LOG);
		registerMetrics(taskManagerMetricGroup);
	}

	@Nonnull
	public TaskLocalStateStore localStateStoreForSubtask(
		@Nonnull JobID jobId,
		@Nonnull AllocationID allocationID,
		@Nonnull JobVertexID jobVertexID,
		@Nonnegative int subtaskIndex,
		@Nonnull ExecutionAttemptID executionAttemptID,
		@Nonnull TaskManagerActions taskManagerActions) {

		synchronized (lock) {

			if (closed) {
				throw new IllegalStateException("TaskExecutorLocalStateStoresManager is already closed and cannot " +
					"register a new TaskLocalStateStore.");
			}

			Map<JobVertexSubtaskKey, OwnedTaskLocalStateStore> taskStateManagers =
				this.taskStateStoresByAllocationID.get(allocationID);
			Map<JobVertexSubtaskKey, Tuple3<ExecutionAttemptID, TaskManagerActions, Long>> taskLocalStateSizeMap =
				this.taskLocalStateSize.computeIfAbsent(allocationID, aid -> new HashMap<>());

			Tuple2<Set<AllocationID>, Long> jidToAllocationId = this.jobIdToAllocationId.computeIfAbsent(jobId, jid -> Tuple2.of(new HashSet<>(), 0L));

			if (taskStateManagers == null) {
				taskStateManagers = new HashMap<>();
				this.taskStateStoresByAllocationID.put(allocationID, taskStateManagers);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Registered new allocation id {} for local state stores for job {}.",
						allocationID.toHexString(), jobId);
				}
			}

			final JobVertexSubtaskKey taskKey = new JobVertexSubtaskKey(jobId, jobVertexID, subtaskIndex);

			OwnedTaskLocalStateStore taskLocalStateStore = taskStateManagers.get(taskKey);

			if (taskLocalStateStore == null) {

				// create the allocation base dirs, one inside each root dir.
				File[] allocationBaseDirectories = allocationBaseDirectories(allocationID);

				LocalRecoveryDirectoryProviderImpl directoryProvider = new LocalRecoveryDirectoryProviderImpl(
					allocationBaseDirectories,
					jobId,
					jobVertexID,
					subtaskIndex);

				LocalRecoveryConfig localRecoveryConfig =
					new LocalRecoveryConfig(localRecoveryEnabled, directoryProvider);

				taskLocalStateStore = localRecoveryConfig.isLocalRecoveryEnabled() ?

						// Real store implementation if local recovery is enabled
						new TaskLocalStateStoreImpl(
							jobId,
							allocationID,
							jobVertexID,
							subtaskIndex,
							localRecoveryConfig,
							discardExecutor,
							this) :

						// NOP implementation if local recovery is disabled
						new NoOpTaskLocalStateStoreImpl(
							jobId,
							allocationID,
							jobVertexID,
							subtaskIndex,
							localRecoveryConfig,
							this);
				taskStateManagers.put(taskKey, taskLocalStateStore);
				taskLocalStateSizeMap.put(taskKey, Tuple3.of(executionAttemptID, taskManagerActions, 0L));
				jidToAllocationId.f0.add(allocationID);

				LOG.debug("Registered new local state store with configuration {} for {} - {} - {} under allocation " +
						"id {}.", localRecoveryConfig, jobId, jobVertexID, subtaskIndex, allocationID);

			} else {
				LOG.debug("Found existing local state store for {} - {} - {} under allocation id {}: {}",
					jobId, jobVertexID, subtaskIndex, allocationID, taskLocalStateStore);
			}

			return taskLocalStateStore;
		}
	}

	public void releaseLocalStateForAllocationId(@Nonnull AllocationID allocationID) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Releasing local state under allocation id {}.", allocationID);
		}

		Map<JobVertexSubtaskKey, OwnedTaskLocalStateStore> cleanupLocalStores;

		synchronized (lock) {
			if (closed) {
				return;
			}
			cleanupLocalStores = taskStateStoresByAllocationID.remove(allocationID);
			Map<JobVertexSubtaskKey, Tuple3<ExecutionAttemptID, TaskManagerActions, Long>> subtaskLocalStateSize = taskLocalStateSize.remove(allocationID);
			if (subtaskLocalStateSize != null) {
				for (Map.Entry<JobVertexSubtaskKey, Tuple3<ExecutionAttemptID, TaskManagerActions, Long>> entry : subtaskLocalStateSize.entrySet()) {
					Tuple2<Set<AllocationID>, Long> jobStateSize = jobIdToAllocationId.get(entry.getKey().jobID);
					if (jobStateSize != null) {
						jobStateSize.f0.remove(allocationID);
						jobStateSize.f1 -= entry.getValue().f2;
						if (jobStateSize.f0.isEmpty()) {
							jobIdToAllocationId.remove(entry.getKey().jobID);
						}
					} else {
						break;
					}
				}
			}
		}

		if (cleanupLocalStores != null) {
			doRelease(cleanupLocalStores.values());
		}

		cleanupAllocationBaseDirs(allocationID);
	}

	public void shutdown() {

		HashMap<AllocationID, Map<JobVertexSubtaskKey, OwnedTaskLocalStateStore>> toRelease;

		synchronized (lock) {

			if (closed) {
				return;
			}

			closed = true;
			toRelease = new HashMap<>(taskStateStoresByAllocationID);
			taskStateStoresByAllocationID.clear();
			taskLocalStateSize.clear();
			jobIdToAllocationId.clear();
			totalLocalStateSize = 0L;
		}

		ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

		LOG.info("Shutting down TaskExecutorLocalStateStoresManager.");

		for (Map.Entry<AllocationID, Map<JobVertexSubtaskKey, OwnedTaskLocalStateStore>> entry :
			toRelease.entrySet()) {

			doRelease(entry.getValue().values());
			cleanupAllocationBaseDirs(entry.getKey());
		}
	}

	@VisibleForTesting
	boolean isLocalRecoveryEnabled() {
		return localRecoveryEnabled;
	}

	@VisibleForTesting
	File[] getLocalStateRootDirectories() {
		return localStateRootDirectories;
	}

	@VisibleForTesting
	String allocationSubDirString(AllocationID allocationID) {
		return "aid_" + allocationID.toHexString();
	}

	private File[] allocationBaseDirectories(AllocationID allocationID) {
		final String allocationSubDirString = allocationSubDirString(allocationID);
		final File[] allocationDirectories = new File[localStateRootDirectories.length];
		for (int i = 0; i < localStateRootDirectories.length; ++i) {
			allocationDirectories[i] = new File(localStateRootDirectories[i], allocationSubDirString);
		}
		return allocationDirectories;
	}

	private void doRelease(Iterable<OwnedTaskLocalStateStore> toRelease) {

		if (toRelease != null) {

			for (OwnedTaskLocalStateStore stateStore : toRelease) {
				try {
					stateStore.dispose();
				} catch (Exception disposeEx) {
					LOG.warn("Exception while disposing local state store {}.", stateStore, disposeEx);
				}
			}
		}
	}

	/**
	 * Deletes the base dirs for this allocation id (recursively).
	 */
	private void cleanupAllocationBaseDirs(AllocationID allocationID) {
		// clear the base dirs for this allocation id.
		File[] allocationDirectories = allocationBaseDirectories(allocationID);
		for (File directory : allocationDirectories) {
			try {
				FileUtils.deleteFileOrDirectory(directory);
			} catch (IOException e) {
				LOG.warn("Exception while deleting local state directory for allocation id {}.", allocationID, e);
			}
		}
	}

	@Override
	public void notifyTaskLocalStateSize(AllocationID allocationID, JobID jobID, JobVertexID jobVertexID, int subtaskIndex, long localStateSize) {
		synchronized (lock) {
			Map<JobVertexSubtaskKey, Tuple3<ExecutionAttemptID, TaskManagerActions, Long>> subtaskLocalStateSize = taskLocalStateSize.get(allocationID);
			JobVertexSubtaskKey subtaskKey = new JobVertexSubtaskKey(jobID, jobVertexID, subtaskIndex);
			if (subtaskLocalStateSize != null && subtaskLocalStateSize.containsKey(subtaskKey)) {
				Tuple3<ExecutionAttemptID, TaskManagerActions, Long> oldSize = subtaskLocalStateSize.get(subtaskKey);
				long delta = localStateSize - oldSize.f2;
				oldSize.f2 = localStateSize;
				totalLocalStateSize += delta;
				jobIdToAllocationId.get(jobID).f1 += delta;
			}

			if (totalLocalStateSize > localStateManageConfig.getExpectedMaxLocalStateSize()
				&& System.currentTimeMillis() - lastFailTaskTime >= FAIL_TASK_MIN_INTERVAL) {

				LOG.warn("The current local state({} MB) of the taskManager exceeds the maximum value ({} MB).", totalLocalStateSize >> 20, localStateManageConfig.getExpectedMaxLocalStateSize() >> 20);
				Optional<Map.Entry<JobID, Tuple2<Set<AllocationID>, Long>>> optionalEntry = jobIdToAllocationId.entrySet().stream().max((o1, o2) -> o1.getValue().f1 < o2.getValue().f1 ? -1 : 1);

				int failedTask = 0;
				if (localStateManageConfig.isFailExceedQuotaTask() && optionalEntry.isPresent() && totalLocalStateSize > localStateManageConfig.getActualMaxLocalStateSize()) {
					Map.Entry<JobID, Tuple2<Set<AllocationID>, Long>> maxLocalStateSizeJob = optionalEntry.get();
					LOG.info("The state of the job({}) occupies the most({} MB), and starts to force fail the task.",
						maxLocalStateSizeJob.getKey(), maxLocalStateSizeJob.getValue().f1 >> 20);
					for (AllocationID aid : maxLocalStateSizeJob.getValue().f0) {
						failedTask += failTaskWithSpecifiedAllocationId(aid);
					}
					lastFailTaskTime = System.currentTimeMillis();
					LOG.info("All {} tasks belonging to the job({}) are all failed.", failedTask, jobID);
				}

				localStateMessage.addMessage(new Message<>(new WarehouseLocalStateMessage(
					totalLocalStateSize,
					localStateManageConfig.getExpectedMaxLocalStateSize(),
					localStateManageConfig.getActualMaxLocalStateSize(),
					true,
					failedTask)));
			}
		}
	}

	@GuardedBy("lock")
	private int failTaskWithSpecifiedAllocationId(AllocationID allocationID) {
		Map<JobVertexSubtaskKey, Tuple3<ExecutionAttemptID, TaskManagerActions, Long>> subtaskWithAllocationId = taskLocalStateSize.get(allocationID);
		if (subtaskWithAllocationId != null) {
			LocalStateExceedDiskQuotaException localStateExceedDiskQuotaException = new LocalStateExceedDiskQuotaException("The local state size exceeds the maximum disk limit in a single taskManager.");
			for (Map.Entry<JobVertexSubtaskKey, Tuple3<ExecutionAttemptID, TaskManagerActions, Long>> entry : subtaskWithAllocationId.entrySet()) {
				TaskManagerActions taskManagerActions = entry.getValue().f1;
				ExecutionAttemptID executionAttemptID = entry.getValue().f0;
				taskManagerActions.failTask(executionAttemptID, localStateExceedDiskQuotaException);
			}
			return subtaskWithAllocationId.size();
		}
		return 0;
	}

	/**
	 * Composite key of {@link JobVertexID} and subtask index that describes the subtask of a job vertex.
	 */
	private static final class JobVertexSubtaskKey {

		@Nonnull
		final JobID jobID;

		/** The job vertex id. */
		@Nonnull
		final JobVertexID jobVertexID;

		/** The subtask index. */
		@Nonnegative
		final int subtaskIndex;

		JobVertexSubtaskKey(@Nonnull JobID jobID, @Nonnull JobVertexID jobVertexID, @Nonnegative int subtaskIndex) {
			this.jobID = jobID;
			this.jobVertexID = jobVertexID;
			this.subtaskIndex = subtaskIndex;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			JobVertexSubtaskKey that = (JobVertexSubtaskKey) o;

			return subtaskIndex == that.subtaskIndex && jobID.equals(that.jobID) && jobVertexID.equals(that.jobVertexID);
		}

		@Override
		public int hashCode() {
			int result = jobID.hashCode();
			result = 31 * result + jobVertexID.hashCode();
			result = 31 * result + subtaskIndex;
			return result;
		}
	}

	private void registerMetrics(MetricGroup metricGroup) {
		metricGroup.gauge(LOCAL_STATE_SIZE, () -> totalLocalStateSize);
		metricGroup.gauge(NUMBER_OF_TM_EXCEED_QUOTA, () -> totalLocalStateSize > localStateManageConfig.getExpectedMaxLocalStateSize() ? 1 : 0);
		metricGroup.gauge(WAREHOUSE_LOCAL_STATE_MESSAGE, localStateMessage);
	}
}
