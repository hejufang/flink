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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Yarn implementation of LocalResultPartitionResolver.
 */
public class YarnLocalResultPartitionResolver extends LocalResultPartitionResolver {
	private static final Logger LOG = LoggerFactory.getLogger(YarnLocalResultPartitionResolver.class);

	private static final int RESULT_PARTITION_MAP_INITIAL_CAPACITY = 2000;

	private static final int APP_ID_MAP_INITIAL_CAPACITY = 100;

	private final FileSystem fileSystem;

	/** Only search result partitions in these applications' directories. */
	@VisibleForTesting
	protected final ConcurrentHashMap<String, String> appIdToUser = new ConcurrentHashMap<>(
		APP_ID_MAP_INITIAL_CAPACITY);

	/** To accelerate the mapping from ResultPartitionID to its directory.
	 *  Since this concurrent hash map is not guarded by any lock, don't use PUT operation
	 *  in order to make sure that no information losses.
	 */
	@VisibleForTesting
	protected final ConcurrentHashMap<ResultPartitionID, ConcurrentHashMap<Integer, YarnResultSubPartitionFileInfo>>
		resultPartitionMap = new ConcurrentHashMap<>(RESULT_PARTITION_MAP_INITIAL_CAPACITY);

	/**
	 * The thread is designed to do expensive recursive directory deletion, only recycle result partition files
	 *  in flink session mode since NodeManager can do the full recycle after stopApplication.
	 */
	private final ScheduledExecutorService diskScannerExecutorService;

	private long lastDiskScanTimestamp = -1L;

	/**
	 * Since YarnShuffleService runs as a module in NodeManager process, while the users of
	 * result partitions' producers vary according to application submitters, in mostly common scenarios
	 * those two kinds of users are not the same. So we cannot just use FileSystem.delete() to
	 * do recycling in YarnShuffleService. Here we use a hadoop tool named "container-executor"
	 * which is used by NodeManager to do privileged operations including recycling containers'
	 * working directories.
	 */
	private final String containerExecutorExecutablePath;

	/**
	 * True if linux-container-executor should limit itself to one user
	 * when running in non-secure mode.
	 */
	private final Boolean containerLimitUsers;

	/**
	 * The UNIX user that containers will run as when Linux-container-executor
	 * is used in nonsecure mode (a use case for this is using cgroups).
	 */
	private final String nonsecureLocalUser;

	YarnLocalResultPartitionResolver(ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration) {
		super(shuffleServiceConfiguration);

		this.fileSystem = shuffleServiceConfiguration.getFileSystem();

		this.containerExecutorExecutablePath = parseContainerExecutorExecutablePath(
			shuffleServiceConfiguration, fileSystem);

		this.nonsecureLocalUser = shuffleServiceConfiguration.getConfiguration().getString(
			YarnConfiguration.NM_NONSECURE_MODE_LOCAL_USER_KEY,
			YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER);

		// For compatibility, use raw string of YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS
		// because this configuration is not supported in yarn 2.4 .
		this.containerLimitUsers = shuffleServiceConfiguration.getConfiguration().getBoolean(
			YarnConfiguration.NM_PREFIX + "linux-container-executor.nonsecure-mode.limit-users",
			true);

		ThreadFactory diskScannerThreadFactory = new DispatcherThreadFactory(
			new ThreadGroup("FlinkShuffleService"), "DiskScanner");
		this.diskScannerExecutorService = Executors.newSingleThreadScheduledExecutor(diskScannerThreadFactory);
		this.diskScannerExecutorService.scheduleWithFixedDelay(
			() -> doDiskScan(),
			0,
			shuffleServiceConfiguration.getDiskScanIntervalInMS(),
			TimeUnit.MILLISECONDS);
	}

	@Override
	public void initializeApplication(String user, String appId) {
		appIdToUser.putIfAbsent(appId, user);
	}

	@Override
	public Set<ResultPartitionID> stopApplication(String appId) {
		// Don't need to deal with partition files because NodeManager will recycle application's directory.
		Set<ResultPartitionID> toRemove = new HashSet<>();
		Iterator<Map.Entry<ResultPartitionID, ConcurrentHashMap<Integer, YarnResultSubPartitionFileInfo>>> partitionIterator =
			resultPartitionMap.entrySet().iterator();
		while (partitionIterator.hasNext()) {
			Map.Entry<ResultPartitionID, ConcurrentHashMap<Integer, YarnResultSubPartitionFileInfo>> entry = partitionIterator.next();
			if (!entry.getValue().isEmpty() && entry.getValue().values().iterator().next().getAppId().equals(appId)) {
				toRemove.add(entry.getKey());
				partitionIterator.remove();
			}
		}
		appIdToUser.remove(appId);
		return toRemove;
	}

	@Override
	public ResultSubPartitionFileInfo getResultSubPartitionFileInfo(
			ResultPartitionID resultPartitionID,
			int subPartitionIndex) throws IOException {
		Map<Integer, YarnResultSubPartitionFileInfo> subPartitionIndexToFileInfo = resultPartitionMap.get(resultPartitionID);
		YarnResultSubPartitionFileInfo fileInfo = null;
		if (subPartitionIndexToFileInfo != null) {
			fileInfo = subPartitionIndexToFileInfo.get(Integer.valueOf(subPartitionIndex));
		}
		if (fileInfo == null) {
			// Cache miss, scan configured directories to search for the result partition's directory.
			fileInfo = searchForResultSubPartition(resultPartitionID, subPartitionIndex);
		}
		if (fileInfo == null) {
			LOG.warn("Fail to find files in subpartition {}.", resultPartitionID.toString());
			throw new PartitionNotFoundException(resultPartitionID);
		}
		fileInfo.updateOnConsumption();
		return fileInfo;
	}

	@Override
	public void recycleResultPartition(ResultPartitionID resultPartitionID) {
		ConcurrentHashMap<Integer, YarnResultSubPartitionFileInfo> subPartitionToFileInfo = resultPartitionMap.get(resultPartitionID);
		if (subPartitionToFileInfo != null) {
			for (YarnResultSubPartitionFileInfo fileInfo : subPartitionToFileInfo.values()) {
				fileInfo.markToDelete(); // Lazy deletion, do real deletion during disk scan.
			}
		}
	}

	@Override
	public void stop() {
		LOG.warn("stop YarnLocalResultPartitionResolver.");
		try {
			diskScannerExecutorService.shutdownNow();
		} catch (Throwable e) {
			LOG.error("Exception occurs when stopping YarnLocalResultPartitionResolver", e);
		}
	}

	// ------------------------------------- Internal Utilities -----------------------------------

	/**
	 * NodeManager will generate one dir for an application in each local dir,
	 * this method is a helper method to get the application's local dir relative to
	 * actual dirs in different disks.
	 *
	 * <p>See {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer}
	 */
	public static String generateRelativeLocalAppDir(String user, String appId) {
		return "usercache/" + user + "/appcache/" + appId;
	}

	/**
	 * Search the directory for a result sub partition if fail to find its directory in result partition cache.
	 * @param resultPartitionID
	 * @return The information of this result sub partition's data files.
	 */
	private YarnResultSubPartitionFileInfo searchForResultSubPartition(
		ResultPartitionID resultPartitionID,
		Integer subPartitionIndex) {

		// Search through all the running applications.
		for (Map.Entry<String, String> appIdAndUser : appIdToUser.entrySet()) {
			// Search through all the configured root directories.
			String appId = appIdAndUser.getKey();
			String user = appIdAndUser.getValue();
			String relativePartitionDir = ExternalBlockShuffleUtils.generatePartitionRootPath(
				generateRelativeLocalAppDir(user, appId),
				resultPartitionID);

			LOG.info("Searching file for subpartition {} in {}", resultPartitionID.toString(), relativePartitionDir);
			for (String rootDir : shuffleServiceConfiguration.getDirToDiskType().keySet()) {
				String partitionDir = rootDir + relativePartitionDir;
				String subPartitionFile = ExternalBlockShuffleUtils.generateSubPartitionFile(partitionDir, subPartitionIndex);

				try {
					final FileStatus subPartitionStatus = fileSystem.getFileStatus(new Path(subPartitionFile));
					if (subPartitionStatus != null) {
						ConcurrentHashMap<Integer, YarnResultSubPartitionFileInfo> subPartitionToFileInfo =
								resultPartitionMap.computeIfAbsent(resultPartitionID, ignored -> new ConcurrentHashMap<>());
						YarnResultSubPartitionFileInfo fileInfo = subPartitionToFileInfo.computeIfAbsent(
								subPartitionIndex,
								ignored -> new YarnResultSubPartitionFileInfo(
										appId,
										new Tuple2<>(rootDir, subPartitionFile),
										true,
										false,
										subPartitionStatus.getModificationTime(),
										System.currentTimeMillis(),
										shuffleServiceConfiguration));
						return fileInfo;
					}
				} catch (IOException e) {
					LOG.warn("Error in searching files for subpartition {}, index {}.",
							resultPartitionID.toString(), subPartitionIndex.toString(), e);
				}
			}
		}
		return null;
	}

	/**
	 * This method will be triggered periodically to achieve the following four goals:
	 * (1) Generate the latest result partition cache for accelerating getResultSubPartitionFileInfo.
	 * (2) Recycle result partitions which are ready to be fetched but haven't been consumed for a period of time.
	 * (3) Recycle result partitions which are unfinished but haven't been accessed for a period of time.
	 * (4) Recycle consumed result partitions according to ExternalBlockResultPartitionManager's demand.
	 */
	@VisibleForTesting
	void doDiskScan() {
		long currTime = System.currentTimeMillis();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Start to do disk scan, currTime: " + currTime);
		}

		// 1. Traverse all the result partition directories including unfinished result partitions.
		// Traverse all the applications.
		Set<String> rootDirs = shuffleServiceConfiguration.getDirToDiskType().keySet();
		for (Map.Entry<String, String> userAndAppId : appIdToUser.entrySet()) {
			String user = userAndAppId.getValue();
			String appId = userAndAppId.getKey();
			String relativeAppDir = generateRelativeLocalAppDir(user, appId);
			// Traverse all the configured directories for this application.
			for (String rootDir : rootDirs) {
				String appDir = rootDir + relativeAppDir + "/";
				FileStatus[] appDirStatuses = null;

				try {
					appDirStatuses = fileSystem.listStatus(new Path(appDir));
				} catch (Exception e) {
					continue;
				}
				if (appDirStatuses == null) {
					continue;
				}
				// Traverse all the result partitions of this application in this root directory.
				for (FileStatus partitionDirStatus : appDirStatuses) {
					if (!partitionDirStatus.isDir()) {
						continue;
					}

					String resultPartitionDir = partitionDirStatus.getPath().getName();
					ResultPartitionID resultPartitionID =
							ExternalBlockShuffleUtils.convertRelativeDirToResultPartitionID(resultPartitionDir);
					if (resultPartitionID == null) {
						continue;
					}

					updateResultSubPartitionFileInfoByFileStatus(
						currTime, appId, resultPartitionID, rootDir, appDir + resultPartitionDir);
				}
			}
		}
		LOG.debug("The partition status is {}", resultPartitionMap);

		// 2. Remove out-of-date caches and partition files to be deleted through resultPartitionMap.
		Iterator<Map.Entry<ResultPartitionID, ConcurrentHashMap<Integer, YarnResultSubPartitionFileInfo>>> partitionIterator =
			resultPartitionMap.entrySet().iterator();
		while (partitionIterator.hasNext()) {
			Iterator<Map.Entry<Integer, YarnResultSubPartitionFileInfo>> subPartitionToFileInfoIterator =
					partitionIterator.next().getValue().entrySet().iterator();

			if (!subPartitionToFileInfoIterator.hasNext()) {
				partitionIterator.remove();
			}

			while (subPartitionToFileInfoIterator.hasNext()) {
				YarnResultSubPartitionFileInfo fileInfo = subPartitionToFileInfoIterator.next().getValue();
				boolean needToRemoveFileInfo = false;
				if (fileInfo.needToDelete()) {
					needToRemoveFileInfo = true;
					// Cannot distinguish between CONSUMED_PARTITION_TTL_TIMEOUT and PARTIAL_CONSUMED_PARTITION_TTL_TIMEOUT,
					// ExternalBlockResultPartitionManager has already logged detailed information before.
					removeResultSubPartition(new Path(fileInfo.getSubPartitionFile()),
							"FETCHED_PARTITION_TTL_TIMEOUT",
							-1,
							false);
				} else if (fileInfo.getFileInfoTimestamp() <= lastDiskScanTimestamp) {
					// The partition's file no longer exists, just delete its file info.
					needToRemoveFileInfo = true;
				}
				if (needToRemoveFileInfo) {
					subPartitionToFileInfoIterator.remove();
				}
			}
		}

		// 3. Update disk scan timestamp.
		if (LOG.isDebugEnabled()) {
			LOG.debug("Finish disk scan, cost {} ms. {}", (System.currentTimeMillis() - currTime), resultPartitionMap);
		}
		lastDiskScanTimestamp = currTime;
	}

	private void updateResultSubPartitionFileInfoByFileStatus(
		long currTime,
		String appId,
		ResultPartitionID resultPartitionID,
		String rootDir,
		String resultPartitionDir) {

		FileStatus[] resultPartitionDirStatuses = null;
		try {
			resultPartitionDirStatuses = fileSystem.listStatus(new Path(resultPartitionDir));
		} catch (Exception e) {
			return;
		}

		ConcurrentHashMap<Integer, YarnResultSubPartitionFileInfo> subPartitionToFileInfo =
				resultPartitionMap.computeIfAbsent(resultPartitionID, ignored -> new ConcurrentHashMap<>());
		for (FileStatus partitionDirStatus : resultPartitionDirStatuses) {
			if (partitionDirStatus.isDir()) {
				continue;
			}
			Integer subPartitionIndex = ExternalBlockShuffleUtils.convertFileNameToSubPartitionIndex(
					partitionDirStatus.getPath().getName());
			if (subPartitionIndex != null) {

				YarnResultSubPartitionFileInfo fileInfo = subPartitionToFileInfo.get(subPartitionIndex);
				if (fileInfo != null) {
					fileInfo.updateFileInfoTimestamp(currTime);
				} else {
					String subPartitionFile = partitionDirStatus.getPath().getPath();

					fileInfo = new YarnResultSubPartitionFileInfo(
							appId,
							new Tuple2<>(rootDir, subPartitionFile),
							true,
							false,
							partitionDirStatus.getModificationTime(),
							currTime,
							shuffleServiceConfiguration);
					subPartitionToFileInfo.put(subPartitionIndex, fileInfo);
				}
			}
		}
	}

	@VisibleForTesting
	static String parseContainerExecutorExecutablePath(
		ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration,
		FileSystem fileSystem) {
		String yarnHomeEnvVar = System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
		File hadoopBin = new File(yarnHomeEnvVar, "bin");
		String defaultContainerExecutorPath = new File(hadoopBin, "container-executor").getAbsolutePath();
		String containerExecutorPath = shuffleServiceConfiguration.getConfiguration().getString(
			YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, defaultContainerExecutorPath);

		// Check whether container-executor file exists.
		try {
			if (fileSystem.exists(new Path(containerExecutorPath))) {
				LOG.info("Use container-executor to do recycling, file path: " + containerExecutorPath);
				return containerExecutorPath;
			}
		} catch (IOException e) {
			// Do nothing.
		}
		throw new IllegalArgumentException("Invalid container-executor configuration: " + containerExecutorPath);
	}

	/**
	 * According to container-executor's help message:
	 *     container-executor ${user} ${yarn-user} ${command} ${command-args}
	 *     where command and command-args:
	 *         delete as user:         3 relative-path
	 * Both user and yarn-user will use the user of the application.
	 *
	 * <p>Get user name by partition directory according to file structure arranged by NodeManager
	 * @see YarnLocalResultPartitionResolver#generateRelativeLocalAppDir , the format of a partition directory is:
	 * ${ConfiguredRootDir}/usercache/${user}/appcache/${appId}/partition_${producerId}_${partitionId} .
	 */
	@VisibleForTesting
	void removeResultSubPartition(Path partitionDir, String recycleReason, long lastActiveTime, boolean printLog) {
		try {
			String user = getRunAsUser(partitionDir.getParent().getParent().getParent().getName());
			Process process = Runtime.getRuntime().exec(new String[] {
				containerExecutorExecutablePath,
				user,
				user,
				"3",
				partitionDir.toString()
			});
			boolean processExit = process.waitFor(30, TimeUnit.SECONDS);
			if (processExit) {
				int exitCode = process.exitValue();
				if (exitCode != 0) {
					LOG.warn("Fail to delete partition's directory: {}, user: {}, reason: {}, "
							+ "lastActiveTime: {}, exitCode: {}.",
						partitionDir, user, recycleReason, lastActiveTime, exitCode);
				} else if (printLog) {
					LOG.info("Delete partition's directory: {}, user: {}, reason: {}, lastActiveTime: {}.",
						partitionDir, user, recycleReason, lastActiveTime);
				}
			} else {
				LOG.warn("Delete partition's directory for more than 30 seconds: {}, user: {}, reason: {}, "
					+ "lastActiveTime: {}.", partitionDir, user, recycleReason, lastActiveTime);
			}
		} catch (Exception e) {
			LOG.warn("Fail to delete partition's directory: {}, exception:", partitionDir, e);
		}
	}

	/**
	 * Get the user of the container process in Linux operating system.
	 * @param user The submitter of the appliation.
	 * @return The user of the container process.
	 * @see org.apache.hadoop.yarn.server.nodemanager LinuxContainerExecutor#getRunAsUser
	 */
	private String getRunAsUser(String user) {
		if (UserGroupInformation.isSecurityEnabled() ||
			!containerLimitUsers) {
			return user;
		} else {
			return nonsecureLocalUser;
		}
	}

	/**
	 * Hold the information of a result partition's files for searching and recycling.
	 */
	@VisibleForTesting
	static class YarnResultSubPartitionFileInfo implements ResultSubPartitionFileInfo {

		/** The application id of this result partition's producer. */
		private final String appId;

		/** Configured root dir and result partition's directory path. */
		private final Tuple2<String, String> rootDirAndPartitionDir;

		/** Whether the producer finishes generating this result partition. */
		private volatile boolean readyToBeConsumed;

		/** Whether this result partition has been consumed. */
		private volatile boolean consumed;

		/** When is the result partition ready to be fetched. */
		private volatile long partitionReadyTime;

		/** Timestamp of this information, used to do recycling if the partition's directory has been deleted. */
		private volatile long fileInfoTimestamp;

		/** TTL for consumed partitions, in milliseconds. */
		private volatile long consumedPartitionTTL;

		/** TTL for partial consumed partitions, in milliseconds. */
		private volatile long partialConsumedPartitionTTL;

		/** TTL for unconsumed partitions, in milliseconds. */
		private volatile long unconsumedPartitionTTL;

		YarnResultSubPartitionFileInfo(
			String appId,
			Tuple2<String, String> rootDirAndPartitionDir,
			boolean readyToBeConsumed,
			boolean consumed,
			long partitionReadyTime,
			long fileInfoTimestamp,
			ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration){

			this.appId = appId;
			this.rootDirAndPartitionDir = rootDirAndPartitionDir;
			this.readyToBeConsumed = readyToBeConsumed;
			this.consumed = consumed;
			this.partitionReadyTime = partitionReadyTime;
			this.fileInfoTimestamp = fileInfoTimestamp;
			this.consumedPartitionTTL = shuffleServiceConfiguration.getDefaultConsumedPartitionTTL();
			this.partialConsumedPartitionTTL = shuffleServiceConfiguration.getDefaultPartialConsumedPartitionTTL();
			this.unconsumedPartitionTTL = shuffleServiceConfiguration.getDefaultUnconsumedPartitionTTL();
		}

		String getAppId() {
			return appId;
		}

		Tuple2<String, String> getRootDirAndPartitionDir() {
			return rootDirAndPartitionDir;
		}

		boolean isReadyToBeConsumed() {
			return readyToBeConsumed;
		}

		boolean isConsumed() {
			return consumed;
		}

		long getPartitionReadyTime() {
			return partitionReadyTime;
		}

		long getFileInfoTimestamp() {
			return fileInfoTimestamp;
		}

		long getUnconsumedPartitionTTL() {
			return unconsumedPartitionTTL;
		}

		@Override
		public String getRootDir() {
			return rootDirAndPartitionDir.f0;
		}

		@Override
		public String getSubPartitionFile() {
			return rootDirAndPartitionDir.f1;
		}

		@Override
		public long getConsumedPartitionTTL() {
			return consumedPartitionTTL;
		}

		@Override
		public long getPartialConsumedPartitionTTL() {
			return partialConsumedPartitionTTL;
		}

		@Override
		public String getAbsolutePath() {
			return getRootDir() + "/" + getSubPartitionFile();
		}

		void setConsumed() {
			consumed = true;
		}

		void updateFileInfoTimestamp(long fileInfoTimestamp) {
			if (this.fileInfoTimestamp > 0) {
				this.fileInfoTimestamp = fileInfoTimestamp;
			}
		}

		void markToDelete() {
			this.fileInfoTimestamp = -1L;
		}

		void cancelDeletion() {
			this.fileInfoTimestamp = System.currentTimeMillis();
		}

		boolean needToDelete() {
			if (fileInfoTimestamp > 0) {
				return false;
			} else {
				return true;
			}
		}

		void updateOnConsumption() {
			if (needToDelete()) {
				cancelDeletion();
			}
			if (!isConsumed()) {
				setConsumed();
			}
		}
	}
}
