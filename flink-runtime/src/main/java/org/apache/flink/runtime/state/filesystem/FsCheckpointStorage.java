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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.GrafanaGauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of durable checkpoint storage to file systems.
 */
public class FsCheckpointStorage extends AbstractFsCheckpointStorage {
	private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointStorage.class);

	private final FileSystem fileSystem;

	private final Path checkpointsDirectory;

	private final Path sharedStateDirectory;

	private final Path taskOwnedStateDirectory;

	private final int fileSizeThreshold;

	private final int writeBufferSize;

	private boolean baseLocationsInitialized = false;

	private final CheckpointWriteFileStatistic metricReference;

	private final CheckpointWriteFileStatistic currentPeriodStatistic;

	private final boolean forceAbsolutePath;

	@VisibleForTesting
	public FsCheckpointStorage(
			Path checkpointBaseDirectory,
			@Nullable Path defaultSavepointDirectory,
			JobID jobId,
			int fileSizeThreshold,
			int writeBufferSize) throws IOException {
		this(checkpointBaseDirectory.getFileSystem(),
				checkpointBaseDirectory,
				defaultSavepointDirectory,
				jobId,
				fileSizeThreshold,
				writeBufferSize,
				new UnregisteredMetricsGroup());
	}

	public FsCheckpointStorage(
			Path checkpointBaseDirectory,
			@Nullable Path defaultSavepointDirectory,
			JobID jobId,
			int fileSizeThreshold,
			int writeBufferSize,
			MetricGroup metricGroup) throws IOException {

		this(checkpointBaseDirectory.getFileSystem(),
				checkpointBaseDirectory,
				defaultSavepointDirectory,
				jobId,
				fileSizeThreshold,
				writeBufferSize,
				metricGroup);
	}

	public FsCheckpointStorage(
			FileSystem fs,
			Path checkpointBaseDirectory,
			@Nullable Path defaultSavepointDirectory,
			JobID jobId,
			int fileSizeThreshold,
			int writeBufferSize,
			MetricGroup metricGroup) throws IOException {
		this(fs,
				checkpointBaseDirectory,
				defaultSavepointDirectory,
				jobId,
				null,
				null,
				fileSizeThreshold,
				writeBufferSize,
				metricGroup,
				false);
	}

	public FsCheckpointStorage(
			FileSystem fs,
			Path checkpointBaseDirectory,
			@Nullable Path defaultSavepointDirectory,
			JobID jobId,
			@Nullable String jobUID,
			@Nullable String checkpointsNamespace,
			int fileSizeThreshold,
			int writeBufferSize,
			MetricGroup metricGroup,
			boolean forceAbsolutePath) throws IOException {
		super(jobId, defaultSavepointDirectory);

		checkArgument(fileSizeThreshold >= 0);
		checkArgument(writeBufferSize >= 0);

		this.fileSystem = checkNotNull(fs);
		if (jobUID != null) {
			this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointBaseDirectory, jobUID, checkpointsNamespace);
		} else {
			this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointBaseDirectory, jobId);
		}
		this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
		this.taskOwnedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
		this.fileSizeThreshold = fileSizeThreshold;
		this.writeBufferSize = writeBufferSize;
		this.forceAbsolutePath = forceAbsolutePath;

		this.metricReference = new CheckpointWriteFileStatistic();
		this.currentPeriodStatistic = new CheckpointWriteFileStatistic();

		metricGroup.gauge(CHECKPOINT_WRITE_FILE_RATE_METRIC, new CheckpointWriteFileRate(metricReference, currentPeriodStatistic));
		metricGroup.gauge(CHECKPOINT_WRITE_FILE_LATENCY_METRIC, (GrafanaGauge<Double>) metricReference::getAvgWriteLatency);
		metricGroup.gauge(CHECKPOINT_CLOSE_FILE_LATENCY_METRIC, (GrafanaGauge<Double>) metricReference::getAvgCloseLatency);
	}

	// ------------------------------------------------------------------------

	@Override
	public Path getCheckpointsDirectory() {
		return checkpointsDirectory;
	}

	// ------------------------------------------------------------------------
	//  CheckpointStorage implementation
	// ------------------------------------------------------------------------

	@Override
	public boolean supportsHighlyAvailableStorage() {
		return true;
	}

	@Override
	public void initializeBaseLocations() throws IOException {
		fileSystem.mkdirs(sharedStateDirectory);
		fileSystem.mkdirs(taskOwnedStateDirectory);
		baseLocationsInitialized = true;
	}

	@Override
	public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) throws IOException {
		if (!baseLocationsInitialized) {
			initializeBaseLocations();
		}

		checkArgument(checkpointId >= 0, "Illegal negative checkpoint id: %d.", checkpointId);
		checkArgument(baseLocationsInitialized, "The base checkpoint location has not been initialized.");

		// prepare all the paths needed for the checkpoints
		final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

		// create the checkpoint exclusive directory
		fileSystem.mkdirs(checkpointDir);

		return new FsCheckpointStorageLocation(
				fileSystem,
				checkpointDir,
				sharedStateDirectory,
				taskOwnedStateDirectory,
				CheckpointStorageLocationReference.getDefault(),
				fileSizeThreshold,
				writeBufferSize,
				currentPeriodStatistic,
				forceAbsolutePath);
	}

	@Override
	public CheckpointStorageLocation initializeLocationForSavepointMetaInCheckpointDir(long checkpointId) throws IOException {
		if (!baseLocationsInitialized) {
			initializeBaseLocations();
		}

		checkArgument(checkpointId >= 0, "Illegal negative checkpoint id: %d.", checkpointId);
		checkArgument(baseLocationsInitialized, "The base checkpoint location has not been initialized.");

		// prepare all the paths needed for the checkpoints
		final Path savepointDir = new Path(checkpointsDirectory, SAVEPOINT_DIR_PREFIX + checkpointId);

		// create the checkpoint exclusive directory
		fileSystem.mkdirs(savepointDir);

		return new FsCheckpointStorageLocation(
			fileSystem,
			savepointDir,
			savepointDir,
			savepointDir,
			CheckpointStorageLocationReference.getDefault(),
			fileSizeThreshold,
			writeBufferSize,
			currentPeriodStatistic,
			forceAbsolutePath);
	}

	@Override
	public void removeSavepointSimpleMetadataPathInCheckpointDir(long checkpointId) throws IOException {
		checkArgument(checkpointId >= 0, "Illegal negative checkpoint id: %d.", checkpointId);
		checkArgument(baseLocationsInitialized, "The base checkpoint location has not been initialized.");

		// prepare all the paths needed for the checkpoints
		final Path savepointDir = new Path(checkpointsDirectory, SAVEPOINT_DIR_PREFIX + checkpointId);
		if (fileSystem.exists(savepointDir)) {
			fileSystem.delete(savepointDir, true);
			LOG.info("Expired savepoint {}'s savepoint simple metadata in {}", checkpointId, savepointDir);
		} else {
			LOG.warn("Cannot find savepoint simple metadata path {}", savepointDir);
		}
	}

	@Override
	public CheckpointStreamFactory resolveCheckpointStorageLocation(
			long checkpointId,
			CheckpointStorageLocationReference reference) throws IOException {

		if (reference.isDefaultReference()) {
			// default reference, construct the default location for that particular checkpoint
			final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

			return new FsCheckpointStorageLocation(
					fileSystem,
					checkpointDir,
					sharedStateDirectory,
					taskOwnedStateDirectory,
					reference,
					fileSizeThreshold,
					writeBufferSize,
					currentPeriodStatistic,
					forceAbsolutePath);
		}
		else {
			// location encoded in the reference
			final Path path = decodePathFromReference(reference);

			return new FsCheckpointStorageLocation(
					path.getFileSystem(),
					path,
					path,
					path,
					reference,
					fileSizeThreshold,
					writeBufferSize,
					currentPeriodStatistic,
					forceAbsolutePath);
		}
	}

	@Override
	public CheckpointStateOutputStream createTaskOwnedStateStream() {
		// as the comment of CheckpointStorageWorkerView#createTaskOwnedStateStream said we may change into shared state,
		// so we use CheckpointedStateScope.SHARED here.
		return new FsCheckpointStateOutputStream(
				taskOwnedStateDirectory,
				fileSystem,
				writeBufferSize,
				fileSizeThreshold,
				false,
				currentPeriodStatistic);
	}

	@Override
	protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) {
		final CheckpointStorageLocationReference reference = encodePathAsReference(location);
		return new FsCheckpointStorageLocation(fs, location, location, location, reference, fileSizeThreshold, writeBufferSize, currentPeriodStatistic, forceAbsolutePath);
	}

	// ------------------------------------------------------------------------
	// Metrics
	// ------------------------------------------------------------------------

	private static final String CHECKPOINT_WRITE_FILE_RATE_METRIC = "checkpointWriteFileRate";

	private static final String CHECKPOINT_WRITE_FILE_LATENCY_METRIC = "checkpointWriteFileLatency";

	private static final String CHECKPOINT_CLOSE_FILE_LATENCY_METRIC = "checkpointCloseFileLatency";

	/**
	 * Metric for write hdfs file.
	 */
	public static class CheckpointWriteFileStatistic {
		private final Object lock = new Object();
		private final long timeSpanInSeconds;

		private long writeBytes = 0;
		private long writeLatency = 0;
		private long writeCount = 0;
		private long closeLatency = 0;
		private long closeCount = 0;

		public CheckpointWriteFileStatistic() {
			this(CheckpointWriteFileRate.DEFAULT_TIME_SPAN_IN_SECONDS);
		}

		public CheckpointWriteFileStatistic(long timeSpanInSeconds) {
			this.timeSpanInSeconds = timeSpanInSeconds;
		}

		public void updateWriteStatistics(long writeBytes, long writeLatency, long writeCount) {
			synchronized (lock) {
				this.writeBytes += writeBytes;
				this.writeLatency += writeLatency;
				this.writeCount += writeCount;
			}
		}

		public void updateCloseStatistics(long closeLatency) {
			synchronized (lock) {
				this.closeLatency += closeLatency;
				this.closeCount += 1;
			}
		}

		public void updateAllStatistics(
				long writeBytes,
				long writeLatency,
				long writeCount,
				long closeLatency,
				long closeCount) {
			synchronized (lock) {
				this.writeBytes += writeBytes;
				this.writeCount += writeCount;
				this.writeLatency += writeLatency;
				this.closeLatency += closeLatency;
				this.closeCount += closeCount;
			}
		}

		public CheckpointWriteFileStatistic getAndResetStatistics() {
			synchronized (lock) {
				CheckpointWriteFileStatistic statistic = new CheckpointWriteFileStatistic(this.timeSpanInSeconds);
				statistic.updateAllStatistics(this.writeBytes, this.writeLatency, this.writeCount, this.closeLatency, this.closeCount);
				this.writeBytes = 0L;
				this.writeCount = 0L;
				this.writeLatency = 0L;
				this.closeLatency = 0L;
				this.closeCount = 0L;
				return statistic;
			}
		}

		public double getWriteRate() {
			synchronized (lock) {
				return ((double) writeBytes) / timeSpanInSeconds;
			}
		}

		public double getAvgWriteLatency() {
			synchronized (lock) {
				return writeCount > 0 ? ((double) writeLatency / writeCount) : 0.0;
			}
		}

		public double getAvgCloseLatency() {
			synchronized (lock) {
				return closeCount > 0 ? ((double) closeLatency / closeCount) : 0.0;
			}
		}

		@Override
		public String toString() {
			return "CheckpointWriteFileStatistic{" +
				"timeSpanInSeconds=" + timeSpanInSeconds +
				", writeBytes=" + writeBytes +
				", writeLatency=" + writeLatency +
				", writeCount=" + writeCount +
				", closeLatency=" + closeLatency +
				", closeCount=" + closeCount +
				'}';
		}
	}

	private static class CheckpointWriteFileRate implements GrafanaGauge<Double>, View {
		public static final int DEFAULT_TIME_SPAN_IN_SECONDS = 60;

		/** The time-span over which the average is calculated. */
		private final int timeSpanInSeconds;
		/** Metric reference. */
		private final CheckpointWriteFileStatistic metricReference;
		/** Current period statistic reference. */
		private final CheckpointWriteFileStatistic currentPeriodStatistic;
		/** Circular array containing the history of values. */
		private final CheckpointWriteFileStatistic[] values;
		/** The index in the array for the current time. */
		private int index = 0;

		public CheckpointWriteFileRate(
				CheckpointWriteFileStatistic metricReference,
				CheckpointWriteFileStatistic currentPeriodStatistic) {
			this.timeSpanInSeconds = DEFAULT_TIME_SPAN_IN_SECONDS;
			this.metricReference = metricReference;
			this.currentPeriodStatistic = currentPeriodStatistic;
			this.values = new CheckpointWriteFileStatistic[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
		}

		@Override
		public Double getValue() {
			return metricReference.getWriteRate();
		}

		@Override
		public void update() {
			index = (index + 1) % values.length;
			CheckpointWriteFileStatistic old = values[index];
			values[index] = currentPeriodStatistic.getAndResetStatistics();
			if (old == null) {
				old = new CheckpointWriteFileStatistic(this.timeSpanInSeconds);
			}
			metricReference.updateAllStatistics(
				values[index].writeBytes - old.writeBytes,
				values[index].writeLatency - old.writeLatency,
				values[index].writeCount - old.writeCount,
				values[index].closeLatency - old.closeLatency,
				values[index].closeCount - old.closeCount);
		}
	}

	@Override
	public List<String> findCompletedCheckpointPointer() throws IOException {
		FileStatus[] statuses = fileSystem.listStatus(checkpointsDirectory);
		if (statuses == null) {
			return Collections.emptyList();
		}
		return Arrays.stream(statuses)
				.filter(fileStatus -> {
					try {
						return fileStatus.getPath().getName().startsWith(CHECKPOINT_DIR_PREFIX)
								&& fileSystem.exists(new Path(fileStatus.getPath(), METADATA_FILE_NAME));
					} catch (IOException e) {
						LOG.info("Exception when checking {} is completed checkpoint.", fileStatus.getPath(), e);
						return false;
					}
				})
				.sorted(Comparator.comparingInt(
						(FileStatus fileStatus) -> {
							try {
								return Integer.parseInt(
										fileStatus.getPath().getName().substring(CHECKPOINT_DIR_PREFIX.length()));
							} catch (Exception e) {
								LOG.info("Exception when parsing checkpoint {} id.", fileStatus.getPath(), e);
								return Integer.MIN_VALUE;
							}
						}).reversed())
				.map(fileStatus -> fileStatus.getPath().toString())
				.collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Tuple3<Long, String, Boolean>> findCompletedCheckpointPointerV2(Set<Long> checkpointsOnStore) throws IOException {
		LOG.info("Find completed checkpoint in checkpointsDirectory: {}", checkpointsDirectory);
		FileStatus[] statuses = fileSystem.listStatus(checkpointsDirectory);
		if (statuses == null) {
			return Collections.emptyList();
		}

		long latestCheckpointIdOnStore = checkpointsOnStore.stream().max(Long::compareTo).orElse(-1L);
		return Arrays.stream(statuses)
			.filter(fileStatus -> {
				try {
					String dirName = fileStatus.getPath().getName();
					long checkpointId;
					if (dirName.startsWith(CHECKPOINT_DIR_PREFIX)) {
						checkpointId = Long.parseLong(dirName.substring(CHECKPOINT_DIR_PREFIX.length()));
						return (checkpointId > latestCheckpointIdOnStore || checkpointsOnStore.contains(checkpointId)) &&
							fileSystem.exists(new Path(fileStatus.getPath(), METADATA_FILE_NAME));
					} else if (dirName.startsWith(SAVEPOINT_DIR_PREFIX)) {
						checkpointId = Long.parseLong(dirName.substring(SAVEPOINT_DIR_PREFIX.length()));
						return (checkpointId > latestCheckpointIdOnStore || checkpointsOnStore.contains(checkpointId)) &&
							fileSystem.exists(new Path(fileStatus.getPath(), METADATA_FILE_NAME));
					} else {
						return false;
					}
				} catch (IOException e) {
					LOG.info("Exception when checking {} is completed checkpoint.", fileStatus.getPath(), e);
					return false;
				} catch (NumberFormatException e) {
					LOG.info("Exception when parsing checkpoint {} id.", fileStatus.getPath(), e);
					return false;
				}
			}).map(fileStatus -> {
				boolean isSavepoint = false;
				long checkpointId;
				String dirName = fileStatus.getPath().getName();
				if (dirName.startsWith(CHECKPOINT_DIR_PREFIX)) {
					checkpointId = Long.parseLong(dirName.substring(CHECKPOINT_DIR_PREFIX.length()));
				} else {
					isSavepoint = true;
					checkpointId = Long.parseLong(dirName.substring(SAVEPOINT_DIR_PREFIX.length()));
				}
				return new Tuple3<>(checkpointId, fileStatus.getPath().toString(), isSavepoint);
			}).sorted(Comparator.comparingLong(
				(Tuple3<Long, String, Boolean> value) -> value.f0).reversed())
			.collect(Collectors.toList());
	}

	@Override
	public List<Tuple2<String, Boolean>> findCompletedCheckpointPointerForCrossVersion() throws IOException {
		FileStatus[] currVersionStatuses = fileSystem.listStatus(checkpointsDirectory);

		final Path newCheckpointPath = new Path(checkpointsDirectory.getPath()
			.replaceFirst("1.11", "1.9")
			.replaceFirst("byte_flink_checkpoint_20210220", "byte_flink_checkpoint"));

		if (!fileSystem.exists(newCheckpointPath)) {
			fileSystem.mkdirs(newCheckpointPath);
		}
		FileStatus[] newStatuses = fileSystem.listStatus(newCheckpointPath);

		FileStatus[] statuses = new FileStatus[newStatuses.length + currVersionStatuses.length];
		System.arraycopy(currVersionStatuses, 0, statuses, 0, currVersionStatuses.length);
		System.arraycopy(newStatuses, 0, statuses, currVersionStatuses.length, newStatuses.length);

		return Arrays.stream(statuses)
			.filter(fileStatus -> {
				try {
					return (fileStatus.getPath().getName().startsWith(CHECKPOINT_DIR_PREFIX) || fileStatus.getPath().getName().startsWith(SAVEPOINT_DIR_PREFIX))
						&& fileSystem.exists(new Path(fileStatus.getPath(), METADATA_FILE_NAME));
				} catch (IOException e) {
					LOG.info("Exception when checking {} is completed checkpoint.", fileStatus.getPath(), e);
					return false;
				}
			})
			.sorted(Comparator.comparingInt(
				(FileStatus fileStatus) -> {
					try {
						int checkpointId;
						String dirName = fileStatus.getPath().getName();
						if (dirName.startsWith(CHECKPOINT_DIR_PREFIX)) {
							checkpointId = Integer.parseInt(dirName.substring(CHECKPOINT_DIR_PREFIX.length()));
						} else {
							checkpointId = Integer.parseInt(dirName.substring(SAVEPOINT_DIR_PREFIX.length()));
						}
						return checkpointId;
					} catch (Exception e) {
						LOG.info("Exception when parsing checkpoint {} id.", fileStatus.getPath(), e);
						return Integer.MIN_VALUE;
					}
				}).reversed())
			.map(fileStatus -> {
				String dirName = fileStatus.getPath().getName();
				if (dirName.startsWith(CHECKPOINT_DIR_PREFIX)) {
					return new Tuple2<>(fileStatus.getPath().toString(), false);
				} else {
					return new Tuple2<>(fileStatus.getPath().toString(), true);
				}
			})
			.collect(Collectors.toList());
	}

	@Override
	public void clearAllCheckpointPointers() throws IOException {
		fileSystem.delete(checkpointsDirectory, true);
		LOG.info("Checkpoints Directory {} deleted.", checkpointsDirectory);
	}

	@Override
	public void clearCheckpointPointers(int checkpointID) throws IOException {
		Path checkpointPath = new Path(checkpointsDirectory, CHECKPOINT_DIR_PREFIX + checkpointID);
		if (fileSystem.exists(checkpointPath)) {
			LOG.info("Trying to delete checkpoint {}.", checkpointPath);
			Path metaFilePath = new Path(checkpointPath, METADATA_FILE_NAME);
			if (fileSystem.exists(metaFilePath)) {
				fileSystem.delete(metaFilePath, false);
			}
			fileSystem.delete(checkpointPath, true);
		} else {
			LOG.info("Checkpoint path {} not found.", checkpointPath);
		}
	}

	@Override
	public void renameNamespaceDirectory(String targetName) throws IOException {
		fileSystem.rename(checkpointsDirectory, new Path(checkpointsDirectory.getParent(), targetName));
	}

	@SuppressWarnings("checkstyle:EmptyStatement")
	@Override
	public Tuple2<String, Boolean> findLatestSnapshotCrossNamespaces(int maxLatestNamespaceTracing, String namespace) throws IOException {
		List<Path> modifySortedDirs;
		FileStatus[] namespaceStatuses = fileSystem.listStatus(checkpointsDirectory.getParent());
		modifySortedDirs = Arrays.stream(namespaceStatuses)
			.filter(fileStatus -> fileStatus.isDir())
			.sorted(Comparator.comparing(FileStatus::getModificationTime, Comparator.reverseOrder()))
			.limit(maxLatestNamespaceTracing)
			.map(fileStatus -> fileStatus.getPath())
			.collect(Collectors.toList());
		LOG.info("Got {} existing namespace paths for job", modifySortedDirs.size());
		int sizeCompletedSnapshotInChkDir = findCompletedCheckpointPointerV2(Collections.emptySet()).size();
		String latestSnapshotPath = null;
		boolean isSavepoint = false;
		for (Path checkpointDir : modifySortedDirs) {
			FileStatus[] checkpointStatuses = fileSystem.listStatus(checkpointDir);
			if (checkpointStatuses == null || checkpointStatuses.length == 0) {
				continue;
			}
			LOG.info("Scanning namespace path for latest snapshot: {}, there are {} checkpoint dirs in it.", checkpointDir.getPath(), checkpointStatuses.length);
			Arrays.sort(checkpointStatuses, new Comparator<FileStatus>() {
				@Override
				public int compare(FileStatus f1, FileStatus f2) {
					return Long.compare(getCheckpointIDFromFileStatus(f2), getCheckpointIDFromFileStatus(f1)); // reverse the checkpointID
				}
			});
			boolean foundLatest = false;
			for (FileStatus fileStatus : checkpointStatuses) {
				Path dirPath = fileStatus.getPath();
				String dirName = dirPath.getName();
				if ((dirName.startsWith(CHECKPOINT_DIR_PREFIX) || dirName.startsWith(SAVEPOINT_DIR_PREFIX))
					&& fileSystem.exists(new Path(dirPath, METADATA_FILE_NAME))) {
					if (dirName.startsWith(SAVEPOINT_DIR_PREFIX) && checkValidSavepointMetadata(dirPath.toString())) {
						foundLatest = true;
						latestSnapshotPath = dirPath.toString();
						isSavepoint = true;
						break;
					} else if (dirName.startsWith(CHECKPOINT_DIR_PREFIX) && checkValidCheckpointMetadata(dirPath.toString())) {
						foundLatest = true;
						latestSnapshotPath = dirPath.toString();
						isSavepoint = false;
						break;
					}
				}
			}
			if (foundLatest) {
				break;
			}
		}
		verifyRestoringFromLatest(latestSnapshotPath, namespace, sizeCompletedSnapshotInChkDir);
		if (isSavepoint) {
			try {
				latestSnapshotPath = resolveSavepoint(latestSnapshotPath).getExternalPointer();
			} catch (IOException e) {
				LOG.error("Failed to find actual savepoint path for {}", latestSnapshotPath);
				throw e;
			}
		}
		boolean needResetSavepointSettings = sizeCompletedSnapshotInChkDir == 0;
		LOG.info("latest snapshot path: {}, needResetSavepointSettings: {}", latestSnapshotPath, needResetSavepointSettings);
		return new Tuple2<>(latestSnapshotPath, needResetSavepointSettings);
	}

	private boolean checkValidCheckpointMetadata(String path) {
		try {
			CompletedCheckpointStorageLocation location = resolveCheckpoint(path);
			try (DataInputStream stream = new DataInputStream(location.getMetadataHandle().openInputStream())) {
				Checkpoints.loadCheckpointMetadata(stream, Thread.currentThread().getContextClassLoader(), location.getExternalPointer());
			}
			return true;
		} catch (Exception e) {
			LOG.info("Invalid checkpoint {}, skip it", path);
			return false;
		}
	}

	private boolean checkValidSavepointMetadata(String path) {
		try {
			CompletedCheckpointStorageLocation location = resolveSavepoint(path);
			try (DataInputStream stream = new DataInputStream(location.getMetadataHandle().openInputStream())) {
				Checkpoints.loadCheckpointMetadata(stream, Thread.currentThread().getContextClassLoader(), location.getExternalPointer());
			}
			return true;
		} catch (Exception e) {
			LOG.info("Invalid savepoint {}, skip it", path);
			return false;
		}
	}

	private void verifyRestoringFromLatest(String latestSnapshotPath, String namespace, int sizeCompletedSnapshotInChkDir) throws IllegalStateException {
		if (latestSnapshotPath == null) {
			throw new IllegalStateException("Can't find any completed snapshot. " +
				"Maybe the job has never succeeded making a completed snapshot, or the latest completed snapshot is too far from now. " +
				"Please switch to 'restoring without states' manually.");
		}
		if (sizeCompletedSnapshotInChkDir > 0 && !latestSnapshotPath.contains(namespace)) {
			throw new IllegalStateException(String.format("There is completed snapshot in namespace %s, but got latest completed snapshot at %s.", namespace, latestSnapshotPath));
		}
	}

	private long getCheckpointIDFromFileStatus(FileStatus fileStatus) {
		long checkpointId;
		try {
			String dirName = fileStatus.getPath().getName();
			if (dirName.startsWith(CHECKPOINT_DIR_PREFIX)) {
				checkpointId = Long.parseLong(dirName.substring(CHECKPOINT_DIR_PREFIX.length()));
			} else if (dirName.startsWith(SAVEPOINT_DIR_PREFIX)) {
				checkpointId = Long.parseLong(dirName.substring(SAVEPOINT_DIR_PREFIX.length()));
			} else {
				checkpointId = Long.MIN_VALUE;
			}
		} catch (Exception e) {
			LOG.info("Exception when parsing checkpoint {} id.", fileStatus.getPath(), e);
			return Long.MIN_VALUE;
		}
		return checkpointId;
	}

	@Override
	public List<FileStatus> listCheckpointPointers() throws IOException {
		FileStatus[] statuses = fileSystem.listStatus(checkpointsDirectory);
		if (statuses == null) {
			return Collections.emptyList();
		}
		return Arrays.stream(statuses)
			.filter(fileStatus -> fileStatus.getPath().getName().startsWith(CHECKPOINT_DIR_PREFIX))
			.collect(Collectors.toList());
	}
}
