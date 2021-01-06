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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
				writeBufferSize);
	}

	public FsCheckpointStorage(
			FileSystem fs,
			Path checkpointBaseDirectory,
			@Nullable Path defaultSavepointDirectory,
			JobID jobId,
			int fileSizeThreshold,
			int writeBufferSize) throws IOException {
		this(fs,
				checkpointBaseDirectory,
				defaultSavepointDirectory,
				jobId,
				null,
				null,
				fileSizeThreshold,
				writeBufferSize);
	}

	public FsCheckpointStorage(
			FileSystem fs,
			Path checkpointBaseDirectory,
			@Nullable Path defaultSavepointDirectory,
			JobID jobId,
			@Nullable String jobName,
			@Nullable String checkpointsNamespace,
			int fileSizeThreshold,
			int writeBufferSize) throws IOException {
		super(jobId, defaultSavepointDirectory);

		checkArgument(fileSizeThreshold >= 0);
		checkArgument(writeBufferSize >= 0);

		this.fileSystem = checkNotNull(fs);
		if (jobName != null) {
			this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointBaseDirectory, jobName, checkpointsNamespace);
		} else {
			this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointBaseDirectory, jobId);
		}
		this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
		this.taskOwnedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
		this.fileSizeThreshold = fileSizeThreshold;
		this.writeBufferSize = writeBufferSize;
	}

	// ------------------------------------------------------------------------

	@VisibleForTesting
	Path getCheckpointsDirectory() {
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
				writeBufferSize);
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
					writeBufferSize);
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
					writeBufferSize);
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
				fileSizeThreshold);
	}

	@Override
	protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) {
		final CheckpointStorageLocationReference reference = encodePathAsReference(location);
		return new FsCheckpointStorageLocation(fs, location, location, location, reference, fileSizeThreshold, writeBufferSize);
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
}
