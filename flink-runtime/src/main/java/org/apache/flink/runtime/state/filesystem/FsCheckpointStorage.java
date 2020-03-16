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

import org.apache.flink.api.common.JobID;
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
import java.util.Comparator;

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

	@Override
	public void initializeLocation() throws IOException {
		// initialize the dedicated directories
		fileSystem.mkdirs(checkpointsDirectory);
		fileSystem.mkdirs(sharedStateDirectory);
		fileSystem.mkdirs(taskOwnedStateDirectory);
	}

	// ------------------------------------------------------------------------

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
	public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) throws IOException {
		checkArgument(checkpointId >= 0);

		// prepare all the paths needed for the checkpoints
		final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

		if (fileSystem.exists(checkpointDir)) {
			throw new IOException("checkpoint dir " +  checkpointDir + " already exists.");
		}
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
	public String findLatestCompletedCheckpointPointer() throws IOException {
		return Arrays.stream(fileSystem.listStatus(checkpointsDirectory))
				.filter(fileStatus -> {
					try {
						return fileStatus.getPath().getName().startsWith(CHECKPOINT_DIR_PREFIX)
								&& fileSystem.exists(new Path(fileStatus.getPath(), METADATA_FILE_NAME));
					} catch (IOException e) {
						LOG.info("Exception when checking {} is completed checkpoint.", fileStatus.getPath(), e);
						return false;
					}
				})
				.max(Comparator.comparingInt(fileStatus -> {
					try {
						return Integer.parseInt(
								fileStatus.getPath().getName().substring(CHECKPOINT_DIR_PREFIX.length()));
					} catch (Exception e) {
						LOG.info("Exception when parsing checkpoint {} id.", fileStatus.getPath(), e);
						return Integer.MIN_VALUE;
					}
				}))
				.map(fileStatus -> fileStatus.getPath().toString())
				.orElse(null);
	}

	@Override
	public long getCheckpointIDFromExternalPointer(String externalPointer) {
		Path externalPath = new Path(externalPointer);
		if (externalPath.getName().startsWith(CHECKPOINT_DIR_PREFIX)) {
			return Long.parseLong(externalPath.getName().substring(CHECKPOINT_DIR_PREFIX.length()));
		}
		return -1;
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
}
