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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * The placeholder of the {@link CompletedCheckpoint}.
 * We use {@code transformer} to convert {@code actualState} to real {@link CompletedCheckpoint}.
 */
public class CompletedCheckpointPlaceHolder<T extends Serializable> extends CompletedCheckpoint implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(CompletedCheckpointPlaceHolder.class);

	private static final long serialVersionUID = -1L;

	private static final JobID DUMMY_JOB_ID = new JobID(1234L, 4321L);

	private static final DummyCheckpointStorageLocation DUMMY_CHECKPOINT_STORAGE_LOCATION = new DummyCheckpointStorageLocation();

	private final T actualState;

	private transient FunctionWithException<T, CompletedCheckpoint, Exception> transformer;

	private TernaryBoolean isSavepoint = TernaryBoolean.UNDEFINED;

	private transient SharedStateRegistry sharedStateRegistry;

	private transient CompletedCheckpoint completedCheckpoint;

	private transient boolean hasAppendTransformer = false;

	private transient BiConsumer<Boolean, String> transformCallback;

	public CompletedCheckpointPlaceHolder(
			long checkpointId,
			T actualState,
			FunctionWithException<T, CompletedCheckpoint, Exception> transformer) {
		this(checkpointId, DUMMY_CHECKPOINT_STORAGE_LOCATION, TernaryBoolean.UNDEFINED, actualState, transformer);
	}

	public CompletedCheckpointPlaceHolder(
			long checkpointId,
			CompletedCheckpointStorageLocation storageLocation,
			boolean isSavepoint,
			T actualState,
			FunctionWithException<T, CompletedCheckpoint, Exception> transformer) {
		this(checkpointId, storageLocation, TernaryBoolean.fromBoolean(isSavepoint), actualState, transformer);
	}

	public CompletedCheckpointPlaceHolder(
			long checkpointId,
			CompletedCheckpointStorageLocation storageLocation,
			TernaryBoolean isSavepoint,
			T actualState,
			FunctionWithException<T, CompletedCheckpoint, Exception> transformer) {
		super(DUMMY_JOB_ID,
			checkpointId,
			0L,
			0L,
			Collections.emptyMap(),
			null,
			CheckpointProperties.forSavepoint(false),
			storageLocation);
		this.isSavepoint = isSavepoint;
		this.actualState = actualState;
		this.transformer = transformer;
	}

	@Override
	public long getTimestamp() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public long getDuration() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public CheckpointProperties getProperties() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public Map<OperatorID, OperatorState> getOperatorStates() {
		try {
			transformPlaceHolder();
			return completedCheckpoint.getOperatorStates();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Can not transform placeHolder", e);
		}
	}

	@Override
	public Collection<MasterState> getMasterHookStates() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public StreamStateHandle getMetadataHandle() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public String getExternalPointer() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public long getStateSize() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public long getTotalStateSize() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public long getRawTotalStateSize() {
		throw new UnsupportedOperationException("Should never be called in placeHolder.");
	}

	@Override
	public void registerSharedStatesAfterRestored(SharedStateRegistry sharedStateRegistry) {
		this.sharedStateRegistry = sharedStateRegistry;
	}

	@Override
	public void discardOnFailedStoring() throws Exception {
		transformPlaceHolder();
		completedCheckpoint.discardOnFailedStoring();
	}

	@Override
	public boolean discardOnSubsume() throws Exception {
		transformPlaceHolder();
		return completedCheckpoint.discardOnSubsume();
	}

	@Override
	public boolean discardOnShutdown(JobStatus jobStatus) throws Exception {
		transformPlaceHolder();
		return completedCheckpoint.discardOnShutdown(jobStatus);
	}

	@Override
	public boolean isDiscardOnShutdown(JobStatus jobStatus) {
		try {
			transformPlaceHolder();
			return completedCheckpoint.isDiscardOnShutdown(jobStatus);
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public boolean isSavepoint() {
		return isSavepoint != TernaryBoolean.UNDEFINED && isSavepoint.getAsBoolean();
	}

	@Override
	public boolean isCheckpoint() {
		return isSavepoint != TernaryBoolean.UNDEFINED && !isSavepoint.getAsBoolean();
	}

	public TernaryBoolean getIsSavepoint() {
		return isSavepoint;
	}

	public void setIsSavepoint(boolean isSavepoint) {
		this.isSavepoint = TernaryBoolean.fromBoolean(isSavepoint);
	}

	public void setTransformCallback(BiConsumer<Boolean, String> transformCallback) {
		this.transformCallback = transformCallback;
	}

	@Override
	void setDiscardCallback(@Nullable CompletedCheckpointStats.DiscardCallback discardCallback) {
		throw new UnsupportedOperationException("unsupported in placeHolder");
	}

	public void appendTransformer(
		FunctionWithException<CompletedCheckpointPlaceHolder<?>, CompletedCheckpoint, Exception> appendTransformer) {
		if (!hasAppendTransformer) {
			final FunctionWithException<T, CompletedCheckpoint, Exception> previousTransformer = this.transformer;
			this.transformer = v -> {
				CompletedCheckpoint underlyingCheckpoint = previousTransformer.apply(v);
				if (underlyingCheckpoint instanceof CompletedCheckpointPlaceHolder) {
					underlyingCheckpoint = appendTransformer.apply((CompletedCheckpointPlaceHolder<?>) underlyingCheckpoint);
				}
				return underlyingCheckpoint;
			};
			hasAppendTransformer = true;
		}
	}

	private synchronized void transformPlaceHolder() throws Exception {
		try {
			if (completedCheckpoint == null) {
				completedCheckpoint = transformer.apply(actualState);
				Preconditions.checkNotNull(completedCheckpoint);
				LOG.info("transform placeholder for checkpoint[{}] success", getCheckpointID());
				completedCheckpoint.setCheckpointStorage(getCheckpointStorage());
				if (sharedStateRegistry != null) {
					completedCheckpoint.registerSharedStatesAfterRestored(sharedStateRegistry);
				}
				if (transformCallback != null) {
					transformCallback.accept(true, "success");
				}
			}
		} catch (Exception e) {
			LOG.warn("transform placeholder for checkpoint[{}] failed", getCheckpointID(), e);
			if (transformCallback != null) {
				transformCallback.accept(false, e.toString());
			}
			throw e;
		}
	}

	public CompletedCheckpoint getUnderlyingCheckpoint() throws Exception {
		transformPlaceHolder();
		return completedCheckpoint;
	}

	@Override
	public String toString() {
		return "CompletedCheckpointPlaceHolder{" +
			"actualState=" + actualState +
			'}';
	}

	private static final class DummyCheckpointStorageLocation implements CompletedCheckpointStorageLocation {

		private static final long serialVersionUID = -1L;

		@Override
		public String getExternalPointer() {
			return null;
		}

		@Override
		public StreamStateHandle getMetadataHandle() {
			return null;
		}

		@Override
		public void disposeStorageLocation() throws IOException {

		}
	}
}
