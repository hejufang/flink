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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.tracker.NonStateStatsTracker;
import org.apache.flink.runtime.state.tracker.StateStatsTracker;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

/**
 * By wrapping the actually used {@link StateBackend}, almost all methods are
 * delegated to the actual StateBackend, except for the creation of
 * {@link org.apache.flink.runtime.state.KeyedStateBackend}. We will continue
 * to nest the corresponding keyedStateBackend when constructing the {@link CachedKeyedStateBackend},
 * and encapsulate the cache in the CachedState when creating the state.
 */
public class CachedStateBackend extends AbstractStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(CachedStateBackend.class);

	/** The manager responsible for managing the Cache. */
	private final CacheManager cacheManager;

	/** StateBackend proxied by the cache. */
	private final StateBackend delegateStateBackend;

	/** The configuration of the cache. */
	private final CacheConfiguration configuration;

	public CachedStateBackend(
			CacheManager cacheManager,
			StateBackend delegateStateBackend,
			CacheConfiguration configuration) {
		this.cacheManager = cacheManager;
		this.delegateStateBackend = delegateStateBackend;
		this.configuration = configuration;
	}

	// ------------------------------------------------------------------------
	//  Checkpoint initialization and persistent storage
	// ------------------------------------------------------------------------

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
		return delegateStateBackend.resolveCheckpoint(externalPointer);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		return delegateStateBackend.createCheckpointStorage(jobId);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId, String jobUID) throws IOException {
		return delegateStateBackend.createCheckpointStorage(jobId, jobUID);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId, @Nullable String jobUID, MetricGroup metricGroup) throws IOException {
		return delegateStateBackend.createCheckpointStorage(jobId, jobUID, metricGroup);
	}

	// ------------------------------------------------------------------------
	//  State holding data structures
	// ------------------------------------------------------------------------

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry,
			TtlTimeProvider ttlTimeProvider,
			MetricGroup metricGroup,
			@Nullable Collection<KeyedStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) throws IOException {
		return createKeyedStateBackend(
				env,
				jobID,
				operatorIdentifier,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				kvStateRegistry,
				ttlTimeProvider,
				metricGroup,
				stateHandles,
				cancelStreamRegistry,
				new NonStateStatsTracker(),
				false);
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry,
		StateStatsTracker statsTracker,
		boolean crossNamespace) throws IOException {
		//TODO use builder to complete restore
		try {
			AbstractKeyedStateBackend<K> keyedStateBackend = delegateStateBackend.createKeyedStateBackend(
				env,
				jobID,
				operatorIdentifier,
				keySerializer,
				numberOfKeyGroups,
				keyGroupRange,
				kvStateRegistry,
				ttlTimeProvider,
				metricGroup,
				stateHandles,
				cancelStreamRegistry,
				statsTracker,
				crossNamespace);
			// TODO restore cache
			return new CachedKeyedStateBackend<>(
				kvStateRegistry,
				keySerializer,
				env.getUserClassLoader(),
				env.getExecutionConfig(),
				ttlTimeProvider,
				cancelStreamRegistry,
				getCompressionDecorator(env.getExecutionConfig()),
				new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups),
				keyedStateBackend,
				cacheManager,
				configuration,
				env.getTaskInfo(),
				metricGroup);
		} catch (BackendBuildingException e) {
			throw e;
		} catch (Exception e) {
			throw new BackendBuildingException("create CachedKeyedStateBackend failed", e);
		}
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier,
			@Nonnull Collection<OperatorStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) throws Exception {
		return createOperatorStateBackend(env, operatorIdentifier, stateHandles, cancelStreamRegistry, new NonStateStatsTracker());
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier,
			@Nonnull Collection<OperatorStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry,
			StateStatsTracker statsTracker) throws Exception {
		return delegateStateBackend.createOperatorStateBackend(env, operatorIdentifier, stateHandles, cancelStreamRegistry, statsTracker);
	}
}
