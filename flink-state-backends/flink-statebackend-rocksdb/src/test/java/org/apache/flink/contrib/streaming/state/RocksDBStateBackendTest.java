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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.runtime.util.BlockingCheckpointOutputStream;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.Snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder.DB_INSTANCE_DIR_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for the partitioned state part of {@link RocksDBStateBackend}.
 */
@RunWith(Parameterized.class)
public class RocksDBStateBackendTest extends StateBackendTestBase<RocksDBStateBackend> {

	private OneShotLatch blocker;
	private OneShotLatch waiter;
	private AtomicBoolean checkpointOutputStreamExceptionFlag;
	private BlockerCheckpointStreamFactory testStreamFactory;
	private RocksDBKeyedStateBackend<Integer> keyedStateBackend;
	private List<RocksObject> allCreatedCloseables;
	private ValueState<Integer> testState1;
	private ValueState<String> testState2;

	@Parameterized.Parameters(name = "RocksDB snapshot type ={0}")
	public static Collection<Object[]> parameters() {
		return Arrays.asList(
			new Object[]{RocksDBStateBackendEnum.FULL, false},
			new Object[]{RocksDBStateBackendEnum.FULL, true},
			new Object[]{RocksDBStateBackendEnum.INCREMENTAL, true},
			new Object[]{RocksDBStateBackendEnum.INCREMENTAL, false},
			new Object[]{RocksDBStateBackendEnum.INCREMENTAL_BATCH_FIX_SIZE_SEQ, false},
			new Object[]{RocksDBStateBackendEnum.INCREMENTAL_BATCH_FIX_SIZE_SEQ, true});
	}

	@Parameterized.Parameter
	public RocksDBStateBackendEnum rocksDBStateBackendEnum;

	@Parameterized.Parameter(1)
	public boolean restoreWithSstFileWriter;

	enum RocksDBStateBackendEnum {
		FULL, INCREMENTAL, INCREMENTAL_BATCH_FIX_SIZE_SEQ
	}

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	public RocksDBStateBatchConfig batchConfig;
	// Store it because we need it for the cleanup test.
	private String dbPath;
	private RocksDB db = null;
	private ColumnFamilyHandle defaultCFHandle = null;
	private final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();

	// Config added to configuration
	private boolean discardStatesIfRocksdbRecoverFail;

	public void prepareRocksDB() throws Exception {
		String dbPath = new File(tempFolder.newFolder(), DB_INSTANCE_DIR_STRING).getAbsolutePath();
		ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();

		ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
		db = RocksDBOperationUtils.openDB(dbPath, Collections.emptyList(),
			columnFamilyHandles, columnOptions, optionsContainer.getDbOptions());
		defaultCFHandle = columnFamilyHandles.remove(0);
	}

	public RocksDBStateBackendEnum getRocksDBStateBackendEnum() {
		return rocksDBStateBackendEnum;
	}

	public boolean isEnableIncrementalCheckpointing() {
		return rocksDBStateBackendEnum != RocksDBStateBackendEnum.FULL;
	}

	@Override
	protected RocksDBStateBackend getStateBackend() throws IOException {
		getConfiguration();
		dbPath = tempFolder.newFolder().getAbsolutePath();
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), isEnableIncrementalCheckpointing());
		backend.setBatchConfig(batchConfig);
		Configuration configuration = new Configuration();
		configuration.set(RocksDBOptions.TIMER_SERVICE_FACTORY, RocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
		configuration.setBoolean(RocksDBOptions.DISCARD_STATES_IF_ROCKSDB_RECOVER_FAIL, discardStatesIfRocksdbRecoverFail);
		if (restoreWithSstFileWriter) {
			configuration.set(RocksDBOptions.ROCKSDB_RESTORE_WITH_SST_FILE_WRITER, true);
			configuration.set(RocksDBOptions.MAX_SST_SIZE_FOR_SST_FILE_WRITER, MemorySize.parse("5k"));
		}
		backend = backend.configure(configuration, Thread.currentThread().getContextClassLoader());
		backend.setDbStoragePath(dbPath);
		return backend;
	}

	@Override
	protected boolean isSerializerPresenceRequiredOnRestore() {
		return false;
	}

	// small safety net for instance cleanups, so that no native objects are left
	@After
	public void cleanupRocksDB() {
		if (keyedStateBackend != null) {
			IOUtils.closeQuietly(keyedStateBackend);
			keyedStateBackend.dispose();
		}
		IOUtils.closeQuietly(defaultCFHandle);
		IOUtils.closeQuietly(db);
		IOUtils.closeQuietly(optionsContainer);

		if (allCreatedCloseables != null) {
			for (RocksObject rocksCloseable : allCreatedCloseables) {
				verify(rocksCloseable, times(1)).close();
			}
			allCreatedCloseables = null;
		}
	}

	private void getConfiguration() {
		switch (getRocksDBStateBackendEnum()) {
			case FULL:
				batchConfig = RocksDBStateBatchConfig.createNoBatchingConfig();
				break;
			case INCREMENTAL:
				batchConfig = RocksDBStateBatchConfig.createNoBatchingConfig();
				break;
			case INCREMENTAL_BATCH_FIX_SIZE_SEQ:
				batchConfig = new RocksDBStateBatchConfig(RocksDBStateBatchMode.FIX_SIZE_WITH_SEQUENTIAL_FILE_NUMBER, 128 * 1024 * 1024L);
				break;
			default:
				throw new IllegalStateException("No RocksDB state backend selected.");
		}
	}

	public void setupRocksKeyedStateBackend() throws Exception {
		getConfiguration();

		blocker = new OneShotLatch();
		waiter = new OneShotLatch();
		checkpointOutputStreamExceptionFlag = new AtomicBoolean(false);
		testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
		testStreamFactory.setBlockerLatch(blocker);
		testStreamFactory.setWaiterLatch(waiter);
		testStreamFactory.setAfterNumberInvocations(10);
		testStreamFactory.setManualExceptionFlag(checkpointOutputStreamExceptionFlag);

		prepareRocksDB();

		keyedStateBackend = RocksDBTestUtils.builderForTestDB(
				tempFolder.newFolder(), // this is not used anyways because the DB is injected
				IntSerializer.INSTANCE,
				spy(db),
				defaultCFHandle,
				optionsContainer.getColumnOptions())
			.setEnableIncrementalCheckpointing(isEnableIncrementalCheckpointing())
			.setSstBatchConfig(batchConfig)
			.build();

		testState1 = keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				new ValueStateDescriptor<>("TestState-1", Integer.class, 0));

		testState2 = keyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				new ValueStateDescriptor<>("TestState-2", String.class, ""));

		allCreatedCloseables = new ArrayList<>();

		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				RocksIterator rocksIterator = spy((RocksIterator) invocationOnMock.callRealMethod());
				allCreatedCloseables.add(rocksIterator);
				return rocksIterator;
			}
		}).when(keyedStateBackend.db).newIterator(any(ColumnFamilyHandle.class), any(ReadOptions.class));

		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Snapshot snapshot = spy((Snapshot) invocationOnMock.callRealMethod());
				allCreatedCloseables.add(snapshot);
				return snapshot;
			}
		}).when(keyedStateBackend.db).getSnapshot();

		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				ColumnFamilyHandle snapshot = spy((ColumnFamilyHandle) invocationOnMock.callRealMethod());
				allCreatedCloseables.add(snapshot);
				return snapshot;
			}
		}).when(keyedStateBackend.db).createColumnFamily(any(ColumnFamilyDescriptor.class));

		for (int i = 0; i < 100; ++i) {
			keyedStateBackend.setCurrentKey(i);
			testState1.update(4200 + i);
			testState2.update("S-" + (4200 + i));
		}
	}

	@Test
	public void testCorrectMergeOperatorSet() throws Exception {
		prepareRocksDB();
		final ColumnFamilyOptions columnFamilyOptions = spy(new ColumnFamilyOptions());
		RocksDBKeyedStateBackend<Integer> test = null;

		try {
			test = RocksDBTestUtils.builderForTestDB(
				tempFolder.newFolder(),
				IntSerializer.INSTANCE,
				db,
				defaultCFHandle,
				columnFamilyOptions)
				.setEnableIncrementalCheckpointing(isEnableIncrementalCheckpointing())
				.build();

			ValueStateDescriptor<String> stubState1 =
				new ValueStateDescriptor<>("StubState-1", StringSerializer.INSTANCE);
			test.createInternalState(StringSerializer.INSTANCE, stubState1);
			ValueStateDescriptor<String> stubState2 =
				new ValueStateDescriptor<>("StubState-2", StringSerializer.INSTANCE);
			test.createInternalState(StringSerializer.INSTANCE, stubState2);

			// The default CF is pre-created so sum up to 2 times (once for each stub state)
			verify(columnFamilyOptions, Mockito.times(2))
				.setMergeOperatorName(RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME);
		} finally {
			if (test != null) {
				IOUtils.closeQuietly(test);
				test.dispose();
			}
			columnFamilyOptions.close();
		}
	}

	@Test
	public void testReleasingSnapshotAfterBackendClosed() throws Exception {
		setupRocksKeyedStateBackend();

		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

			RocksDB spyDB = keyedStateBackend.db;

			if (!isEnableIncrementalCheckpointing()) {
				verify(spyDB, times(1)).getSnapshot();
				verify(spyDB, times(0)).releaseSnapshot(any(Snapshot.class));
			}

			//Ensure every RocksObjects not closed yet
			for (RocksObject rocksCloseable : allCreatedCloseables) {
				verify(rocksCloseable, times(0)).close();
			}

			snapshot.cancel(true);

			this.keyedStateBackend.dispose();

			verify(spyDB, times(1)).close();
			assertEquals(true, keyedStateBackend.isDisposed());

			//Ensure every RocksObjects was closed exactly once
			for (RocksObject rocksCloseable : allCreatedCloseables) {
				verify(rocksCloseable, times(1)).close();
			}

		} finally {
			keyedStateBackend.dispose();
			keyedStateBackend = null;
		}
	}

	@Test
	public void testDismissingSnapshot() throws Exception {
		setupRocksKeyedStateBackend();
		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			snapshot.cancel(true);
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}
	}

	@Test
	public void testDismissingSnapshotNotRunnable() throws Exception {
		setupRocksKeyedStateBackend();
		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			snapshot.cancel(true);
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			try {
				snapshot.get();
				fail();
			} catch (Exception ignored) {

			}
			asyncSnapshotThread.join();
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}
	}

	@Test
	public void testCompletingSnapshot() throws Exception {
		setupRocksKeyedStateBackend();
		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			waiter.await(); // wait for snapshot to run
			waiter.reset();
			runStateUpdates();
			blocker.trigger(); // allow checkpointing to start writing
			waiter.await(); // wait for snapshot stream writing to run

			SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
			KeyedStateHandle keyedStateHandle = snapshotResult.getJobManagerOwnedSnapshot();
			assertNotNull(keyedStateHandle);
			assertTrue(keyedStateHandle.getStateSize() > 0);
			assertEquals(2, keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups());

			for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
				assertTrue(stream.isClosed());
			}

			asyncSnapshotThread.join();
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}
	}

	@Test
	public void testCancelRunningSnapshot() throws Exception {
		setupRocksKeyedStateBackend();
		try {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			waiter.await(); // wait for snapshot to run
			waiter.reset();
			runStateUpdates();
			snapshot.cancel(true);
			blocker.trigger(); // allow checkpointing to start writing

			for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
				assertTrue(stream.isClosed());
			}

			waiter.await(); // wait for snapshot stream writing to run
			try {
				snapshot.get();
				fail();
			} catch (Exception ignored) {
			}

			asyncSnapshotThread.join();
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}
	}

	@Test
	public void testFailureRunningSnapshot() throws Exception {
		setupRocksKeyedStateBackend();
		if (!isEnableIncrementalCheckpointing()) {
			return;
		}

		// Different from other tests, this CI test need to use local fs as checkpoint fs system.
		File checkpointPrivateFolder = tempFolder.newFolder("private");
		org.apache.flink.core.fs.Path checkpointPrivateDirectory = org.apache.flink.core.fs.Path.fromLocalFile(checkpointPrivateFolder);

		File checkpointSharedFolder = tempFolder.newFolder("shared");
		org.apache.flink.core.fs.Path checkpointSharedDirectory = org.apache.flink.core.fs.Path.fromLocalFile(checkpointSharedFolder);

		FileSystem fileSystem = checkpointPrivateDirectory.getFileSystem();
		int writeBufferSize = 4096;
		FsCheckpointStreamFactory checkpointStreamFactory =
			new FsCheckpointStreamFactory(
				fileSystem, checkpointPrivateDirectory, checkpointSharedDirectory, 0, writeBufferSize);
		testStreamFactory.setCheckpointFsFactory(checkpointStreamFactory);

		// (1) run a completed checkpoint
		try {
			runStateUpdates();
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			waiter.await(); // wait for snapshot to run
			waiter.reset();
			blocker.trigger(); // allow checkpointing to start writing
			waiter.await(); // wait for snapshot stream writing to run

			SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();
			KeyedStateHandle keyedStateHandle = snapshotResult.getJobManagerOwnedSnapshot();
			assertNotNull(keyedStateHandle);
			assertTrue(keyedStateHandle.getStateSize() > 0);
			assertEquals(2, keyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups());

			for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
				assertTrue(stream.isClosed());
			}

			asyncSnapshotThread.join();
		} finally {

		}

		Set<Path> filesInCheckpointDirFirst = new HashSet<>(
			Arrays.asList(org.apache.flink.util.FileUtils.listDirectory(checkpointSharedFolder.toPath())));

		keyedStateBackend.notifyCheckpointComplete(0L);

		// (2) run a consecutive checkpoint
		try {
			runStateUpdates();
			RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
				keyedStateBackend.snapshot(1L, 4L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			waiter.await(); // wait for snapshot to run
			waiter.reset();
			checkpointOutputStreamExceptionFlag.set(true); // open the exception flag

			blocker.trigger(); // allow checkpointing to start writing
			waiter.await(); // wait for snapshot stream writing to run

			try {
				snapshot.get();
				fail();
			} catch (Exception ignored) {
			}

			asyncSnapshotThread.join();
			verifyRocksObjectsReleased();
		} finally {
			this.keyedStateBackend.dispose();
			this.keyedStateBackend = null;
		}

		Set<Path> filesInCheckpointDirSecond = new HashSet<>(
			Arrays.asList(org.apache.flink.util.FileUtils.listDirectory(checkpointSharedFolder.toPath())));

		log.info("shared dir after first chk: {}", filesInCheckpointDirFirst);
		log.info("shared dir after second chk: {}", filesInCheckpointDirSecond);

		assertEquals(filesInCheckpointDirFirst, filesInCheckpointDirSecond);
	}

	@Test
	public void testDisposeDeletesAllDirectories() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);
		Collection<File> allFilesInDbDir =
			FileUtils.listFilesAndDirs(new File(dbPath), new AcceptAllFilter(), new AcceptAllFilter());
		try {
			ValueStateDescriptor<String> kvId =
				new ValueStateDescriptor<>("id", String.class, null);

			kvId.initializeSerializerUnlessSet(new ExecutionConfig());

			ValueState<String> state =
				backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

			backend.setCurrentKey(1);
			state.update("Hello");

			// more than just the root directory
			assertTrue(allFilesInDbDir.size() > 1);
		} finally {
			IOUtils.closeQuietly(backend);
			backend.dispose();
		}
		allFilesInDbDir =
			FileUtils.listFilesAndDirs(new File(dbPath), new AcceptAllFilter(), new AcceptAllFilter());

		// just the root directory left
		assertEquals(1, allFilesInDbDir.size());
	}

	@Test
	public void testSharedIncrementalStateDeRegistration() throws Exception {
		if (isEnableIncrementalCheckpointing()) {
			AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);
			try {
				ValueStateDescriptor<String> kvId =
					new ValueStateDescriptor<>("id", String.class, null);

				kvId.initializeSerializerUnlessSet(new ExecutionConfig());

				ValueState<String> state =
					backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				Queue<IncrementalRemoteKeyedStateHandle> previousStateHandles = new LinkedList<>();
				SharedStateRegistry sharedStateRegistry = spy(new SharedStateRegistry());
				for (int checkpointId = 0; checkpointId < 3; ++checkpointId) {

					reset(sharedStateRegistry);

					backend.setCurrentKey(checkpointId);
					state.update("Hello-" + checkpointId);

					RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot = backend.snapshot(
						checkpointId,
						checkpointId,
						createStreamFactory(),
						CheckpointOptions.forCheckpointWithDefaultLocation());

					snapshot.run();

					SnapshotResult<KeyedStateHandle> snapshotResult = snapshot.get();

					IncrementalRemoteKeyedStateHandle stateHandle =
						(IncrementalRemoteKeyedStateHandle) snapshotResult.getJobManagerOwnedSnapshot();

					Map<StateHandleID, StreamStateHandle> sharedState =
						new HashMap<>(stateHandle.getSharedState());

					stateHandle.registerSharedStates(sharedStateRegistry);

					for (Map.Entry<StateHandleID, StreamStateHandle> e : sharedState.entrySet()) {
						verify(sharedStateRegistry).registerReference(
							stateHandle.createSharedStateRegistryKeyFromFileName(e.getKey()),
							e.getValue());
					}

					previousStateHandles.add(stateHandle);
					backend.notifyCheckpointComplete(checkpointId);

					//-----------------------------------------------------------------

					if (previousStateHandles.size() > 1) {
						checkRemove(previousStateHandles.remove(), sharedStateRegistry);
					}
				}

				while (!previousStateHandles.isEmpty()) {

					reset(sharedStateRegistry);

					checkRemove(previousStateHandles.remove(), sharedStateRegistry);
				}
			} finally {
				IOUtils.closeQuietly(backend);
				backend.dispose();
			}
		}
	}

	@Test
	public void testDiscardStatesIfIncrementalRecoverError() throws Exception {
		if (rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL ||
			rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL_BATCH_FIX_SIZE_SEQ) {
			try {
				CheckpointStreamFactory streamFactory = createStreamFactory();

				SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

				// use an IntSerializer at first
				AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

				ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

				ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				// write some state
				backend.setCurrentKey(1);
				state.update("1");
				backend.setCurrentKey(2);
				state.update("2");

				// draw a snapshot
				IncrementalRemoteKeyedStateHandle snapshot1 = (IncrementalRemoteKeyedStateHandle) runSnapshot(
					backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);

				backend.dispose();

				assertEquals(1, snapshot1.getSharedState().size());
				Map.Entry<StateHandleID, StreamStateHandle> entry = snapshot1.getSharedState().entrySet().stream().findFirst().orElse(null);
				assertTrue(entry != null && (entry.getValue() instanceof ByteStreamStateHandle || entry.getValue() instanceof BatchStateHandle));

				//verify recover success.
				backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
				state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				backend.setCurrentKey(1);
				assertEquals("1", state.value());
				backend.setCurrentKey(2);
				assertEquals("2", state.value());
				assertEquals(2, backend.numKeyValueStateEntries());
				backend.dispose();

				// prepare abnormal data.
				boolean batchMode = entry.getValue() instanceof BatchStateHandle;
				StreamStateHandle oriStateHandle = entry.getValue();
				ByteStreamStateHandle byteStreamStateHandle = batchMode ?
					(ByteStreamStateHandle) ((BatchStateHandle) oriStateHandle).getDelegateStateHandle() :
					(ByteStreamStateHandle) oriStateHandle;
				byte[] wrongData = Arrays.copyOfRange(byteStreamStateHandle.getData(), 0, byteStreamStateHandle.getData().length - 2);
				ByteStreamStateHandle errorStreamStateHandle = new ByteStreamStateHandle(byteStreamStateHandle.getHandleName(), wrongData);
				StreamStateHandle errorStateHandle = batchMode ? new BatchStateHandle(errorStreamStateHandle,
					((BatchStateHandle) oriStateHandle).getStateHandleIds(),
					((BatchStateHandle) oriStateHandle).getOffsetsAndSizes(),
					((BatchStateHandle) oriStateHandle).getBatchFileID()) :
					errorStreamStateHandle;
				entry.setValue(errorStateHandle);

				//verify recover failed.
				try {
					discardStatesIfRocksdbRecoverFail = false;
					backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
					fail("backend should failed to recover");
				} catch (Exception e) {
					assertTrue(ExceptionUtils.findThrowable(e, RocksDBException.class).isPresent());
				}

				//verify that all states are discarded for recovery
				discardStatesIfRocksdbRecoverFail = true;
				backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
				state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				backend.setCurrentKey(1);
				assertNull(state.value());
				backend.setCurrentKey(2);
				assertNull(state.value());
				assertEquals(0, backend.numKeyValueStateEntries());
				backend.dispose();
			} finally {
				discardStatesIfRocksdbRecoverFail = false;
			}
		}
	}

	@Test
	public void testDisposeCleanUpRocksDBStateDataTransferThread() throws Exception {

		if (rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL ||
			rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL_BATCH_FIX_SIZE_SEQ) {
			RocksDBKeyedStateBackend<Integer> test = null;
			final ColumnFamilyOptions columnFamilyOptions = spy(new ColumnFamilyOptions());

			try {
				prepareRocksDB();
				CheckpointStreamFactory streamFactory = createStreamFactory();
				SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
				test = RocksDBTestUtils.builderForTestDB(
					tempFolder.newFolder(),
					IntSerializer.INSTANCE,
					db,
					defaultCFHandle,
					columnFamilyOptions)
					.setNumberOfTransferingThreads(2)
					.setEnableIncrementalCheckpointing(isEnableIncrementalCheckpointing())
					.build();

				ValueStateDescriptor<String> stubState1 =
					new ValueStateDescriptor<>("StubState-1", StringSerializer.INSTANCE);
				test.createInternalState(StringSerializer.INSTANCE, stubState1);
				ValueStateDescriptor<String> stubState2 =
					new ValueStateDescriptor<>("StubState-2", StringSerializer.INSTANCE);
				test.createInternalState(StringSerializer.INSTANCE, stubState2);

				ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

				ValueState<String> state = test.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				// write some state
				test.setCurrentKey(1);
				state.update("1");
				test.setCurrentKey(2);
				state.update("2");

				// draw a snapshot
				runSnapshot(
					test.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);

				test.dispose();

				for (Thread t : Thread.getAllStackTraces().keySet()) {
					if (t.getName().contains("Flink-RocksDBStateDataTransfer") && t.getState() != Thread.State.TERMINATED) {
						fail("Detected rocksDBStateDataTransfer thread leak. Thread name: " + t.getName());
					}
				}
			} finally {
				if (test != null) {
					IOUtils.closeQuietly(test);
					test.dispose();
				}
				columnFamilyOptions.close();
			}
		}
	}

	@Test
	public void testDBNativeCheckpointTimeout() throws Exception {

		if (rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL ||
			rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL_BATCH_FIX_SIZE_SEQ) {
			RocksDBKeyedStateBackend<Integer> test = null;
			final ColumnFamilyOptions columnFamilyOptions = spy(new ColumnFamilyOptions());

			try {
				prepareRocksDB();
				CheckpointStreamFactory streamFactory = createStreamFactory();
				SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
				test = RocksDBTestUtils.builderForTestDB(
					tempFolder.newFolder(),
					IntSerializer.INSTANCE,
					db,
					defaultCFHandle,
					columnFamilyOptions)
					.setNumberOfTransferingThreads(2)
					.setEnableIncrementalCheckpointing(isEnableIncrementalCheckpointing())

					// set timeout to 1 millis
					.setDBNativeCheckpointTimeout(1)
					.build();

				ValueStateDescriptor<String> stubState1 =
					new ValueStateDescriptor<>("StubState-1", StringSerializer.INSTANCE);
				test.createInternalState(StringSerializer.INSTANCE, stubState1);
				ValueStateDescriptor<String> stubState2 =
					new ValueStateDescriptor<>("StubState-2", StringSerializer.INSTANCE);
				test.createInternalState(StringSerializer.INSTANCE, stubState2);

				ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

				ValueState<String> state = test.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				// write some state
				test.setCurrentKey(1);
				state.update("1");
				test.setCurrentKey(2);
				state.update("2");

				// draw a snapshot

				try {
					runSnapshot(
					test.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);
					fail("Expect TimeoutException here");
				} catch (Exception e) {
					assertTrue(ExceptionUtils.findThrowable(e, TimeoutException.class).isPresent());
				}

				test.dispose();

			} finally {
				if (test != null) {
					IOUtils.closeQuietly(test);
					test.dispose();
				}
				columnFamilyOptions.close();
			}
		}
	}

	@Test
	public void testDisposeTimeout() throws Exception {
		RocksDBKeyedStateBackend<Integer> test = null;
		final ColumnFamilyOptions columnFamilyOptions = spy(new ColumnFamilyOptions());

		prepareRocksDB();
		test = RocksDBTestUtils.builderForTestDB(
			tempFolder.newFolder(),
			IntSerializer.INSTANCE,
			db,
			defaultCFHandle,
			columnFamilyOptions)
			.setNumberOfTransferingThreads(2)
			.setEnableIncrementalCheckpointing(isEnableIncrementalCheckpointing())
			// set timeout to 1 millis
			.setDisposeTimeout(1)
			.build();

		try {
			test.dispose();
			fail("Expect TimeoutException here");
		} catch (Exception e) {
			Assert.assertEquals(e.getMessage(), "Failed to dispose RocksdbKeyedStateBackend.");
		}
	}

	private void checkRemove(IncrementalRemoteKeyedStateHandle remove, SharedStateRegistry registry) throws Exception {
		for (StateHandleID id : remove.getSharedState().keySet()) {
			verify(registry, times(0)).unregisterReference(
				remove.createSharedStateRegistryKeyFromFileName(id));
		}

		remove.discardState();

		for (StateHandleID id : remove.getSharedState().keySet()) {
			verify(registry).unregisterReference(
				remove.createSharedStateRegistryKeyFromFileName(id));
		}
	}

	private void runStateUpdates() throws Exception{
		for (int i = 50; i < 150; ++i) {
			if (i % 10 == 0) {
				Thread.sleep(1);
			}
			keyedStateBackend.setCurrentKey(i);
			testState1.update(4200 + i);
			testState2.update("S-" + (4200 + i));
		}
	}

	private void verifyRocksObjectsReleased() {
		//Ensure every RocksObject was closed exactly once
		for (RocksObject rocksCloseable : allCreatedCloseables) {
			verify(rocksCloseable, times(1)).close();
		}

		assertNotNull(null, keyedStateBackend.db);
		RocksDB spyDB = keyedStateBackend.db;

		if (!isEnableIncrementalCheckpointing()) {
			verify(spyDB, times(1)).getSnapshot();
			verify(spyDB, times(1)).releaseSnapshot(any(Snapshot.class));
		}

		keyedStateBackend.dispose();
		verify(spyDB, times(1)).close();
		assertEquals(true, keyedStateBackend.isDisposed());
	}

	@Test
	public void testSstFileWriterRecoverMode() throws Exception {
		if (restoreWithSstFileWriter) {
			CheckpointStreamFactory streamFactory = createStreamFactory();

			SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

			// use an IntSerializer at first
			AbstractKeyedStateBackend<Integer> backend1 = createKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(0, 9), env);
			AbstractKeyedStateBackend<Integer> backend2 = createKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(10, 19), env);

			ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);
			ValueState<String> valueState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
			ValueState<String> valueState2 = backend2.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

			MapStateDescriptor<String, String> kvMap = new MapStateDescriptor<>("kvMap", String.class, String.class);
			MapState<String, String> mapState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);
			MapState<String, String> mapState2 = backend2.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);

			for (int i = 0; i < 1_0000; i++) {
				int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(i, 20);
				if (keyGroup < 10) {
					update(i, backend1, valueState1, mapState1);
				} else {
					update(i, backend2, valueState2, mapState2);
				}
			}
			KeyedStateHandle backend1Snapshot1, backend2Snapshot1;
			if (rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL ||
				rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL_BATCH_FIX_SIZE_SEQ) {
				// draw a incremental snapshot
				backend1Snapshot1 = (IncrementalRemoteKeyedStateHandle) runSnapshot(
					backend1.snapshot(1L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);
				backend2Snapshot1 = (IncrementalRemoteKeyedStateHandle) runSnapshot(
					backend2.snapshot(1L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);
			} else {
				// draw a full snapshot
				backend1Snapshot1 = (KeyGroupsStateHandle) runSnapshot(
					backend1.snapshot(1L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					null);
				backend2Snapshot1 = (KeyGroupsStateHandle) runSnapshot(
					backend2.snapshot(1L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					null);
			}

			backend1.dispose();
			backend2.dispose();

			// verify recover success.
			backend1 = restoreKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(0, 9), new ArrayList<>(Arrays.asList(backend1Snapshot1)), env);
			valueState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
			mapState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);

			backend2 = restoreKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(10, 19), new ArrayList<>(Arrays.asList(backend2Snapshot1)), env);
			valueState2 = backend2.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
			mapState2 = backend2.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);

			for (int i = 0; i < 1_0000; i++) {
				int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(i, 20);
				if (keyGroup < 10) {
					checkStateExist(i, backend1, valueState1, mapState1);
					checkStateNotExist(i, backend2, valueState2, mapState2);
				} else {
					checkStateExist(i, backend2, valueState2, mapState2);
					checkStateNotExist(i, backend1, valueState1, mapState1);
				}
			}
			backend1.dispose();
			backend2.dispose();

			// scale up
			backend1 = restoreKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(0, 6), new ArrayList<>(Arrays.asList(backend1Snapshot1)), env);
			valueState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
			mapState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);

			backend2 = restoreKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(7, 13), new ArrayList<>(Arrays.asList(backend1Snapshot1, backend2Snapshot1)), env);
			valueState2 = backend2.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
			mapState2 = backend2.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);

			AbstractKeyedStateBackend<Integer> backend3 = restoreKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(14, 20), new ArrayList<>(Arrays.asList(backend2Snapshot1)), env);
			ValueState<String> valueState3 = backend3.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
			MapState<String, String> mapState3 = backend3.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);

			for (int i = 0; i < 10_000; i++) {
				int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(i, 20);
				if (keyGroup < 7) {
					checkStateExist(i, backend1, valueState1, mapState1);
					checkStateNotExist(i, backend2, valueState2, mapState2);
					checkStateNotExist(i, backend3, valueState3, mapState3);
				} else if (keyGroup < 14) {
					checkStateExist(i, backend2, valueState2, mapState2);
					checkStateNotExist(i, backend1, valueState1, mapState1);
					checkStateNotExist(i, backend3, valueState3, mapState3);
				} else {
					checkStateExist(i, backend3, valueState3, mapState3);
					checkStateNotExist(i, backend2, valueState2, mapState2);
					checkStateNotExist(i, backend1, valueState1, mapState1);
				}
			}

			backend1.dispose();
			backend2.dispose();
			backend3.dispose();

			// scale down
			backend1 = restoreKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(0, 19), new ArrayList<>(Arrays.asList(backend1Snapshot1, backend2Snapshot1)), env);
			valueState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
			mapState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);
			for (int i = 0; i < 10_000; i++) {
				checkStateExist(i, backend1, valueState1, mapState1);
			}
			backend1.dispose();
		}
	}

	@Test
	public void testSstFileWriterRecoverModeWithError() throws Exception {
		if (restoreWithSstFileWriter) {
			CheckpointStreamFactory streamFactory = createStreamFactory();

			SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

			// use an IntSerializer at first
			AbstractKeyedStateBackend<Integer> backend1 = createKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(0, 9), env);
			AbstractKeyedStateBackend<Integer> backend2 = createKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(10, 19), env);

			ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);
			ValueState<String> valueState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
			ValueState<String> valueState2 = backend2.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

			MapStateDescriptor<String, String> kvMap = new MapStateDescriptor<>("kvMap", String.class, String.class);
			MapState<String, String> mapState1 = backend1.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);
			MapState<String, String> mapState2 = backend2.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvMap);

			for (int i = 0; i < 1_0000; i++) {
				int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(i, 20);
				if (keyGroup < 10) {
					update(i, backend1, valueState1, mapState1);
				} else {
					update(i, backend2, valueState2, mapState2);
				}
			}

			KeyedStateHandle backend1Snapshot1;
			if (rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL ||
				rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL_BATCH_FIX_SIZE_SEQ) {
				// draw a incremental snapshot
				backend1Snapshot1 = (IncrementalRemoteKeyedStateHandle) runSnapshot(
					backend1.snapshot(1L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);
			} else {
				// draw a full snapshot
				backend1Snapshot1 = (KeyGroupsStateHandle) runSnapshot(
					backend1.snapshot(1L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					null);
			}
			backend1.dispose();
			backend2.dispose();
			backend1 = null;

			Thread.currentThread().interrupt();
			try {
				// scale
				backend1 = restoreKeyedBackend(IntSerializer.INSTANCE, 20, KeyGroupRange.of(0, 6), new ArrayList<>(Arrays.asList(backend1Snapshot1)), env);
				fail();
			} catch (Exception ignore) {
				// ignore
			} finally {
				if (backend1 != null) {
					backend1.dispose();
				}
			}
		}
	}

	@Test
	public void testIncrementalRecoverWithCrossNamespace() throws Exception {
		if (rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL ||
			rocksDBStateBackendEnum == RocksDBStateBackendEnum.INCREMENTAL_BATCH_FIX_SIZE_SEQ) {
			try {
				CheckpointStreamFactory streamFactory = createStreamFactory();

				SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

				// use an IntSerializer at first
				AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

				ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

				ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				// write some state
				backend.setCurrentKey(1);
				state.update("1");
				backend.setCurrentKey(2);
				state.update("2");

				// draw a snapshot
				IncrementalRemoteKeyedStateHandle snapshot1 = (IncrementalRemoteKeyedStateHandle) runSnapshot(
					backend.snapshot(1, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);

				backend.dispose();

				assertEquals(1, snapshot1.getSharedState().size());
				Map.Entry<StateHandleID, StreamStateHandle> entry = snapshot1.getSharedState().entrySet().stream().findFirst().orElse(null);
				SharedStateRegistryKey registryKey = snapshot1.createSharedStateRegistryKeyFromFileName(entry.getKey());
				StreamStateHandle stateHandle = entry.getValue();
				SharedStateRegistry.Result result = sharedStateRegistry.unregisterReference(registryKey);
				assertEquals(null, result.getReference());
				assertEquals(0, result.getReferenceCount());

				sharedStateRegistry.registerReference(registryKey, stateHandle);
				UUID oldBackendIdentifier = snapshot1.getBackendIdentifier();

				//verify recover success with out cross namespace.
				backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);
				state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				backend.setCurrentKey(1);
				assertEquals("1", state.value());
				backend.setCurrentKey(2);
				assertEquals("2", state.value());
				assertEquals(2, backend.numKeyValueStateEntries());

				IncrementalRemoteKeyedStateHandle snapshot2 = (IncrementalRemoteKeyedStateHandle) runSnapshot(
					backend.snapshot(2, 4, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);
				backend.dispose();

				// make sure backendIdentifier is equal
				Assert.assertEquals(oldBackendIdentifier, snapshot2.getBackendIdentifier());

				// make sure shared state can be register success
				assertEquals(1, snapshot2.getSharedState().size());
				result = sharedStateRegistry.unregisterReference(registryKey);
				assertEquals(stateHandle, result.getReference());
				assertEquals(1, result.getReferenceCount());

				//verify recover success with cross namespace.
				backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1, true);
				state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);

				backend.setCurrentKey(1);
				assertEquals("1", state.value());
				backend.setCurrentKey(2);
				assertEquals("2", state.value());
				assertEquals(2, backend.numKeyValueStateEntries());

				IncrementalRemoteKeyedStateHandle snapshot3 = (IncrementalRemoteKeyedStateHandle) runSnapshot(
					backend.snapshot(3L, 6, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
					sharedStateRegistry);
				backend.dispose();

				// make sure backendIdentifier is not equal
				Assert.assertNotEquals(oldBackendIdentifier, snapshot3.getBackendIdentifier());

				// make sure we have different shared state
				assertEquals(1, snapshot3.getSharedState().size());
				result = sharedStateRegistry.unregisterReference(registryKey);
				assertNull(result.getReference());
				assertEquals(0, result.getReferenceCount());

				entry = snapshot3.getSharedState().entrySet().stream().findFirst().orElse(null);
				registryKey = snapshot3.createSharedStateRegistryKeyFromFileName(entry.getKey());
				result = sharedStateRegistry.unregisterReference(registryKey);
				assertNull(result.getReference());
				assertEquals(0, result.getReferenceCount());
			} finally {
				discardStatesIfRocksdbRecoverFail = false;
			}
		}
	}

	private void update(
			Integer key,
			AbstractKeyedStateBackend<Integer> backend,
			ValueState<String> valueState,
			MapState<String, String> mapState) throws Exception {

		backend.setCurrentKey(key);
		valueState.update(String.valueOf(key));
		for (int j = 0; j < 3; j++) {
			mapState.put(key + "-key-" + j, key + "-value-" + j);
		}
	}

	private void checkStateExist(
			Integer key,
			AbstractKeyedStateBackend<Integer> backend,
			ValueState<String> valueState,
			MapState<String, String> mapState) throws Exception {

		backend.setCurrentKey(key);
		assertEquals(String.valueOf(key), valueState.value());
		for (int j = 0; j < 3; j++) {
			assertEquals(key + "-value-" + j, mapState.get(key + "-key-" + j));
		}
	}

	private void checkStateNotExist(
		Integer key,
		AbstractKeyedStateBackend<Integer> backend,
		ValueState<String> valueState,
		MapState<String, String> mapState) throws Exception {

		backend.setCurrentKey(key);
		assertNull(valueState.value());
		for (int j = 0; j < 3; j++) {
			assertNull(mapState.get(key + "-key-" + j));
		}
	}

	private static class AcceptAllFilter implements IOFileFilter {
		@Override
		public boolean accept(File file) {
			return true;
		}

		@Override
		public boolean accept(File file, String s) {
			return true;
		}
	}
}
