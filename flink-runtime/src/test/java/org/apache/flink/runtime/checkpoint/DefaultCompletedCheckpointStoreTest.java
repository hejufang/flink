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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.persistence.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DefaultCompletedCheckpointStore}.
 */
public class DefaultCompletedCheckpointStoreTest extends TestLogger {

	private final long timeout = 100L;

	private TestingStateHandleStore.Builder<CompletedCheckpoint> builder;

	private TestingRetrievableStateStorageHelper<CompletedCheckpoint> checkpointStorageHelper;

	private ExecutorService executorService;

	@Before
	public void setup() {
		builder = TestingStateHandleStore.builder();
		checkpointStorageHelper = new TestingRetrievableStateStorageHelper<>();
		executorService = Executors.newFixedThreadPool(2, new ExecutorThreadFactory("IO-Executor"));
	}

	@After
	public void after() {
		executorService.shutdownNow();
	}

	@Test
	public void testAtLeastOneCheckpointRetained() throws Exception {
		CompletedCheckpoint cp1 = getCheckpoint(1L, false);
		CompletedCheckpoint cp2 = getCheckpoint(2L, false);
		CompletedCheckpoint sp1 = getCheckpoint(3L, true);
		CompletedCheckpoint sp2 = getCheckpoint(4L, true);
		CompletedCheckpoint sp3 = getCheckpoint(5L, true);
		testCheckpointRetention(1, asList(cp1, cp2, sp1, sp2, sp3), asList(cp2, sp3));
	}

	@Test
	public void testOlderSavepointSubsumed() throws Exception {
		CompletedCheckpoint cp1 = getCheckpoint(1L, false);
		CompletedCheckpoint sp1 = getCheckpoint(2L, true);
		CompletedCheckpoint cp2 = getCheckpoint(3L, false);
		testCheckpointRetention(1, asList(cp1, sp1, cp2), asList(cp2));
	}

	@Test
	public void testSubsumeAfterStoppingWithSavepoint() throws Exception {
		CompletedCheckpoint cp1 = getCheckpoint(1L, false);
		CompletedCheckpoint sp1 = getCheckpoint(2L, true);
		CompletedCheckpoint stop = getCheckpoint(CheckpointProperties.forSyncSavepoint(false), 3L);
		testCheckpointRetention(1, asList(cp1, sp1, stop), asList(stop));
	}

	@Test
	public void testNotSubsumedIfNotNeeded() throws Exception {
		CompletedCheckpoint cp1 = getCheckpoint(1L, false);
		CompletedCheckpoint cp2 = getCheckpoint(2L, false);
		CompletedCheckpoint cp3 = getCheckpoint(3L, false);
		testCheckpointRetention(3, asList(cp1, cp2, cp3), asList(cp1, cp2, cp3));
	}

	private void testCheckpointRetention(
		int numRetain,
		List<CompletedCheckpoint> completed,
		List<CompletedCheckpoint> expectedRetained)
		throws Exception {
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore =
			builder.setGetAllSupplier(() -> createStateHandles(3)).build();
		final CompletedCheckpointStore completedCheckpointStore =
			createCompletedCheckpointStore(stateHandleStore, numRetain);

		for (CompletedCheckpoint c : completed) {
			completedCheckpointStore.addCheckpoint(c);
		}
		assertEquals(expectedRetained, completedCheckpointStore.getAllCheckpoints());
	}

	/**
	 * We have three completed checkpoints(1, 2, 3) in the state handle store. We expect that
	 * {@link DefaultCompletedCheckpointStore#recover()} should recover the sorted checkpoints by name.
	 */
	@Test
	public void testRecoverSortedCheckpoints() throws Exception {
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(3))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();

		final List<CompletedCheckpoint> recoveredCompletedCheckpoint = completedCheckpointStore.getAllCheckpoints();
		assertThat(recoveredCompletedCheckpoint.size(), is(3));
		final List<Long> checkpointIds = recoveredCompletedCheckpoint.stream()
			.map(CompletedCheckpoint::getCheckpointID)
			.collect(Collectors.toList());
		assertThat(checkpointIds, contains(1L, 2L, 3L));

	}

	/**
	 * We got an {@link IOException} when retrieving checkpoint 2. It should be skipped.
	 */
	@Test
	public void testCorruptDataInStateHandleStoreShouldBeSkipped() throws Exception {
		final long corruptCkpId = 3L;
		checkpointStorageHelper.setRetrieveStateFunction(state -> {
			if (state.getCheckpointID() == corruptCkpId) {
				throw new IOException("Failed to retrieve checkpoint " + corruptCkpId);
			}
			return state;
		});

		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(3))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();

		final List<CompletedCheckpoint> recoveredCompletedCheckpoint = completedCheckpointStore.getAllCheckpoints();
		assertThat(recoveredCompletedCheckpoint.size(), is(2));
		final List<Long> checkpointIds = recoveredCompletedCheckpoint.stream()
			.map(CompletedCheckpoint::getCheckpointID)
			.collect(Collectors.toList());
		// Checkpoint 2 should be skipped.
		assertThat(checkpointIds, contains(1L, 2L));
	}

	/**
	 * We got an {@link IOException} when retrieving checkpoint 2. It should be skipped.
	 */
	@Test
	public void testCorruptDataInStateHandleStoreShouldNotBeSkipped() throws Exception {
		final long corruptCkpId = 2L;
		checkpointStorageHelper.setRetrieveStateFunction(state -> {
			if (state.getCheckpointID() == corruptCkpId) {
				throw new IOException("Failed to retrieve checkpoint " + corruptCkpId);
			}
			return state;
		});

		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(3))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();

		final List<CompletedCheckpoint> recoveredCompletedCheckpoint = completedCheckpointStore.getAllCheckpoints();
		assertThat(recoveredCompletedCheckpoint.size(), is(3));
		final List<Long> checkpointIds = recoveredCompletedCheckpoint.stream()
			.map(CompletedCheckpoint::getCheckpointID)
			.collect(Collectors.toList());
		// Checkpoint 2 should be skipped.
		assertThat(checkpointIds, contains(1L, 2L, 3L));
	}

	/**
	 * {@link DefaultCompletedCheckpointStore#recover()} should throw exception when all the checkpoints retrieved
	 * failed while the checkpoint pointers are not empty.
	 */
	@Test
	public void testRecoverFailedWhenRetrieveCheckpointAllFailed() {
		final int ckpNum = 3;
		checkpointStorageHelper.setRetrieveStateFunction((state) -> {
			throw new IOException("Failed to retrieve checkpoint " + state.getCheckpointID());
		});

		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(ckpNum))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		try {
			completedCheckpointStore.recover();
			fail("We should get an exception when retrieving state failed.");
		} catch (Exception ex) {
			final String errMsg = "Could not read any of the " + ckpNum + " checkpoints from storage.";
			assertThat(ex, FlinkMatchers.containsMessage(errMsg));
		}
	}

	@Test
	public void testAddCheckpointSuccessfullyShouldRemoveOldOnes () throws Exception {
		final int num = 1;
		final CompletableFuture<CompletedCheckpoint> addFuture = new CompletableFuture<>();
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(num))
			.setAddFunction((ignore, ckp) -> {
				addFuture.complete(ckp);
				return null;
			})
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));
		assertThat(completedCheckpointStore.getAllCheckpoints().get(0).getCheckpointID(), is(1L));

		final long ckpId = 100L;
		final CompletedCheckpoint ckp = CompletedCheckpointStoreTest.createCheckpoint(ckpId, new SharedStateRegistry());
		completedCheckpointStore.addCheckpoint(ckp);

		// We should persist the completed checkpoint to state handle store.
		final CompletedCheckpoint addedCkp = addFuture.get(timeout, TimeUnit.MILLISECONDS);
		assertThat(addedCkp.getCheckpointID(), is(ckpId));

		// Check the old checkpoint is removed and new one is added.
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));
		assertThat(completedCheckpointStore.getAllCheckpoints().get(0).getCheckpointID(), is(ckpId));
	}

	@Test
	public void testAddCheckpointFailedShouldNotRemoveOldOnes() throws Exception {
		final int num = 1;
		final String errMsg = "Add to state handle failed.";
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(num))
			.setAddFunction((ignore, ckp) -> {
				throw new FlinkException(errMsg);
			})
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));
		assertThat(completedCheckpointStore.getAllCheckpoints().get(0).getCheckpointID(), is(1L));

		final long ckpId = 100L;
		final CompletedCheckpoint ckp = CompletedCheckpointStoreTest.createCheckpoint(ckpId, new SharedStateRegistry());

		try {
			completedCheckpointStore.addCheckpoint(ckp);
			fail("We should get an exception when add checkpoint to failed..");
		} catch (FlinkException ex) {
			assertThat(ex, FlinkMatchers.containsMessage(errMsg));
		}
		// Check the old checkpoint still exists.
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));
		assertThat(completedCheckpointStore.getAllCheckpoints().get(0).getCheckpointID(), is(1L));
	}

	@Test
	public void testShutdownShouldDiscardStateHandleWhenJobIsGloballyTerminalState() throws Exception {
		final int num = 3;
		final AtomicInteger removeCalledNum = new AtomicInteger(0);
		final CompletableFuture<Void> clearEntriesAllFuture = new CompletableFuture<>();
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(num))
			.setRemoveFunction(ignore -> {
				removeCalledNum.incrementAndGet();
				return true;
			})
			.setClearEntriesRunnable(() -> clearEntriesAllFuture.complete(null))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(num));

		completedCheckpointStore.shutdown(JobStatus.CANCELED);
		assertThat(removeCalledNum.get(), is(num));
		assertThat(clearEntriesAllFuture.isDone(), is(true));
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(0));
	}

	@Test
	public void testShutdownShouldNotDiscardStateHandleWhenJobIsNotGloballyTerminalState() throws Exception {
		final AtomicInteger removeCalledNum = new AtomicInteger(0);
		final CompletableFuture<Void> removeAllFuture = new CompletableFuture<>();
		final CompletableFuture<Void> releaseAllFuture = new CompletableFuture<>();
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder
			.setGetAllSupplier(() -> createStateHandles(3))
			.setRemoveFunction(ignore -> {
				removeCalledNum.incrementAndGet();
				return true;
			})
			.setReleaseAllHandlesRunnable(() -> releaseAllFuture.complete(null))
			.setClearEntriesRunnable(() -> removeAllFuture.complete(null))
			.build();
		final CompletedCheckpointStore completedCheckpointStore = createCompletedCheckpointStore(stateHandleStore);

		completedCheckpointStore.recover();
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(3));

		completedCheckpointStore.shutdown(JobStatus.CANCELLING);
		try {
			removeAllFuture.get(timeout, TimeUnit.MILLISECONDS);
			fail("We should get an expected timeout because the job is not globally terminated.");
		} catch (TimeoutException ex) {
			// expected
		}
		assertThat(removeCalledNum.get(), is(0));
		assertThat(removeAllFuture.isDone(), is(false));
		assertThat(releaseAllFuture.isDone(), is(true));
		assertThat(completedCheckpointStore.getAllCheckpoints().size(), is(0));
	}

	@Test
	public void testSubsumeAfterRecoverWithNoCheckpointInStore() throws Exception {
		int numRetain = 3;
		// no checkpoints in recover list
		List<CompletedCheckpoint> recoveredList = generateRecoveredCheckpointList(
			0,
			2,
			3,
			4);

		testCheckpointRetentionWithRecoveredList(numRetain, recoveredList);
	}

	@Test
	public void testSubsumeAfterRecoverWithCheckpointInStore() throws Exception {
		int numRetain = 3;
		// 1 checkpoint in recover list
		List<CompletedCheckpoint> recoveredList = generateRecoveredCheckpointList(
			1,
			2,
			3,
			4);
		testCheckpointRetentionWithRecoveredList(numRetain, recoveredList);
	}

	/**
	 * Test the retained completed checkpoints with recovered list.
	 * The size of the completedList is numRetain,
	 * i.e. the number of the checkpoint in the completedList is from 0 to numRetain,
	 * while the number of savepoint is numRetain - numOfCheckpoint
	 *
	 * @param numRetain The max number of retained checkpoints.
	 * @param recoveredList The completed checkpoint list recovered from zk and hdfs.
	 */
	private void testCheckpointRetentionWithRecoveredList(int numRetain, List<CompletedCheckpoint> recoveredList) throws Exception {
		// The offset of completed checkpointIds
		int baseId = recoveredList.size();
		// test of CheckpointRetention with 0 ~ numRetain checkpoint(s) in completed list after job recovery
		for (int numOfCheckpoint = 0; numOfCheckpoint <= numRetain; numOfCheckpoint++) {
			List<CompletedCheckpoint> completedList =
				generateCompletedCheckpointList(baseId, numOfCheckpoint, numRetain - numOfCheckpoint);
			testCheckpointRetentionWithRecoveredList(numRetain, recoveredList, completedList);
		}
	}

	/**
	 * Get the expected completed checkpoint list based on the recoveredList and completedList .
	 *
	 * @param numRetain The max number of retained checkpoints.
	 * @param recoveredList The completed checkpoint list recovered from zk and hdfs.
	 * @param completedList The completed checkpoint list made afer the job start.
	 * @return The expected completed checkpoint list of the checkpoint store.
	 */
	private List<CompletedCheckpoint> getExpectedRetainedCheckpointList(int numRetain, List<CompletedCheckpoint> recoveredList, List<CompletedCheckpoint> completedList) {
		List<CompletedCheckpoint> appendList = Stream.of(recoveredList, completedList).flatMap(Collection::stream).collect(Collectors.toList());

		// get the latest CHECKPOINT index of the appendList
		// latestCheckpointIndex is maintained -1 if no CHECKPOINT in the list
		int latestCheckpointIndex = -1;
		for (int i = appendList.size() - 1; i >= 0; i--) {
			if (appendList.get(i).isCheckpoint()) {
				latestCheckpointIndex = i;
				break;
			}
		}

		int n = appendList.size();
		// Case 1: the index of the latest checkpoint is in [0, n - numRetain)
		if (latestCheckpointIndex >= 0 && latestCheckpointIndex < n - numRetain) {
			List<CompletedCheckpoint> res = new ArrayList<>(appendList.subList(n - numRetain + 1, n));
			// add CHECKPOINT to the head of the list
			res.add(0, appendList.get(latestCheckpointIndex));
			return res;
		}
		// Other cases:
		// 1. the index of the latest checkpoint is in [n - numRetain, n)
		// 2. no checkpoint in the list, i.e. latestCheckpointIndex = -1
		else {
			return appendList.subList(n - numRetain, n);
		}
	}

	/**
	 * Test the retained completed checkpoints with recovered list.
	 *
	 * @param numRetain The max number of retained checkpoints.
	 * @param recoveredList The completed checkpoint list recovered from zk and hdfs.
	 * @param completedList The completed checkpoint list made afer the job start.
	 */
	private void testCheckpointRetentionWithRecoveredList(
		int numRetain,
		List<CompletedCheckpoint> recoveredList,
		List<CompletedCheckpoint> completedList)
		throws Exception {
		final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore =
			builder.setGetAllSupplier(() -> createStateHandles(3)).build();
		final CompletedCheckpointStore completedCheckpointStore =
			createCompletedCheckpointStore(stateHandleStore, numRetain);

		for (CompletedCheckpoint c : recoveredList) {
			completedCheckpointStore.addCheckpointInOrder(c);
		}

		for (CompletedCheckpoint c : completedList) {
			completedCheckpointStore.addCheckpoint(c);
		}

		List<CompletedCheckpoint> expectedRetainedList = getExpectedRetainedCheckpointList(numRetain, recoveredList, completedList);
		assertEquals(expectedRetainedList, completedCheckpointStore.getAllCheckpoints());
	}

	/**
	 * Mock user makes a list of completed checkpoints after the job restart.
	 * We ignore the order of savepoint and checkpoint but keep the num of them.
	 *
	 * @param baseId           The base checkpoint id in the completed checkpoint list.
	 * @param numOfCheckpoints The number of checkpoints after the job restart.
	 * @param numOfSavepoints  The number of savepoints.
	 * @return A CompletedCheckpoint list with numOfCheckpoints checkpoints and numOfSavepoints savepoints, and the checkpoint id starts from baseId.
	 */
	private List<CompletedCheckpoint> generateCompletedCheckpointList(int baseId, int numOfCheckpoints, int numOfSavepoints) {
		int totalCompletedCheckpoints = numOfCheckpoints + numOfSavepoints;
		List<CompletedCheckpoint> result = new ArrayList<>(totalCompletedCheckpoints);
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < totalCompletedCheckpoints; i++) {
			list.add(i);
		}
		Collections.shuffle(list);
		for (int i = 0; i < totalCompletedCheckpoints; i++) {
			// keep the checkpoint id in increasing order
			result.add(getCheckpoint(baseId + i + 1, list.get(i) >= numOfCheckpoints));
		}
		assertEquals(totalCompletedCheckpoints, result.size());
		return result;
	}

	/**
	 * Mock the completed checkpoint list which is recovered from hdfs and zk.
	 * We alse ignore the order of savepoint, checkpoint and placeholder but keep the num of them.
	 *
	 * @param numOfCheckpoints  The number of checkpoints after the job restart.
	 * @param numOfSavepoints   The number of savepoints.
	 * @param numOfPlaceholders The number of placeholders.
	 * @param numOfPlaceholders The number of placeholders with exception.
	 * @return A recovered CompletedCheckpoint list with checkpoints, savepoints and placeholders, and the checkpoint id starts from 0.
	 */
	private List<CompletedCheckpoint> generateRecoveredCheckpointList(int numOfCheckpoints, int numOfSavepoints, int numOfPlaceholders, int numPlaceholdersWithException) {
		int totalCompletedCheckpoints = numOfCheckpoints + numOfSavepoints + numOfPlaceholders + numPlaceholdersWithException;
		List<CompletedCheckpoint> result = new ArrayList<>(totalCompletedCheckpoints);
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < totalCompletedCheckpoints; i++) {
			list.add(i);
		}
		Collections.shuffle(list);
		for (int i = 0; i < totalCompletedCheckpoints; i++) {
			// keep the checkpoint id in increasing order
			if (list.get(i) < numOfCheckpoints) {    // add checkpoint
				result.add(getCheckpoint(i + 1, false));
			} else if (list.get(i) < (numOfCheckpoints + numOfSavepoints)) {    // add savepoint
				result.add(getCheckpoint(i + 1, true));
			} else if (list.get(i) < (numOfCheckpoints + numOfSavepoints + numOfPlaceholders)) {    // add placeholder
				result.add(getCheckpointPlaceHolder(i + 1, false));
			} else {        // add placeholder with exception
				result.add(getCheckpointPlaceHolderWithException(i + 1));
			}
		}
		assertEquals(totalCompletedCheckpoints, result.size());
		return result;
	}

	private CompletedCheckpoint getCheckpointPlaceHolder(long id, boolean isSavepoint) {
		return new CompletedCheckpointPlaceHolder<>(id, getCheckpoint(id, isSavepoint), value -> value);
	}

	private CompletedCheckpoint getCheckpointPlaceHolderWithException(long id) {
		return new CompletedCheckpointPlaceHolder<>(id, null, unused -> {
			throw new Exception("cannot transform placeholder");
		});
	}

	private  List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> createStateHandles(int num) {
		final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> stateHandles =
			new ArrayList<>();
		for (int i = 1; i <= num; i++) {
			final CompletedCheckpointStoreTest.TestCompletedCheckpoint completedCheckpoint =
				CompletedCheckpointStoreTest.createCheckpoint(i, new SharedStateRegistry());
			final RetrievableStateHandle<CompletedCheckpoint> checkpointStateHandle =
				checkpointStorageHelper.store(completedCheckpoint);
			stateHandles.add(new Tuple2<>(checkpointStateHandle, String.valueOf(i)));
		}
		return stateHandles;
	}

	private CompletedCheckpointStore createCompletedCheckpointStore(
		TestingStateHandleStore<CompletedCheckpoint> stateHandleStore) {
		return createCompletedCheckpointStore(stateHandleStore, 1);
	}

	private CompletedCheckpointStore createCompletedCheckpointStore(
		TestingStateHandleStore<CompletedCheckpoint> stateHandleStore, int toRetain) {
		return new DefaultCompletedCheckpointStore<>(
			toRetain,
			stateHandleStore,
			new CheckpointStoreUtil() {
				@Override
				public String checkpointIDToName(long checkpointId) {
					return String.valueOf(checkpointId);
				}

				@Override
				public long nameToCheckpointID(String name) {
					return Long.valueOf(name);
				}
			},
			executorService);
	}

	private CompletedCheckpoint getCheckpoint(long id, boolean isSavepoint) {
		return getCheckpoint(
			isSavepoint
				? CheckpointProperties.forSavepoint(false)
				: CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION),
			id);
	}

	private CompletedCheckpoint getCheckpoint(CheckpointProperties props, long id) {
		return new CompletedCheckpoint(
			new JobID(),
			id,
			0L,
			0L,
			Collections.emptyMap(),
			Collections.emptyList(),
			props,
			new TestCompletedCheckpointStorageLocation(),
			null);
	}
}
