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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * Test for local result partition resolver.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({YarnLocalResultPartitionResolver.class})
public class YarnLocalResultPartitionResolverTest {
	private static final Logger LOG = LoggerFactory.getLogger(YarnLocalResultPartitionResolverTest.class);

	private static final long UNCONSUMED_PARTITION_TTL = 600000;

	private static final long UNFINISHED_PARTITION_TTL = 300000;

	private static final long CUSTOMIZED_UNCONSUMED_PARTITION_TTL = 160000;

	private static final long CUSTOMIZED_UNFINISHED_PARTITION_TTL = 150000;

	private static final FileSystem FILE_SYSTEM = FileSystem.getLocalFileSystem();

	private final ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration =
		mock(ExternalBlockShuffleServiceConfiguration.class);

	private YarnLocalResultPartitionResolver resultPartitionResolver;

	private final int localDirCnt = 3;

	private String testRootDir;

	private ConcurrentHashMap<String, String> appIdToUser;

	private HashMap<ResultPartitionID, String> resultPartitionIDToAppId;

	private HashMap<ResultPartitionID, HashMap<Integer, Tuple2<String, String>>> resultPartitionIDToLocalDir;

	enum ResultPartitionState {
		UNDEFINED,
		UNCONSUMED,
		CONSUMED
	}

	/** Deque should be used as a stack due to case design. */
	private HashMap<ResultPartitionState, Deque<ResultPartitionID>> stateToResultPartitionIDs;

	private Set<ResultPartitionID> removedResultPartitionIDs;

	private Set<ResultPartitionID> toBeRemovedResultPartitionIDs;

	@Before
	public void setup() throws IOException {
		appIdToUser = new ConcurrentHashMap<>();
		resultPartitionIDToAppId = new HashMap<>();
		resultPartitionIDToLocalDir = new HashMap<>();
		stateToResultPartitionIDs = new HashMap<ResultPartitionState, Deque<ResultPartitionID>>() {{
			for (ResultPartitionState value : ResultPartitionState.values()) {
				put(value, new ArrayDeque<>());
			}
		}};
		removedResultPartitionIDs = new HashSet<>();
		toBeRemovedResultPartitionIDs = new HashSet<>();

		Configuration configuration = new Configuration();
		when(externalBlockShuffleServiceConfiguration.getConfiguration()).thenReturn(configuration);
		when(externalBlockShuffleServiceConfiguration.getFileSystem()).thenReturn(FILE_SYSTEM);
		when(externalBlockShuffleServiceConfiguration.getDiskScanIntervalInMS()).thenReturn(3600000L);
		when(externalBlockShuffleServiceConfiguration.getDefaultUnconsumedPartitionTTL()).thenReturn(UNCONSUMED_PARTITION_TTL);
		when(externalBlockShuffleServiceConfiguration.getDefaultUnfinishedPartitionTTL()).thenReturn(UNFINISHED_PARTITION_TTL);

		checkArgument(UNFINISHED_PARTITION_TTL < UNCONSUMED_PARTITION_TTL,
			"UNFINISHED_PARTITION_TTL should be less than UNFINISHED_PARTITION_TTL to test recycling");

		checkArgument(CUSTOMIZED_UNFINISHED_PARTITION_TTL < UNFINISHED_PARTITION_TTL,
			"The customized UNFINISHED_PARTITION_TTL should be less than the default one to test the customized TTL");
		checkArgument(CUSTOMIZED_UNCONSUMED_PARTITION_TTL < UNCONSUMED_PARTITION_TTL,
			"The customized UNCONSUMED_PARTITION_TTL should be less than the default one to test the customized TTL");
		checkArgument(CUSTOMIZED_UNFINISHED_PARTITION_TTL < UNCONSUMED_PARTITION_TTL,
			"The customized CUSTOMIZED_UNFINISHED_PARTITION_TTL should be less than the customized UNCONSUMED_PARTITION_TTL to " +
				"test the customized TTL");

		this.testRootDir = System.getProperty("java.io.tmpdir");
		if (!System.getProperty("java.io.tmpdir").endsWith("/")) {
			this.testRootDir += "/";
		}
		this.testRootDir += "yarn_shuffle_test_" + UUID.randomUUID().toString() + "/";
		FILE_SYSTEM.mkdirs(new Path(testRootDir));
		assertTrue("Fail to create testRootDir: " + testRootDir, FILE_SYSTEM.exists(new Path(testRootDir)));

		String localDirPrefix = "localDir";
		Map<String, String> dirToDiskType = new HashMap<>();
		for (int i = 0; i < localDirCnt; i++) {
			String localDir = testRootDir + localDirPrefix + i + "/";
			dirToDiskType.put(localDir, "SSD");
			FILE_SYSTEM.mkdirs(new Path(localDir));
			assertTrue("Fail to create local dir: " + localDir, FILE_SYSTEM.exists(new Path(localDir)));
		}
		when(externalBlockShuffleServiceConfiguration.getDirToDiskType()).thenReturn(dirToDiskType);

		mockStatic(System.class);
		// Mock container-executor.
		when(System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key())).thenReturn(testRootDir);
		String containerExecutorPath = testRootDir + "bin/container-executor";
		configuration.setString(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, containerExecutorPath);
		FILE_SYSTEM.create(new Path(containerExecutorPath), FileSystem.WriteMode.OVERWRITE);
		assertTrue("Fail to mock container-executor: " + containerExecutorPath,
			FILE_SYSTEM.exists(new Path(containerExecutorPath)));

		createYarnLocalResultPartitionResolver();

		LOG.debug("Configurations for YarnLocalResultPartitionResolverTest:\n\tTest root dir: " + testRootDir);
	}

	@After
	public void tearDown() {
		if (resultPartitionResolver != null) {
			resultPartitionResolver.stop();
		}
	}

	@Rule
	public TestWatcher watchman = new TestWatcher() {
		@Override
		protected void succeeded(Description description) {
			// Do recycle after test cases pass.
			try {
				Path testRootDirPath = new Path(testRootDir);
				if (FILE_SYSTEM.exists(testRootDirPath)) {
					FILE_SYSTEM.delete(testRootDirPath, true);
				}
			} catch (IOException e) {
				// Do nothing
			}

			super.succeeded(description);
		}

		@Override
		protected void failed(Throwable e, Description description) {
			// Leave result partition directories for debugging.
			super.failed(e, description);
		}
	};

	@Ignore
	public void testBasicProcess() {
		int userCnt = 2;
		int appCnt = 3;
		int resultPartitionCnt = 6;
		int subPartitionCnt = 2;

		generateAppIdToUser(userCnt, appCnt);
		createResultPartitionIDs(resultPartitionCnt);
		long currTime = 1L;

		// 1. NM will call initializeApplication() before launching containers.
		appIdToUser.forEach((app, user) -> {
			resultPartitionResolver.initializeApplication(user, app);
		});
		assertEquals(appIdToUser, resultPartitionResolver.appIdToUser);

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMeta(0, subPartitionCnt);

		// 2. create all result sub partition files
		createResultSubPartitions(subPartitionCnt);

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMeta(resultPartitionCnt, subPartitionCnt);

		// 3. one result partition is finished, the files are removed
		removeResultPartitions(1);

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMeta(resultPartitionCnt - 1, subPartitionCnt);

		// 4. set one result partition to be removed, disk scan will clear it.
		triggerRecycleConsumedResultPartition(1);

		when(System.currentTimeMillis()).thenReturn(++currTime);
		triggerDiskScan();
		validateResultPartitionMeta(resultPartitionCnt - 2, subPartitionCnt);

		// 5. Stops applications one by one.
		Set<ResultPartitionID> remainingResultPartitions = new HashSet<>();
		for (String appId : resultPartitionIDToAppId.values()) {

			LOG.debug("StopApplication: " + appId);
			remainingResultPartitions.addAll(resultPartitionResolver.stopApplication(appId));
		}
		assertEquals(resultPartitionCnt - 2, remainingResultPartitions.size());
	}

	@Test
	public void testGetResultSubPartitionFileInfo() throws Exception {
		int userCnt = 2;
		int appCnt = 3;
		int resultPartitionCnt = 6;

		generateAppIdToUser(userCnt, appCnt);
		createResultPartitionIDs(resultPartitionCnt);
		long currTime = 1L;

		// 1. NM will call initializeApplication() before launching containers.
		appIdToUser.forEach((app, user) -> {
			resultPartitionResolver.initializeApplication(user, app);
		});
		assertEquals(appIdToUser, resultPartitionResolver.appIdToUser);

		// 2. Prepares external result partitions.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		createResultSubPartitions(2);

		// 3. Searches an unknown result partition.
		when(System.currentTimeMillis()).thenReturn(++currTime);
		try {
			ResultPartitionID newResultPartitionID = new ResultPartitionID();
			LocalResultPartitionResolver.ResultSubPartitionFileInfo descriptor =
				resultPartitionResolver.getResultSubPartitionFileInfo(newResultPartitionID, 0);
			assertTrue("Expect PartitionNotFoundException to be thrown out.", false);
		} catch (PartitionNotFoundException e){
			// Do nothing.
		} catch (IOException e) {
			assertTrue("Unexpected IOException", false);
		}

		validateResultPartitionMeta(0, 2);

		ResultPartitionID resultPartitionID = resultPartitionIDToAppId.keySet().iterator().next();
		LocalResultPartitionResolver.ResultSubPartitionFileInfo fileInfo =
				resultPartitionResolver.getResultSubPartitionFileInfo(resultPartitionID, 0);
		assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID).get(0).f0, fileInfo.getRootDir());
		assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID).get(0).f1, fileInfo.getSubPartitionFile());

	}

	// ************************************** Test Utilities ********************************************/

	private void createYarnLocalResultPartitionResolver() {
		resultPartitionResolver = spy(new YarnLocalResultPartitionResolver(externalBlockShuffleServiceConfiguration));
	}

	void triggerDiskScan() {
		resultPartitionResolver.doDiskScan();
	}

	private void generateAppIdToUser(int userCnt, int appCnt) {
		for (int i = 0; i < appCnt; i++) {
			String user = "user" + (i % userCnt);
			String appId = "flinkStreaming" + i;
			appIdToUser.put(appId, user);

			// Prepare app's local dirs
			String relativeAppDir = YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(user, appId);
			externalBlockShuffleServiceConfiguration.getDirToDiskType().keySet().forEach(localDir -> {
				Path appLocalDir = new Path(localDir + relativeAppDir);
				try {
					FILE_SYSTEM.mkdirs(appLocalDir);
					assertTrue("Fail to mkdir for appLocalDir " + appLocalDir, FILE_SYSTEM.exists(appLocalDir));
				} catch (IOException e) {
					assertTrue("Caught except when mkdir for appLocalDir " + appLocalDir, false);
				}
			});

		}
	}

	private void createResultPartitionIDs(int resultPartitionCnt) {
		String[] appArray = appIdToUser.keySet().toArray(new String[appIdToUser.size()]);
		for (int i = 0; i < resultPartitionCnt; i++) {
			ResultPartitionID resultPartitionID = new ResultPartitionID();
			resultPartitionIDToAppId.put(resultPartitionID, appArray[i % appArray.length]);
			stateToResultPartitionIDs.get(ResultPartitionState.UNDEFINED).offerLast(resultPartitionID);
		}
	}

	private Set<ResultPartitionID> pollResultPartitionIDS(ResultPartitionState state, int cnt) {
		Set<ResultPartitionID> resultPartitionIDS = new HashSet();
		Deque<ResultPartitionID> totalResultPartitionIDS = stateToResultPartitionIDs.get(state);
		while (!totalResultPartitionIDS.isEmpty() && (resultPartitionIDS.size() < cnt)) {
			// Treat deque as a stack.
			resultPartitionIDS.add(totalResultPartitionIDS.pollLast());
		}
		assertEquals(cnt, resultPartitionIDS.size());
		return resultPartitionIDS;
	}

	private void createResultSubPartitions(
		int subPartitionCnt) {

		Set<ResultPartitionID> resultPartitionIDS = resultPartitionIDToAppId.keySet();
		String[] rootDirs = externalBlockShuffleServiceConfiguration.getDirToDiskType().keySet().toArray(
			new String[externalBlockShuffleServiceConfiguration.getDirToDiskType().size()]);
		Random random = new Random();
		resultPartitionIDS.forEach(resultPartitionID -> {
			LOG.debug("Creating result partition: " + resultPartitionID);
			String selectedRootDir = rootDirs[Math.abs(random.nextInt()) % rootDirs.length];
			String appId = resultPartitionIDToAppId.get(resultPartitionID);
			String user = appIdToUser.get(appId);
			String resultPartitionDir = ExternalBlockShuffleUtils.generatePartitionRootPath(
				selectedRootDir + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(user, appId),
				resultPartitionID);
			resultPartitionIDToLocalDir.put(resultPartitionID, new HashMap<>());
			try {
				for (int i = 0; i < subPartitionCnt; i++) {
					Path subPartitionFile = new Path(ExternalBlockShuffleUtils.generateSubPartitionFile(resultPartitionDir, i));
					resultPartitionIDToLocalDir.get(resultPartitionID)
							.put(i, new Tuple2<>(selectedRootDir, subPartitionFile.getPath()));
					FILE_SYSTEM.create(subPartitionFile, FileSystem.WriteMode.OVERWRITE);
					assertTrue("ResultSubPartition file should exist.", FILE_SYSTEM.exists(subPartitionFile));
				}
			} catch (Exception e) {
				assertTrue("Fail to generate result sub partition file in " + selectedRootDir + ", exception: " + e.getMessage(), false);
			}
		});
	}

	private void removeResultPartitions(int numOfPartition) {
		Iterator<ResultPartitionID> resultPartitionIDS = resultPartitionIDToAppId.keySet().iterator();
		for (int i = 0; i < numOfPartition; i++) {
			while (resultPartitionIDS.hasNext()) {
				ResultPartitionID resultPartitionID = resultPartitionIDS.next();
				if (!resultPartitionResolver.resultPartitionMap.get(resultPartitionID).isEmpty()) {
					LOG.debug("Remove result partition: " + resultPartitionID);
					removedResultPartitionIDs.add(resultPartitionID);
					try {
						Tuple2<String, String> rootDirAndPartitionDir = resultPartitionIDToLocalDir.get(resultPartitionID).get(0);
						FILE_SYSTEM.delete(new Path(rootDirAndPartitionDir.f1).getParent(), true);
						assertTrue("Fail to delete result partition dir " + rootDirAndPartitionDir.f1,
							!FILE_SYSTEM.exists(new Path(rootDirAndPartitionDir.f1)));
					} catch (IOException e) {
						// Do nothing.
					}
					break;
				}
			}
		}
	}

	private void triggerRecycleConsumedResultPartition(int numOfPartition) {
		Iterator<ResultPartitionID> resultPartitionIDS = resultPartitionIDToAppId.keySet().iterator();
		for (int i = 0; i < numOfPartition; i++) {
			while (resultPartitionIDS.hasNext()) {
				ResultPartitionID resultPartitionID = resultPartitionIDS.next();
				if (!resultPartitionResolver.resultPartitionMap.get(resultPartitionID).isEmpty()) {
					LOG.debug("Transit from CONSUMED to RECYCLED(REMOVED): " + resultPartitionID);
					toBeRemovedResultPartitionIDs.add(resultPartitionID);
					resultPartitionResolver.recycleResultPartition(resultPartitionID);
					break;
				}
			}
		}
	}

	private void validateResultPartitionMeta(int expectedNumOfPartition, int expectedNumOfSubPartition) {
		int numOfPartition = 0;
		for (ResultPartitionID resultPartitionID : resultPartitionIDToAppId.keySet()) {
			ConcurrentHashMap<Integer, YarnLocalResultPartitionResolver.YarnResultSubPartitionFileInfo> subPartitionToFileInfo =
					resultPartitionResolver.resultPartitionMap.get(resultPartitionID);
			if (subPartitionToFileInfo != null && !subPartitionToFileInfo.isEmpty()) {
				numOfPartition++;
				assertEquals(expectedNumOfSubPartition, subPartitionToFileInfo.size());
				for (int i = 0; i < expectedNumOfSubPartition; i++) {
					YarnLocalResultPartitionResolver.YarnResultSubPartitionFileInfo fileInfo =
							subPartitionToFileInfo.get(i);

					assertTrue("Resolver should find the directory for " + resultPartitionID + ":" + i, fileInfo != null);
					assertTrue(resultPartitionID.toString(), fileInfo.isReadyToBeConsumed());
					assertTrue(resultPartitionID.toString(), !fileInfo.isConsumed());
					assertTrue(resultPartitionID.toString(), !fileInfo.needToDelete());
					assertTrue(resultPartitionID.toString(), fileInfo.getFileInfoTimestamp() > 1L);
					assertTrue(resultPartitionID.toString(), fileInfo.getPartitionReadyTime() > 1L);
					assertEquals(resultPartitionIDToAppId.get(resultPartitionID), fileInfo.getAppId());
					assertEquals(resultPartitionIDToLocalDir.get(resultPartitionID).get(i), fileInfo.getRootDirAndPartitionDir());
				}
			}
		}
		assertEquals(numOfPartition, expectedNumOfPartition);
	}

}
