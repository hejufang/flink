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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

/**
 * Test for ExternalBlockResultPartitionManager.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExternalBlockResultPartitionManager.class, LocalResultPartitionResolverFactory.class})
public class ExternalBlockResultPartitionManagerTest {
	private final ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration =
		mock(ExternalBlockShuffleServiceConfiguration.class);

	private ExternalBlockResultPartitionManager resultPartitionManager;

	private final LocalResultPartitionResolver localResultPartitionResolver = mock(LocalResultPartitionResolver.class);

	private final Map<String, String> dirToDiskType;

	private final Map<String, Integer> diskTypeToIOThreadNum;

	private final long consumedPartitionTTL = 600000L;

	private final long partialConsumedPartitionTTL = 3600000L;

	/** Map from ResultPartitionID to root Directory and result partition directory. */
	private final Map<ResultPartitionID, LocalResultPartitionResolver.ResultSubPartitionFileInfo> resultPartitionFileInfoMap = new HashMap<>();

	public ExternalBlockResultPartitionManagerTest() {
		this.dirToDiskType = new HashMap<String, String>() {{
			put("/local-dir1/", "SSD");
			put("/local-dir2/", "SSD");
			put("/local-dir3/", "MOCK_DISK_TYPE");
			put("/local-dir4/", ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE);
		}};

		this.diskTypeToIOThreadNum = new HashMap<String, Integer>() {{
			put("SSD", 30);
			put("MOCK_DISK_TYPE", 4);
			put(ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE, 1);
		}};
	}

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		when(externalBlockShuffleServiceConfiguration.getConfiguration()).thenReturn(configuration);
		when(externalBlockShuffleServiceConfiguration.getDiskScanIntervalInMS()).thenReturn(3600000L);
		when(externalBlockShuffleServiceConfiguration.getBufferNumber()).thenReturn(100);
		when(externalBlockShuffleServiceConfiguration.getMemorySizePerBufferInBytes()).thenReturn(4096);
		when(externalBlockShuffleServiceConfiguration.getDirToDiskType()).thenReturn(dirToDiskType);
		when(externalBlockShuffleServiceConfiguration.getDiskTypeToIOThreadNum()).thenReturn(diskTypeToIOThreadNum);
		when(externalBlockShuffleServiceConfiguration.getWaitCreditDelay()).thenReturn(2L);
		when(externalBlockShuffleServiceConfiguration.getDefaultConsumedPartitionTTL()).thenReturn(consumedPartitionTTL);
		when(externalBlockShuffleServiceConfiguration.getDefaultPartialConsumedPartitionTTL()).thenReturn(partialConsumedPartitionTTL);
		when(externalBlockShuffleServiceConfiguration.getSelfCheckIntervalInMS()).thenReturn(3600000L);

		mockStatic(System.class);

		mockStatic(LocalResultPartitionResolverFactory.class);
		when(LocalResultPartitionResolverFactory.create(any(ExternalBlockShuffleServiceConfiguration.class)))
			.thenReturn(localResultPartitionResolver);
		doAnswer(invocation -> {
			ResultPartitionID resultPartitionID = invocation.getArgument(0);
			LocalResultPartitionResolver.ResultSubPartitionFileInfo descriptor = resultPartitionFileInfoMap.get(resultPartitionID);

			if (descriptor != null) {
				return descriptor;
			} else {
				throw new IOException("Cannot find result partition " + resultPartitionID);
			}
		}).when(localResultPartitionResolver).getResultSubPartitionFileInfo(any(ResultPartitionID.class), any(Integer.class));

		resultPartitionManager = spy(new ExternalBlockResultPartitionManager(externalBlockShuffleServiceConfiguration));
	}

	@After
	public void tearDown() {
		if (resultPartitionManager != null) {
			resultPartitionManager.stop();
		}
	}

	@Test
	public void testConstructor() {
		assertEquals(externalBlockShuffleServiceConfiguration.getBufferNumber(),
			(Integer) resultPartitionManager.bufferPool.getNumBuffers());

		dirToDiskType.forEach((dir, diskType) -> {
			Integer expectedTheadNum = diskTypeToIOThreadNum.get(diskType);
			assertTrue("Thread pool for dir " + dir, resultPartitionManager.dirToThreadPool.containsKey(dir));
			assertEquals(expectedTheadNum, (Integer) resultPartitionManager.dirToThreadPool.get(dir).getCorePoolSize());
		});
	}

	@Test
	public void testInitializeAndStopApplication() {
		resultPartitionManager.initializeApplication("user", "flinkStreamingJob1");
		verify(localResultPartitionResolver, times(1))
			.initializeApplication("user", "flinkStreamingJob1");
		resultPartitionManager.initializeApplication("user", "flinkStreamingJob2");
		verify(localResultPartitionResolver, times(1))
			.initializeApplication("user", "flinkStreamingJob2");

		when(localResultPartitionResolver.stopApplication("flinkStreamingJob1"))
			.thenReturn(Collections.EMPTY_SET);
		resultPartitionManager.stopApplication("flinkStreamingJob1");
		verify(localResultPartitionResolver, times(1)).stopApplication("flinkStreamingJob1");
		when(localResultPartitionResolver.stopApplication("flinkStreamingJob2"))
			.thenReturn(Collections.EMPTY_SET);
		resultPartitionManager.stopApplication("flinkStreamingJob2");
		verify(localResultPartitionResolver, times(1)).stopApplication("flinkStreamingJob2");
	}

	@Test
	public void testBasicProcess() throws Exception {
		assertEquals(0, resultPartitionManager.resultPartitionMetaMap.size());

		// Tests the creation of result partition meta.
		createResultPartitions(6);
		resultPartitionFileInfoMap.forEach((resultPartitionID, rootDirAndPartitionDir) -> {
			assertTrue(resultPartitionID.toString(),
				!resultPartitionManager.resultPartitionMetaMap.contains(resultPartitionID));
			ResultSubpartitionView resultSubpartitionView = null;
			try {
				resultSubpartitionView = resultPartitionManager.createSubpartitionView(
					resultPartitionID, 0, mock(BufferAvailabilityListener.class));
				assertTrue(resultSubpartitionView != null);
			} catch (IOException e) {
				assertTrue("Unexpected exception: " + e, false);
			}
			assertTrue(resultPartitionID.toString(),
				resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
			ExternalBlockResultPartitionMeta resultPartitionMeta =
				resultPartitionManager.resultPartitionMetaMap.get(resultPartitionID);
			assertEquals(1, resultPartitionMeta.getReferenceCount());
		});

		// Tests reference count.
		ResultPartitionID resultPartitionID = resultPartitionFileInfoMap.keySet().iterator().next();
		for (int i = 0; i < 5; i++) {
			try {
				ResultSubpartitionView resultSubpartitionView = resultPartitionManager.createSubpartitionView(
					resultPartitionID, 0, mock(BufferAvailabilityListener.class));
				assertTrue(resultSubpartitionView != null);
			} catch (IOException e) {
				assertTrue("Unexpected exception: " + e, false);
			}
			assertTrue(resultPartitionID.toString(),
				resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
			ExternalBlockResultPartitionMeta resultPartitionMeta =
				resultPartitionManager.resultPartitionMetaMap.get(resultPartitionID);
			assertEquals(2 + i, resultPartitionMeta.getReferenceCount());
		}
	}

	@Test
	public void testRecycleByTTL() {
		long baseTime = 0;
		int cntPartition = 6;

		createResultPartitions(cntPartition);
		for (ResultPartitionID resultPartitionID : resultPartitionFileInfoMap.keySet()) {
			assertTrue(resultPartitionID.toString(),
				!resultPartitionManager.resultPartitionMetaMap.contains(resultPartitionID));
			ResultSubpartitionView resultSubpartitionView = null;
			try {
				resultSubpartitionView = resultPartitionManager.createSubpartitionView(
					resultPartitionID, 0, mock(BufferAvailabilityListener.class));
				assertTrue(resultSubpartitionView != null);
				resultSubpartitionView.releaseAllResources();
				baseTime = resultPartitionManager.resultPartitionMetaMap.get(resultPartitionID).getLastActiveTimeInMs();
			} catch (IOException e) {
				assertTrue("Unexpected exception: " + e, false);
			}
		}

		triggerRecycling();
		resultPartitionFileInfoMap.forEach((resultPartitionID, fileInfo) -> {
			assertTrue("ResultPartition should not be recycled, " + resultPartitionID,
				resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
		});
		verify(localResultPartitionResolver, never()).recycleResultPartition(any(ResultPartitionID.class));

		when(System.currentTimeMillis()).thenReturn(baseTime + consumedPartitionTTL + 1);
		triggerRecycling();
		resultPartitionFileInfoMap.forEach((resultPartitionID, fileInfo) -> {
			assertTrue("ResultPartition should be recycled, " + resultPartitionID,
					!resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
		});
		verify(localResultPartitionResolver, times(cntPartition)).recycleResultPartition(any(ResultPartitionID.class));
	}

	// ******************************** Test Utilities *************************************

	private void createResultPartitions(int cnt) {
		String[] localDirArray = dirToDiskType.keySet().toArray(new String[dirToDiskType.size()]);
		for (int i = 0; i < cnt; i++) {
			Random random = new Random();
			String localDir = localDirArray[Math.abs(random.nextInt()) % localDirArray.length];
			// We don't really care about root dir and partition dir since we don't test read/write in this unittest.
			resultPartitionFileInfoMap.put(new ResultPartitionID(),
				new MockResultSubPartitionFileInfo(localDir, localDir + "partition" + i + "/", consumedPartitionTTL, partialConsumedPartitionTTL));
		}
	}

	void triggerRecycling() {
		resultPartitionManager.recycleResultPartitions();
	}

	static class MockResultSubPartitionFileInfo implements LocalResultPartitionResolver.ResultSubPartitionFileInfo {
		private final String rootDir;
		private final String partitionDir;
		private final long consumedPartitionTTL;
		private final long partialConsumedPartitionTTL;

		public MockResultSubPartitionFileInfo(String rootDir, String partitionDir, long consumedPartitionTTL, long partialConsumedPartitionTTL) {
			this.rootDir = rootDir;
			this.partitionDir = partitionDir;
			this.consumedPartitionTTL = consumedPartitionTTL;
			this.partialConsumedPartitionTTL = partialConsumedPartitionTTL;
		}

		public String getRootDir() {
			return rootDir;
		}

		public String getSubPartitionFile() {
			return partitionDir;
		}

		public long getConsumedPartitionTTL() {
			return consumedPartitionTTL;
		}

		public long getPartialConsumedPartitionTTL() {
			return partialConsumedPartitionTTL;
		}

		@Override
		public String getAbsolutePath() {
			return rootDir + "/" + partitionDir;
		}
	}
}
