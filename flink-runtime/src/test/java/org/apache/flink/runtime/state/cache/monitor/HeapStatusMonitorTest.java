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

package org.apache.flink.runtime.state.cache.monitor;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import sun.management.GarbageCollectionNotifInfoCompositeData;

import javax.management.Notification;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.Map;

/**
 * Test that HeapStatusMonitor handles GC notification correctly.
 */
public class HeapStatusMonitorTest {

	@Test
	public void testGCListenerUpdateCorrectly() {
		HeapStatusMonitor.GcNotificationListener gcListener = new HeapStatusMonitor.GcNotificationListener();

		// first period: only trigger the GC once.
		Map<String, MemoryUsage> memoryUsageAfterGc = new HashMap<>(2);
		memoryUsageAfterGc.put("young", new MemoryUsage(512L, 512L, 2048L, 2048L));
		memoryUsageAfterGc.put("old", new MemoryUsage(2048L, 1024L, 4096L, 4096L));
		gcListener.handleNotification(createGCNotification(100L, memoryUsageAfterGc), new Object());

		HeapMonitorResult result = gcListener.getHeapMonitorResult();
		checkResult(1, 100L, 100L, 100L, 1536, 1536, result);

		// second period: trigger GC twice.
		memoryUsageAfterGc.clear();
		memoryUsageAfterGc.put("young", new MemoryUsage(512L, 600, 2048L, 2048L));
		memoryUsageAfterGc.put("old", new MemoryUsage(2048L, 1200L, 4096L, 4096L));
		gcListener.handleNotification(createGCNotification(80L, memoryUsageAfterGc), new Object());

		memoryUsageAfterGc.clear();
		memoryUsageAfterGc.put("young", new MemoryUsage(512L, 700, 2048L, 2048L));
		memoryUsageAfterGc.put("old", new MemoryUsage(2048L, 1300L, 4096L, 4096L));
		gcListener.handleNotification(createGCNotification(90L, memoryUsageAfterGc), new Object());
		result = gcListener.getHeapMonitorResult();
		checkResult(2, 85L, 90L, 170L, 1900, 3800, result);
	}

	private void checkResult(long gcCount, long avgGcTime, long maxGcTime, long totalGcTime, long avgMemoryUsageAfterGc, long totalMemoryUsageAfterGc, HeapMonitorResult result) {
		Assert.assertEquals(gcCount, result.getGcCount());
		Assert.assertEquals(avgGcTime, result.getAvgGcTime());
		Assert.assertEquals(maxGcTime, result.getMaxGcTime());
		Assert.assertEquals(totalGcTime, result.getTotalGcTime());
		Assert.assertEquals(avgMemoryUsageAfterGc, result.getAvgMemoryUsageAfterGc());
		Assert.assertEquals(totalMemoryUsageAfterGc, result.getTotalMemoryUsageAfterGc());
	}

	private Notification createGCNotification(long gcTime, Map<String, MemoryUsage> memoryUsageAfterGc) {
		GcInfo gcInfo = Mockito.mock(GcInfo.class);
		Mockito.when(gcInfo.getDuration()).thenReturn(gcTime);
		Mockito.when(gcInfo.getMemoryUsageAfterGc()).thenReturn(memoryUsageAfterGc);
		GarbageCollectionNotificationInfo gcNotificationInfo = new GarbageCollectionNotificationInfo("gcName", "gcAction", "gcCause", gcInfo);
		GarbageCollectionNotifInfoCompositeData gcInfoCompositeData = new GarbageCollectionNotifInfoCompositeData(gcNotificationInfo);
		Notification notification = new Notification(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION, new Object(), 1L);
		notification.setUserData(gcInfoCompositeData);
		return notification;
	}
}
