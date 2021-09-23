/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.cache.monitor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilterSupport;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Periodically monitor the state of the heap.
 */
public class HeapStatusMonitor {
	private static final Logger LOG = LoggerFactory.getLogger(HeapStatusMonitor.class);

	/** Executor to check memory usage periodically. */
	private final ScheduledThreadPoolExecutor checkExecutor;

	/** The scheduling period of the monitored thread. */
	private final long interval;

	/** The task of monitoring periodically scheduled. */
	private final HeapMonitorTask monitorTask;

	public HeapStatusMonitor(long interval, HeapStatusListener heapStatusListener) {
		this.interval = interval;
		this.checkExecutor = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("heap-status-monitor"));
		this.checkExecutor.setRemoveOnCancelPolicy(true);
		this.checkExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.checkExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
		this.monitorTask = new HeapMonitorTask(heapStatusListener);
	}

	public void startMonitor() {
		LOG.info("Start monitoring the heap status.");
		monitorTask.registerGcListener();
		checkExecutor.scheduleWithFixedDelay(monitorTask, interval, interval, TimeUnit.MILLISECONDS);
	}

	public void shutDown() {
		LOG.info("Stop monitoring the heap status.");
		checkExecutor.shutdownNow();
		monitorTask.unregisterGcListener();
	}

	/** Collect GC information in the most recent period of time, and notify {@link HeapStatusListener} of the result. */
	private static class HeapMonitorTask implements Runnable {
		private final HeapStatusListener heapStatusListener;
		private GcNotificationListener gcListener;

		public HeapMonitorTask(HeapStatusListener heapStatusListener) {
			this.heapStatusListener = heapStatusListener;
		}

		@Override
		public void run() {
			if (gcListener != null) {
				heapStatusListener.notifyHeapStatus(gcListener.getHeapMonitorResult());
			}
		}

		/** Register the listener for gc notification. */
		public void registerGcListener() {
			this.gcListener = new GcNotificationListener();
			NotificationFilterSupport filter = new NotificationFilterSupport();
			filter.enableType(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION);
			for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
				((NotificationBroadcaster) bean).addNotificationListener(gcListener, filter, bean.getName());
			}
		}

		/** Unregister the listener for gc notification. */
		public void unregisterGcListener() {
			if (gcListener != null) {
				for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
					try {
						((NotificationEmitter) bean).removeNotificationListener(gcListener);
					} catch (ListenerNotFoundException ignore) {
						// ignore
					}
				}
				gcListener = null;
			}
		}
	}

	/**
	 * Register {@link NotificationListener} to monitor information such as memory usage, gc frequency and duration.
	 */
	@VisibleForTesting
	public static class GcNotificationListener implements NotificationListener {
		private final AtomicLong maxGcTime;
		private final AtomicLong totalGcTime;
		private final AtomicLong totalMemoryUsageAfterGc;
		private final AtomicLong gcCount;
		private final long maxMemorySize;

		public GcNotificationListener() {
			this.maxGcTime = new AtomicLong(0);
			this.totalGcTime = new AtomicLong(0);
			this.totalMemoryUsageAfterGc = new AtomicLong(0);
			this.gcCount = new AtomicLong(0);
			this.maxMemorySize = Runtime.getRuntime().maxMemory();
		}

		@Override
		public void handleNotification(Notification notification, Object handback) {
			String notifyType = notification.getType();
			if (notifyType.equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
				CompositeData cd = (CompositeData) notification.getUserData();
				GarbageCollectionNotificationInfo gcInfo = GarbageCollectionNotificationInfo.from(cd);

				gcCount.getAndAdd(1L);
				// update gc time
				long gcTime = gcInfo.getGcInfo().getDuration();
				totalGcTime.getAndAdd(gcTime);
				while (gcTime > maxGcTime.get()) {
					long currentMaxGcTime = maxGcTime.get();
					if (gcTime > currentMaxGcTime) {
						maxGcTime.compareAndSet(currentMaxGcTime, gcTime);
					}
				}
				// update memory usage
				long memoryUsage = 0L;
				for (Map.Entry<String, MemoryUsage> entry : gcInfo.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
					memoryUsage += entry.getValue().getUsed();
				}
				totalMemoryUsageAfterGc.getAndAdd(memoryUsage);
			}
		}

		public HeapMonitorResult getHeapMonitorResult() {
			return new HeapMonitorResult(
				maxGcTime.getAndSet(0L),
				totalGcTime.getAndSet(0L),
				totalMemoryUsageAfterGc.getAndSet(0L),
				gcCount.getAndSet(0L),
				maxMemorySize);
		}
	}
}
