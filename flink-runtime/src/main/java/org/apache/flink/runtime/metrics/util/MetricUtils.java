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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.ClientMetricGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.ProcessMetricGroup;
import org.apache.flink.runtime.metrics.groups.SqlGatewayMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.taskexecutor.slot.SlotNotFoundException;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.metrics.util.SystemResourcesMetricsInitializer.instantiateSystemMetrics;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to register pre-defined metric sets.
 */
public class MetricUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);
	private static final String METRIC_GROUP_STATUS_NAME = "Status";
	private static final String METRICS_ACTOR_SYSTEM_NAME = "flink-metrics";

	static final String METRIC_GROUP_HEAP_NAME = "Heap";
	static final String METRIC_GROUP_NONHEAP_NAME = "NonHeap";
	static final String METRIC_GROUP_METASPACE_NAME = "Metaspace";

	@VisibleForTesting
	static final String METRIC_GROUP_FLINK = "Flink";

	@VisibleForTesting
	static final String METRIC_GROUP_MEMORY = "Memory";

	@VisibleForTesting
	static final String METRIC_GROUP_MANAGED_MEMORY = "Managed";

	private static final int DEFAULT_MONITOR_INTERVAL = 30_000;

	private static final ReentrantLock PROCESS_CPU_LOAD_LOCK = new ReentrantLock();
	private static double processCpuLoad;
	private static long currentCpuTime;
	private static long prevCpuTime;
	private static long currentTimestamp;
	private static long prevTimestamp;

	private MetricUtils() {
	}

	public static ProcessMetricGroup instantiateProcessMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname,
			final Optional<Time> systemResourceProbeInterval) {
		return instantiateProcessMetricGroup(metricRegistry, hostname, systemResourceProbeInterval, false, DEFAULT_MONITOR_INTERVAL);
	}

	public static ProcessMetricGroup instantiateProcessMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname,
			final Optional<Time> systemResourceProbeInterval,
			boolean cpuFineGrainedMonitorEnabled,
			int cpuFineGrainedMonitorInterval) {
		final ProcessMetricGroup processMetricGroup = ProcessMetricGroup.create(metricRegistry, hostname);

		createAndInitializeStatusMetricGroup(processMetricGroup, cpuFineGrainedMonitorEnabled, cpuFineGrainedMonitorInterval);

		systemResourceProbeInterval.ifPresent(interval -> instantiateSystemMetrics(processMetricGroup, interval));

		return processMetricGroup;
	}

	public static JobManagerMetricGroup instantiateJobManagerMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname) {
		final JobManagerMetricGroup jobManagerMetricGroup = new JobManagerMetricGroup(
			metricRegistry,
			hostname);

		return jobManagerMetricGroup;
	}

	public static Tuple2<TaskManagerMetricGroup, MetricGroup> instantiateTaskManagerMetricGroup(
			MetricRegistry metricRegistry,
			String hostName,
			ResourceID resourceID,
			Optional<Time> systemResourceProbeInterval) {
		return instantiateTaskManagerMetricGroup(metricRegistry, hostName, resourceID, systemResourceProbeInterval,
			false, DEFAULT_MONITOR_INTERVAL);
	}

	public static Tuple2<TaskManagerMetricGroup, MetricGroup> instantiateTaskManagerMetricGroup(
			MetricRegistry metricRegistry,
			String hostName,
			ResourceID resourceID,
			Optional<Time> systemResourceProbeInterval,
			boolean cpuFineGrainedMonitorEnabled,
			int cpuFineGrainedMonitorInterval) {
		final TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(
			metricRegistry,
			hostName,
			resourceID.toString());

		MetricGroup statusGroup = createAndInitializeStatusMetricGroup(taskManagerMetricGroup, cpuFineGrainedMonitorEnabled, cpuFineGrainedMonitorInterval);

		if (systemResourceProbeInterval.isPresent()) {
			instantiateSystemMetrics(taskManagerMetricGroup, systemResourceProbeInterval.get());
		}
		return Tuple2.of(taskManagerMetricGroup, statusGroup);
	}

	public static SqlGatewayMetricGroup instantiateSqlGatewayMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostName,
			final Optional<Time> systemResourceProbeInterval) {
		return instantiateSqlGatewayMetricGroup(metricRegistry, hostName, systemResourceProbeInterval, false, DEFAULT_MONITOR_INTERVAL);
	}

	public static SqlGatewayMetricGroup instantiateSqlGatewayMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostName,
			final Optional<Time> systemResourceProbeInterval,
			boolean cpuFineGrainedMonitorEnabled,
			int cpuFineGrainedMonitorInterval) {
		final SqlGatewayMetricGroup sqlGatewayMetricGroup = new SqlGatewayMetricGroup(
			metricRegistry,
			hostName
		);
		createAndInitializeStatusMetricGroup(sqlGatewayMetricGroup, cpuFineGrainedMonitorEnabled, cpuFineGrainedMonitorInterval);
		systemResourceProbeInterval.ifPresent(
			interval -> instantiateSystemMetrics(sqlGatewayMetricGroup, interval));
		return sqlGatewayMetricGroup;
	}

	private static MetricGroup createAndInitializeStatusMetricGroup(AbstractMetricGroup<?> parentMetricGroup) {
		return createAndInitializeStatusMetricGroup(parentMetricGroup, false, DEFAULT_MONITOR_INTERVAL);
	}

	private static MetricGroup createAndInitializeStatusMetricGroup(
			AbstractMetricGroup<?> parentMetricGroup,
			boolean cpuFineGrainedMonitorEnabled,
			int cpuFineGrainedMonitorInterval) {
		MetricGroup statusGroup = parentMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);

		instantiateStatusMetrics(statusGroup, cpuFineGrainedMonitorEnabled, cpuFineGrainedMonitorInterval);
		return statusGroup;
	}

	public static ClientMetricGroup instantiateClientMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname) {

		return new ClientMetricGroup(
			metricRegistry,
			hostname);
	}

	public static void instantiateStatusMetrics(
			MetricGroup metricGroup) {
		instantiateStatusMetrics(metricGroup, false, DEFAULT_MONITOR_INTERVAL);
	}

	public static void instantiateStatusMetrics(
			MetricGroup metricGroup,
			boolean cpuFineGrainedMonitorEnabled,
			int cpuFineGrainedMonitorInterval) {
		MetricGroup jvm = metricGroup.addGroup("JVM");

		instantiateClassLoaderMetrics(jvm.addGroup("ClassLoader"));
		instantiateGarbageCollectorMetrics(jvm.addGroup("GarbageCollector"));
		instantiateMemoryMetrics(jvm.addGroup(METRIC_GROUP_MEMORY));
		instantiateThreadMetrics(jvm.addGroup("Threads"));
		instantiateCPUMetrics(jvm.addGroup("CPU"), cpuFineGrainedMonitorEnabled, cpuFineGrainedMonitorInterval);
	}

	public static void instantiateFlinkMemoryMetricGroup(
			MetricGroup parentMetricGroup,
			TaskSlotTable<?> taskSlotTable,
			Supplier<Long> managedMemoryTotalSupplier) {
		checkNotNull(parentMetricGroup);
		checkNotNull(taskSlotTable);
		checkNotNull(managedMemoryTotalSupplier);

		MetricGroup flinkMemoryMetricGroup = parentMetricGroup.addGroup(METRIC_GROUP_FLINK).addGroup(METRIC_GROUP_MEMORY);

		instantiateManagedMemoryMetrics(flinkMemoryMetricGroup, taskSlotTable, managedMemoryTotalSupplier);
	}

	private static void instantiateManagedMemoryMetrics(
			MetricGroup metricGroup,
			TaskSlotTable<?> taskSlotTable,
			Supplier<Long> managedMemoryTotalSupplier) {
		MetricGroup managedMemoryMetricGroup = metricGroup.addGroup(METRIC_GROUP_MANAGED_MEMORY);

		managedMemoryMetricGroup.gauge("Used", () -> getUsedManagedMemory(taskSlotTable));
		managedMemoryMetricGroup.gauge("Total", managedMemoryTotalSupplier::get);
	}

	private static long getUsedManagedMemory(TaskSlotTable<?> taskSlotTable) {
		long usedMemory = 0L;
		try {
			for (MemoryManager memoryManager : taskSlotTable.getMemoryManagers()) {
				usedMemory += memoryManager.getMemorySize() - memoryManager.availableMemory();
			}
		} catch (SlotNotFoundException e) {
			LOG.debug("The task slot is not present anymore and will be ignored in calculating the amount of used memory.", e);
		}

		return usedMemory;
	}

	public static RpcService startRemoteMetricsRpcService(Configuration configuration, String hostname) throws Exception {
		final String portRange = configuration.getString(MetricOptions.QUERY_SERVICE_PORT);

		return startMetricRpcService(configuration, AkkaRpcServiceUtils.remoteServiceBuilder(configuration, hostname, portRange));
	}

	public static RpcService startLocalMetricsRpcService(Configuration configuration) throws Exception {
		return startMetricRpcService(configuration, AkkaRpcServiceUtils.localServiceBuilder(configuration));
	}

	private static RpcService startMetricRpcService(
			Configuration configuration, AkkaRpcServiceUtils.AkkaRpcServiceBuilder rpcServiceBuilder) throws Exception {
		final int threadPriority = configuration.getInteger(MetricOptions.QUERY_SERVICE_THREAD_PRIORITY);

		return rpcServiceBuilder
			.withActorSystemName(METRICS_ACTOR_SYSTEM_NAME)
			.withActorSystemExecutorConfiguration(new BootstrapTools.FixedThreadPoolExecutorConfiguration(1, 1, threadPriority))
			.createAndStart();
	}

	private static void instantiateClassLoaderMetrics(MetricGroup metrics) {
		final ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();
		metrics.<Long, Gauge<Long>>gauge("ClassesLoaded", mxBean::getTotalLoadedClassCount);
		metrics.<Long, Gauge<Long>>gauge("ClassesUnloaded", mxBean::getUnloadedClassCount);
	}

	private static void instantiateGarbageCollectorMetrics(MetricGroup metrics) {
		List<GarbageCollectorMXBean> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();

		for (final GarbageCollectorMXBean garbageCollector: garbageCollectors) {
			MetricGroup gcGroup = metrics.addGroup(garbageCollector.getName());

			gcGroup.<Long, Gauge<Long>>gauge("Count", garbageCollector::getCollectionCount);
			gcGroup.<Long, Gauge<Long>>gauge("Time", garbageCollector::getCollectionTime);
		}
	}

	private static void instantiateMemoryMetrics(MetricGroup metrics) {
		instantiateHeapMemoryMetrics(metrics.addGroup(METRIC_GROUP_HEAP_NAME));
		instantiateNonHeapMemoryMetrics(metrics.addGroup(METRIC_GROUP_NONHEAP_NAME));
		instantiateMetaspaceMemoryMetrics(metrics);

		final MBeanServer con = ManagementFactory.getPlatformMBeanServer();

		final String directBufferPoolName = "java.nio:type=BufferPool,name=direct";

		try {
			final ObjectName directObjectName = new ObjectName(directBufferPoolName);

			MetricGroup direct = metrics.addGroup("Direct");

			direct.<Long, Gauge<Long>>gauge("Count", new AttributeGauge<>(con, directObjectName, "Count", -1L));
			direct.<Long, Gauge<Long>>gauge("MemoryUsed", new AttributeGauge<>(con, directObjectName, "MemoryUsed", -1L));
			direct.<Long, Gauge<Long>>gauge("TotalCapacity", new AttributeGauge<>(con, directObjectName, "TotalCapacity", -1L));
		} catch (MalformedObjectNameException e) {
			LOG.warn("Could not create object name {}.", directBufferPoolName, e);
		}

		final String mappedBufferPoolName = "java.nio:type=BufferPool,name=mapped";

		try {
			final ObjectName mappedObjectName = new ObjectName(mappedBufferPoolName);

			MetricGroup mapped = metrics.addGroup("Mapped");

			mapped.<Long, Gauge<Long>>gauge("Count", new AttributeGauge<>(con, mappedObjectName, "Count", -1L));
			mapped.<Long, Gauge<Long>>gauge("MemoryUsed", new AttributeGauge<>(con, mappedObjectName, "MemoryUsed", -1L));
			mapped.<Long, Gauge<Long>>gauge("TotalCapacity", new AttributeGauge<>(con, mappedObjectName, "TotalCapacity", -1L));
		} catch (MalformedObjectNameException e) {
			LOG.warn("Could not create object name {}.", mappedBufferPoolName, e);
		}
	}

	@VisibleForTesting
	static void instantiateHeapMemoryMetrics(final MetricGroup metricGroup) {
		instantiateMemoryUsageMetrics(metricGroup, () -> ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
	}

	@VisibleForTesting
	static void instantiateNonHeapMemoryMetrics(final MetricGroup metricGroup) {
		instantiateMemoryUsageMetrics(metricGroup, () -> ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage());
	}

	@VisibleForTesting
	static void instantiateMetaspaceMemoryMetrics(final MetricGroup parentMetricGroup) {
		final List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans()
			.stream()
			.filter(bean -> "Metaspace".equals(bean.getName()))
			.collect(Collectors.toList());

		if (memoryPoolMXBeans.isEmpty()) {
			LOG.info("The '{}' metrics will not be exposed because no pool named 'Metaspace' could be found. This might be caused by the used JVM.", METRIC_GROUP_METASPACE_NAME);
			return;
		}

		final MetricGroup metricGroup = parentMetricGroup.addGroup(METRIC_GROUP_METASPACE_NAME);
		final Iterator<MemoryPoolMXBean> beanIterator = memoryPoolMXBeans.iterator();

		final MemoryPoolMXBean firstPool = beanIterator.next();
		instantiateMemoryUsageMetrics(metricGroup, firstPool::getUsage);

		if (beanIterator.hasNext()) {
			LOG.debug("More than one memory pool named 'Metaspace' is present. Only the first pool was used for instantiating the '{}' metrics.", METRIC_GROUP_METASPACE_NAME);
		}
	}

	private static void instantiateMemoryUsageMetrics(final MetricGroup metricGroup, final Supplier<MemoryUsage> memoryUsageSupplier) {
		metricGroup.<Long, Gauge<Long>>gauge(MetricNames.MEMORY_USED, () -> memoryUsageSupplier.get().getUsed());
		metricGroup.<Long, Gauge<Long>>gauge(MetricNames.MEMORY_COMMITTED, () -> memoryUsageSupplier.get().getCommitted());
		metricGroup.<Long, Gauge<Long>>gauge(MetricNames.MEMORY_MAX, () -> memoryUsageSupplier.get().getMax());
	}

	private static void instantiateThreadMetrics(MetricGroup metrics) {
		final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

		metrics.<Integer, Gauge<Integer>>gauge("Count", mxBean::getThreadCount);
	}

	private static void instantiateCPUMetrics(MetricGroup metrics) {
		instantiateCPUMetrics(metrics, false, DEFAULT_MONITOR_INTERVAL);
	}

	private static void instantiateCPUMetrics(MetricGroup metrics, boolean cpuFineGrainedMonitorEnabled, int cpuFineGrainedMonitorInterval) {
		try {
			final com.sun.management.OperatingSystemMXBean mxBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
			final int cpuCores = Runtime.getRuntime().availableProcessors();
			//getProcessCpuLoad returns the "recent cpu usage" for the JVM process. This value is a double in the [0.0,1.0] interval
			//getProcessCpuLoad * cpuCores means nums of Cpus JVM used.
			LOG.info("cpuFineGrainedMonitorEnabled: {}, cpuFineGrainedMonitorInterval: {}", cpuFineGrainedMonitorEnabled, cpuFineGrainedMonitorInterval);
			if (cpuFineGrainedMonitorEnabled) {
				monitorProcessCpuLoad(mxBean, cpuFineGrainedMonitorInterval);
				metrics.<Double, Gauge<Double>>gauge("Cores", () -> getProcessCpuLoad());
			} else {
				metrics.<Double, Gauge<Double>>gauge("Cores", () -> mxBean.getProcessCpuLoad() * cpuCores);
			}
			metrics.<Double, Gauge<Double>>gauge("Load", () -> mxBean.getProcessCpuLoad());
			metrics.<Long, Gauge<Long>>gauge("Time", mxBean::getProcessCpuTime);
		} catch (Exception e) {
			LOG.warn("Cannot access com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()" +
				" - CPU load metrics will not be available.", e);
		}
	}

	private static double getProcessCpuLoad() {
		double maxProcessCpuLoad;
		try {
			PROCESS_CPU_LOAD_LOCK.lock();
			maxProcessCpuLoad = processCpuLoad;
			processCpuLoad = 0.0;
		} finally {
			PROCESS_CPU_LOAD_LOCK.unlock();
		}
		return maxProcessCpuLoad;
	}

	private static void monitorProcessCpuLoad(com.sun.management.OperatingSystemMXBean mxBean, int cpuFineGrainedMonitorInterval) {
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		prevTimestamp = System.currentTimeMillis();
		prevCpuTime = mxBean.getProcessCpuTime();

		Runnable runnable = () -> {
			currentCpuTime = mxBean.getProcessCpuTime();
			currentTimestamp = System.currentTimeMillis();
			long interval = currentTimestamp - prevTimestamp;
			if (interval == 0) {
				return;
			}
			double cpuLoad = (currentCpuTime - prevCpuTime) / 1E9 / interval * 1000.0;
			try {
				PROCESS_CPU_LOAD_LOCK.lock();
				processCpuLoad = Math.max(cpuLoad, processCpuLoad);
			} finally {
				PROCESS_CPU_LOAD_LOCK.unlock();
			}
			prevCpuTime = currentCpuTime;
			prevTimestamp = currentTimestamp;
		};
		executor.scheduleWithFixedDelay(
			runnable,
			0,
			cpuFineGrainedMonitorInterval,
			TimeUnit.MILLISECONDS);
	}

	private static final class AttributeGauge<T> implements Gauge<T> {
		private final MBeanServer server;
		private final ObjectName objectName;
		private final String attributeName;
		private final T errorValue;

		private AttributeGauge(MBeanServer server, ObjectName objectName, String attributeName, T errorValue) {
			this.server = Preconditions.checkNotNull(server);
			this.objectName = Preconditions.checkNotNull(objectName);
			this.attributeName = Preconditions.checkNotNull(attributeName);
			this.errorValue = errorValue;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T getValue() {
			try {
				return (T) server.getAttribute(objectName, attributeName);
			} catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException e) {
				LOG.warn("Could not read attribute {}.", attributeName, e);
				return errorValue;
			}
		}
	}
}
