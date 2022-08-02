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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * The set of configuration options relating to TaskManager and Task settings.
 */
@PublicEvolving
@ConfigGroups(groups = @ConfigGroup(name = "TaskManagerMemory", keyPrefix = "taskmanager.memory"))
public class TaskManagerOptions {

	// ------------------------------------------------------------------------
	//  General TaskManager Options
	// ------------------------------------------------------------------------

	/**
	 * JVM heap size for the TaskManagers with memory size.
	 *
	 * @deprecated use {@link #TOTAL_FLINK_MEMORY} for standalone setups and {@link #TOTAL_PROCESS_MEMORY} for containerized setups.
	 */
	@Deprecated
	public static final ConfigOption<MemorySize> TASK_MANAGER_HEAP_MEMORY =
		key("taskmanager.heap.size")
			.memoryType()
			.noDefaultValue()
			.withDescription("JVM heap size for the TaskManagers, which are the parallel workers of" +
				" the system. On YARN setups, this value is automatically configured to the size of the TaskManager's" +
				" YARN container, minus a certain tolerance value.");

	/**
	 * JVM heap size (in megabytes) for the TaskManagers.
	 *
	 * @deprecated use {@link #TOTAL_FLINK_MEMORY} for standalone setups and {@link #TOTAL_PROCESS_MEMORY} for containerized setups.
	 */
	@Deprecated
	public static final ConfigOption<Integer> TASK_MANAGER_HEAP_MEMORY_MB =
			key("taskmanager.heap.mb")
			.intType()
			.noDefaultValue()
			.withDescription("JVM heap size (in megabytes) for the TaskManagers, which are the parallel workers of" +
				" the system. On YARN setups, this value is automatically configured to the size of the TaskManager's" +
				" YARN container, minus a certain tolerance value.");

	/**
	 * Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Boolean> KILL_ON_OUT_OF_MEMORY =
			key("taskmanager.jvm-exit-on-oom")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.");

	/**
	 * Whether the quarantine monitor for task managers shall be started. The quarantine monitor
	 * shuts down the actor system if it detects that it has quarantined another actor system
	 * or if it has been quarantined by another actor system.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> EXIT_ON_FATAL_AKKA_ERROR =
			key("taskmanager.exit-on-fatal-akka-error")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether the quarantine monitor for task managers shall be started. The quarantine monitor" +
				" shuts down the actor system if it detects that it has quarantined another actor system" +
				" or if it has been quarantined by another actor system.");

	/**
	 * The external address of the network interface where the TaskManager is exposed.
	 * Overrides {@link #HOST_BIND_POLICY} automatic address binding.
	 */
	@Documentation.Section({Documentation.Sections.COMMON_HOST_PORT, Documentation.Sections.ALL_TASK_MANAGER})
	public static final ConfigOption<String> HOST =
		key("taskmanager.host")
			.stringType()
			.noDefaultValue()
			.withDescription("The external address of the network interface where the TaskManager is exposed." +
				" Because different TaskManagers need different values for this option, usually it is specified in an" +
				" additional non-shared TaskManager-specific config file.");

	/**
	 * The local address of the network interface that the task manager binds to.
	 */
	public static final ConfigOption<String> BIND_HOST =
		key("taskmanager.bind-host")
			.stringType()
			.noDefaultValue()
			.withDescription("The local address of the network interface that the task manager binds to. If not" +
				" configured, '0.0.0.0' will be used.");

	/**
	 * The default network port range the task manager expects incoming IPC connections. The {@code "0"} means that
	 * the TaskManager searches for a free port.
	 */
	@Documentation.Section({Documentation.Sections.COMMON_HOST_PORT, Documentation.Sections.ALL_TASK_MANAGER})
	public static final ConfigOption<String> RPC_PORT =
		key("taskmanager.rpc.port")
			.stringType()
			.defaultValue("0")
			.withDescription("The external RPC port where the TaskManager is exposed. Accepts a list of ports" +
				" (“50100,50101”), ranges (“50100-50200”) or a combination of both. It is recommended to set a" +
				" range of ports to avoid collisions when multiple TaskManagers are running on the same machine.");


	/**
	 * The local port that the task manager binds to.
	 */
	public static final ConfigOption<Integer> RPC_BIND_PORT =
		key("taskmanager.rpc.bind-port")
			.intType()
			.noDefaultValue()
			.withDescription("The local RPC port that the TaskManager binds to. If not configured, the external port" +
				" (configured by '" + RPC_PORT.key() + "') will be used.");

	/**
	 * The initial registration backoff between two consecutive registration attempts. The backoff
	 * is doubled for each new registration attempt until it reaches the maximum registration backoff.
	 *
	 * @deprecated use {@link ClusterOptions#INITIAL_REGISTRATION_TIMEOUT} instead
	 */
	@Deprecated
	public static final ConfigOption<Duration> INITIAL_REGISTRATION_BACKOFF =
		key("taskmanager.registration.initial-backoff")
			.durationType()
			.defaultValue(TimeUtils.parseDuration("500 ms"))
			.withDeprecatedKeys("taskmanager.initial-registration-pause")
			.withDescription("The initial registration backoff between two consecutive registration attempts. The backoff" +
				" is doubled for each new registration attempt until it reaches the maximum registration backoff.");

	/**
	 * The maximum registration backoff between two consecutive registration attempts.
	 *
	 * @deprecated use {@link ClusterOptions#MAX_REGISTRATION_TIMEOUT} instead
	 */
	@Deprecated
	public static final ConfigOption<Duration> REGISTRATION_MAX_BACKOFF =
		key("taskmanager.registration.max-backoff")
			.durationType()
			.defaultValue(TimeUtils.parseDuration("30 s"))
			.withDeprecatedKeys("taskmanager.max-registration-pause")
			.withDescription("The maximum registration backoff between two consecutive registration attempts. The max" +
				" registration backoff requires a time unit specifier (ms/s/min/h/d).");

	/**
	 * The backoff after a registration has been refused by the job manager before retrying to connect.
	 *
	 * @deprecated use {@link ClusterOptions#REFUSED_REGISTRATION_DELAY} instead
	 */
	@Deprecated
	public static final ConfigOption<Duration> REFUSED_REGISTRATION_BACKOFF =
		key("taskmanager.registration.refused-backoff")
			.durationType()
			.defaultValue(TimeUtils.parseDuration("10 s"))
			.withDeprecatedKeys("taskmanager.refused-registration-pause")
			.withDescription("The backoff after a registration has been refused by the job manager before retrying to connect.");

	/**
	 * Defines the timeout it can take for the TaskManager registration. If the duration is
	 * exceeded without a successful registration, then the TaskManager terminates.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Duration> REGISTRATION_TIMEOUT =
		key("taskmanager.registration.timeout")
			.durationType()
			.defaultValue(TimeUtils.parseDuration("5 min"))
			.withDeprecatedKeys("taskmanager.maxRegistrationDuration")
			.withDescription("Defines the timeout for the TaskManager registration. If the duration is" +
				" exceeded without a successful registration, then the TaskManager terminates.");

	/**
	 * The config parameter defining the number of task managers.
	 */
	public static final ConfigOption<Integer> NUM_INITIAL_TASK_MANAGERS =
		key("taskmanager.number-initial")
			.intType()
			.defaultValue(0)
			.withDescription("The number of task managers. 0 for not initial task managers on RM start.");

	public static final ConfigOption<Double> NUM_INITIAL_TASK_MANAGERS_PERCENTAGE =
		key("taskmanager.number-initial-percentage")
			.defaultValue(0.0)
			.withDescription("The percentage of number of task managers for initializing on RM start.");

	/**
	 * The config parameter defining the number of extra task managers, avoid yarn slow nodes.
	 */
	public static final ConfigOption<Integer> NUM_EXTRA_INITIAL_TASK_MANAGERS =
		key("taskmanager.number-extra-initial")
			.defaultValue(0)
			.withDescription("The number of extra task managers, avoid yarn slow nodes.");

	/**
	 * Initialize task managers when slotManager start.
	 */
	public static final ConfigOption<Boolean> INITIAL_TASK_MANAGER_ON_START =
		key("taskmanager.initial-on-start")
			.booleanType()
			.defaultValue(false)
			.withDescription("Initialize task managers when slotManager start.");

	/**
	 * The config parameter defining the number of task slots of a task manager.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Integer> NUM_TASK_SLOTS =
		key("taskmanager.numberOfTaskSlots")
			.intType()
			.defaultValue(1)
			.withDescription("The number of parallel operator or user function instances that a single TaskManager can" +
				" run. If this value is larger than 1, a single TaskManager takes multiple instances of a function or" +
				" operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the" +
				" available memory is divided between the different operator or function instances. This value" +
				" is typically proportional to the number of physical CPU cores that the TaskManager's machine has" +
				" (e.g., equal to the number of cores, or half the number of cores).");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Boolean> DEBUG_MEMORY_LOG =
		key("taskmanager.debug.memory.log")
			.booleanType()
			.defaultValue(false)
			.withDeprecatedKeys("taskmanager.debug.memory.startLogThread")
			.withDescription("Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Long> DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS =
		key("taskmanager.debug.memory.log-interval")
			.longType()
			.defaultValue(5000L)
			.withDeprecatedKeys("taskmanager.debug.memory.logIntervalMs")
			.withDescription("The interval (in ms) for the log thread to log the current memory usage.");

	// ------------------------------------------------------------------------
	//  Managed Memory Options
	// ------------------------------------------------------------------------

	/**
	 * Size of memory buffers used by the network stack and the memory manager.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<MemorySize> MEMORY_SEGMENT_SIZE =
			key("taskmanager.memory.segment-size")
			.memoryType()
			.defaultValue(MemorySize.parse("32kb"))
			.withDescription("Size of memory buffers used by the network stack and the memory manager.");

	/**
	 * The config parameter for automatically defining the TaskManager's binding address,
	 * if {@link #HOST} configuration option is not set.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<String> HOST_BIND_POLICY =
		key("taskmanager.network.bind-policy")
			.stringType()
			.defaultValue("ip")
			.withDescription(Description.builder()
				.text("The automatic address binding policy used by the TaskManager if \"" + HOST.key() + "\" is not set." +
					" The value should be one of the following:\n")
				.list(
					text("\"name\" - uses hostname as binding address"),
					text("\"ip\" - uses host's ip address as binding address"))
				.build());

	// ------------------------------------------------------------------------
	//  Resource Options
	// ------------------------------------------------------------------------

	/**
	 * This config option describes number of cpu cores of task executors.
	 * In case of Yarn / Mesos / Kubernetes, it is used to launch a container for the task executor.
	 *
	 * <p>DO NOT USE THIS CONFIG OPTION.
	 * This config option is currently only used internally, for passing cpu cores into task executors
	 * for dynamic fine grained slot resource management. The feature is not completed at the moment,
	 * and the config option is experimental and might be changed / removed in the future. Thus, we do
	 * not expose this config option to users.
	 *
	 * <p>For configuring the cpu cores of container on Yarn / Mesos / Kubernetes, please use
	 * {@link YarnConfigOptions#VCORES}, {@link MesosTaskManagerParameters#MESOS_RM_TASKS_CPUS} and
	 * {@link KubernetesConfigOptions#TASK_MANAGER_CPU}.
	 */
	@Documentation.ExcludeFromDocumentation
	public static final ConfigOption<Double> CPU_CORES =
		key("taskmanager.cpu.cores")
			.doubleType()
			.noDefaultValue()
			.withDescription("CPU cores for the TaskExecutors. In case of Yarn setups, this value will be rounded to "
				+ "the closest positive integer. If not explicitly configured, legacy config options "
				+ "'yarn.containers.vcores', 'mesos.resourcemanager.tasks.cpus' and 'kubernetes.taskmanager.cpu' will be "
				+ "used for Yarn / Mesos / Kubernetes setups, and '" + NUM_TASK_SLOTS.key() + "' will be used for "
				+ "standalone setups (approximate number of slots).");

	/**
	 * Total Process Memory size for the TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> TOTAL_PROCESS_MEMORY =
		key("taskmanager.memory.process.size")
			.memoryType()
			.noDefaultValue()
			.withDescription("Total Process Memory size for the TaskExecutors. This includes all the memory that a "
				+ "TaskExecutor consumes, consisting of Total Flink Memory, JVM Metaspace, and JVM Overhead. On "
				+ "containerized setups, this should be set to the container memory. See also "
				+ "'taskmanager.memory.flink.size' for total Flink memory size configuration."
			);

	/**
	 * Total Flink Memory size for the TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> TOTAL_FLINK_MEMORY =
		key("taskmanager.memory.flink.size")
			.memoryType()
			.noDefaultValue()
			.withDescription(String.format("Total Flink Memory size for the TaskExecutors. This includes all the "
					+ "memory that a TaskExecutor consumes, except for JVM Metaspace and JVM Overhead. It consists of "
					+ "Framework Heap Memory, Task Heap Memory, Task Off-Heap Memory, Managed Memory, and Network "
					+ "Memory. See also '%s' for total process memory size configuration.",
				TOTAL_PROCESS_MEMORY.key()));

	/**
	 * Framework Heap Memory size for TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> FRAMEWORK_HEAP_MEMORY =
		key("taskmanager.memory.framework.heap.size")
			.memoryType()
			.defaultValue(MemorySize.parse("128m"))
			.withDescription("Framework Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved"
				+ " for TaskExecutor framework, which will not be allocated to task slots.");

	/**
	 * Framework Off-Heap Memory size for TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> FRAMEWORK_OFF_HEAP_MEMORY =
		key("taskmanager.memory.framework.off-heap.size")
			.memoryType()
			.defaultValue(MemorySize.parse("128m"))
			.withDescription("Framework Off-Heap Memory size for TaskExecutors. This is the size of off-heap memory"
				+ " (JVM direct memory and native memory) reserved for TaskExecutor framework, which will not be"
				+ " allocated to task slots. The configured value will be fully counted when Flink calculates the JVM"
				+ " max direct memory size parameter.");

	/**
	 * Task Heap Memory size for TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> TASK_HEAP_MEMORY =
		key("taskmanager.memory.task.heap.size")
			.memoryType()
			.noDefaultValue()
			.withDescription("Task Heap Memory size for TaskExecutors. This is the size of JVM heap memory reserved for"
				+ " tasks. If not specified, it will be derived as Total Flink Memory minus Framework Heap Memory,"
				+ " Task Off-Heap Memory, Managed Memory and Network Memory.");

	/**
	 * Task Off-Heap Memory size for TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> TASK_OFF_HEAP_MEMORY =
		key("taskmanager.memory.task.off-heap.size")
			.memoryType()
			.defaultValue(MemorySize.ZERO)
			.withDescription("Task Off-Heap Memory size for TaskExecutors. This is the size of off heap memory (JVM"
				+ " direct memory and native memory) reserved for tasks. The configured value will be fully counted"
				+ " when Flink calculates the JVM max direct memory size parameter.");

	/**
	 * Managed Memory size for TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> MANAGED_MEMORY_SIZE =
		key("taskmanager.memory.managed.size")
			.memoryType()
			.noDefaultValue()
			.withDeprecatedKeys("taskmanager.memory.size")
			.withDescription("Managed Memory size for TaskExecutors. This is the size of off-heap memory managed by the"
				+ " memory manager, reserved for sorting, hash tables, caching of intermediate results and RocksDB state"
				+ " backend. Memory consumers can either allocate memory from the memory manager in the form of"
				+ " MemorySegments, or reserve bytes from the memory manager and keep their memory usage within that"
				+ " boundary. If unspecified, it will be derived to make up the configured fraction of the Total Flink"
				+ " Memory.");

	/**
	 * Fraction of Total Flink Memory to be used as Managed Memory, if {@link #MANAGED_MEMORY_SIZE} is not specified.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<Float> MANAGED_MEMORY_FRACTION =
		key("taskmanager.memory.managed.fraction")
			.floatType()
			.defaultValue(0.4f)
			.withDescription("Fraction of Total Flink Memory to be used as Managed Memory, if Managed Memory size is not"
				+ " explicitly specified.");

	/**
	 * Min Network Memory size for TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> NETWORK_MEMORY_MIN =
		key("taskmanager.memory.network.min")
			.memoryType()
			.defaultValue(MemorySize.parse("64m"))
			.withDeprecatedKeys(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.key())
			.withDescription("Min Network Memory size for TaskExecutors. Network Memory is off-heap memory reserved for"
				+ " ShuffleEnvironment (e.g., network buffers). Network Memory size is derived to make up the configured"
				+ " fraction of the Total Flink Memory. If the derived size is less/greater than the configured min/max"
				+ " size, the min/max size will be used. The exact size of Network Memory can be explicitly specified by"
				+ " setting the min/max to the same value.");

	/**
	 * Max Network Memory size for TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> NETWORK_MEMORY_MAX =
		key("taskmanager.memory.network.max")
			.memoryType()
			.defaultValue(MemorySize.parse("1g"))
			.withDeprecatedKeys(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key())
			.withDescription("Max Network Memory size for TaskExecutors. Network Memory is off-heap memory reserved for"
				+ " ShuffleEnvironment (e.g., network buffers). Network Memory size is derived to make up the configured"
				+ " fraction of the Total Flink Memory. If the derived size is less/greater than the configured min/max"
				+ " size, the min/max size will be used. The exact size of Network Memory can be explicitly specified by"
				+ " setting the min/max to the same value.");

	/**
	 * Fraction of Total Flink Memory to be used as Network Memory.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<Float> NETWORK_MEMORY_FRACTION =
		key("taskmanager.memory.network.fraction")
			.floatType()
			.defaultValue(0.1f)
			.withDeprecatedKeys(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key())
			.withDescription("Fraction of Total Flink Memory to be used as Network Memory. Network Memory is off-heap"
				+ " memory reserved for ShuffleEnvironment (e.g., network buffers). Network Memory size is derived to"
				+ " make up the configured fraction of the Total Flink Memory. If the derived size is less/greater than"
				+ " the configured min/max size, the min/max size will be used. The exact size of Network Memory can be"
				+ " explicitly specified by setting the min/max size to the same value.");

	/**
	 * JVM Metaspace Size for the TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> JVM_METASPACE =
		key("taskmanager.memory.jvm-metaspace.size")
			.memoryType()
			.defaultValue(MemorySize.parse("256m"))
			.withDescription("JVM Metaspace Size for the TaskExecutors.");

	/**
	 * Min JVM Overhead size for the TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> JVM_OVERHEAD_MIN =
		key("taskmanager.memory.jvm-overhead.min")
			.memoryType()
			.defaultValue(MemorySize.parse("192m"))
			.withDescription("Min JVM Overhead size for the TaskExecutors. This is off-heap memory reserved for JVM"
				+ " overhead, such as thread stack space, compile cache, etc. This includes native memory but not direct"
				+ " memory, and will not be counted when Flink calculates JVM max direct memory size parameter. The size"
				+ " of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If the"
				+ " derived size is less/greater than the configured min/max size, the min/max size will be used. The"
				+ " exact size of JVM Overhead can be explicitly specified by setting the min/max size to the same value.");

	/**
	 * Max JVM Overhead size for the TaskExecutors.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<MemorySize> JVM_OVERHEAD_MAX =
		key("taskmanager.memory.jvm-overhead.max")
			.memoryType()
			.defaultValue(MemorySize.parse("1g"))
			.withDescription("Max JVM Overhead size for the TaskExecutors. This is off-heap memory reserved for JVM"
				+ " overhead, such as thread stack space, compile cache, etc. This includes native memory but not direct"
				+ " memory, and will not be counted when Flink calculates JVM max direct memory size parameter. The size"
				+ " of JVM Overhead is derived to make up the configured fraction of the Total Process Memory. If the"
				+ " derived size is less/greater than the configured min/max size, the min/max size will be used. The"
				+ " exact size of JVM Overhead can be explicitly specified by setting the min/max size to the same value.");

	/**
	 * Fraction of Total Process Memory to be reserved for JVM Overhead.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MEMORY)
	public static final ConfigOption<Float> JVM_OVERHEAD_FRACTION =
		key("taskmanager.memory.jvm-overhead.fraction")
			.floatType()
			.defaultValue(0.1f)
			.withDescription("Fraction of Total Process Memory to be reserved for JVM Overhead. This is off-heap memory"
				+ " reserved for JVM overhead, such as thread stack space, compile cache, etc. This includes native"
				+ " memory but not direct memory, and will not be counted when Flink calculates JVM max direct memory"
				+ " size parameter. The size of JVM Overhead is derived to make up the configured fraction of the Total"
				+ " Process Memory. If the derived size is less/greater than the configured min/max size, the min/max"
				+ " size will be used. The exact size of JVM Overhead can be explicitly specified by setting the min/max"
				+ " size to the same value.");

	// ------------------------------------------------------------------------
	//  Task Options
	// ------------------------------------------------------------------------

	/**
	 * Enable state related initialization or not.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Boolean> TASK_STATE_INIT_ENABLED =
			key("task.state-init.enabled")
			.booleanType()
			.defaultValue(true)
			.withDescription("Enable state related initialization or not. In batch/olap mode, we suggest to set " +
				"false to disable these useless state initialization in stream task.");

	/**
	 * Time interval in milliseconds between two successive task cancellation
	 * attempts.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Long> TASK_CANCELLATION_INTERVAL =
			key("task.cancellation.interval")
			.longType()
			.defaultValue(30000L)
			.withDeprecatedKeys("task.cancellation-interval")
			.withDescription("Time interval between two successive task cancellation attempts in milliseconds.");

	/**
	 * Timeout in milliseconds after which a task cancellation times out and
	 * leads to a fatal TaskManager error. A value of <code>0</code> deactivates
	 * the watch dog.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT =
			key("task.cancellation.timeout")
			.longType()
			.defaultValue(180000L)
			.withDescription("Timeout in milliseconds after which a task cancellation times out and" +
				" leads to a fatal TaskManager error. A value of 0 deactivates" +
				" the watch dog.");
	/**
	 * This configures how long we wait for the timers in milliseconds to finish all pending timer threads
	 * when the stream task is cancelled.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT_TIMERS = ConfigOptions
			.key("task.cancellation.timers.timeout")
			.longType()
			.defaultValue(7500L)
			.withDeprecatedKeys("timerservice.exceptional.shutdown.timeout")
			.withDescription("Time we wait for the timers in milliseconds to finish all pending timer threads" +
				" when the stream task is cancelled.");

	/**
	 * The threshold of DataOutputSerializer buffer to prune.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Integer> SERIALIZER_PRUNE_BUFFER_THRESHOLD = ConfigOptions
			.key("taskmanager.serializer.prune.buffer.threshold")
			.intType()
			.defaultValue(1024 * 1024)
			.withDeprecatedKeys("prune.buffer.threshold")
			.withDescription("The threshold of DataOutputSerializer buffer to prune.");

	public static final ConfigOption<Boolean> MEMORY_MANAGER_SLOT_BASED_ENABLE = ConfigOptions
			.key("memory.manager.slot-based-enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("The tasks in different jobs share the memory manager base slots.");

	public static final ConfigOption<Boolean> RELEASE_SLOT_WHEN_JOB_MASTER_DISCONNECTED = ConfigOptions
			.key("taskmanager.release-slot-when-job-master-disconnected.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether release all slots when job master is disconnected.");

	public static final ConfigOption<Long> WAIT_SLOT_RELEASE_BEFORE_SEND_SLOT_REPORTER_TIMEOUT = ConfigOptions
			.key("taskmanager.wait-slot-release-before-send-slot-reporter.timeout-ms")
			.longType()
			.defaultValue(180000L)
			.withDescription("Timeout to wait all slot finish releasing, " +
					"only works when RELEASE_SLOT_WHEN_JOB_MASTER_DISCONNECTED is enabled.");

	public static final ConfigOption<Duration> ALLOCATE_MEMORY_SEGMENTS_TIMEOUT = ConfigOptions
			.key("taskmanager.allocate-memory-segments.timeout")
				.durationType()
				.defaultValue(TimeUtils.parseDuration("10 s"))
				.withDescription("Timeout for allocate memory segments from MemoryPoolManager.");

	public static final ConfigOption<Boolean> MEMORY_POOL_MANAGER_SEGMENT_ALLOCATE_LAZY_ENABLE = ConfigOptions
			.key("taskmanager.memory-pool-mananager.allocate-lazy.enable")
			.booleanType()
			.defaultValue(true)
			.withDescription("Lazy allocate segments in memory pool manager.");

	public static final ConfigOption<Boolean> MEMORY_POOL_MANAGER_ENABLE = ConfigOptions
			.key("taskmanager.memory-pool-manager.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("If true, use the memory pool manager.");

	public static final ConfigOption<Integer> MEMORY_POOL_MANAGER_BATCH_SIZE = ConfigOptions
		.key("taskmanager.memory-pool-manager-batch-size")
		.intType()
		.defaultValue(-1)
		.withDescription("If is greater than 1, the memory pool manager will pack several number of segments into a batch.");

	public static final ConfigOption<Boolean> MEMORY_BATCH_POOL_MANAGER_RELEASE_SEGMENTS_FINALLY_ENABLE = ConfigOptions
		.key("taskmanager.memory-batch-pool-manager-release-segments-finally-enable")
		.booleanType()
		.defaultValue(true)
		.withDescription("If this is set, the memory batch pool will only actually release segments after the task invoke is finished.");

	public static final ConfigOption<Boolean> MEMORY_POOL_SEGMENT_OWNER_CHECKER_ENABLE = ConfigOptions
			.key("taskmanager.memory-pool-segment-owner-checker.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("If true, it will check the owner of segment in any get/put operation" +
				" which is allocated from pool memory manager.");

	public static final ConfigOption<Long> LAZY_MEMORY_SEGMENT_POOL_PER_REQUEST_MEMORY_SIZE = ConfigOptions
		.key("taskmanager.lazy-memory-segment-pool.per-request-memory-bytes")
		.longType()
		.defaultValue(16 * 1024 * 1024L)
		.withDescription("Per request memory size in LazyMemorySegmentPool");

	public static final ConfigOption<Boolean> NOTIFY_FINAL_STATE_IN_TASK_THREAD_ENABLE = ConfigOptions
		.key("taskmanager.notify-final-state-in-task-thread.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("If true, the task will notify the final state in the task thread, " +
			"otherwise it will be done in task executor akka thread. If the task pushes results to " +
			"job manager, this flag must be true to make sure the task sends final status after its results.");

	public static final ConfigOption<Integer> RESULT_PUSH_CLIENT_COUNT = ConfigOptions
		.key("taskmanager.result-push-client.count")
		.intType()
		.defaultValue(100)
		.withDescription("Result push client count in task manager for jobs.");

	public static final ConfigOption<Integer> RESULT_PUSH_CLIENT_CONNECT_TIMEOUT_MILLS = ConfigOptions
		.key("taskmanager.result-push-client-connect-timeout.mills")
		.intType()
		.defaultValue(10000)
		.withDescription("Result push client connect timeout mills");

	public static final ConfigOption<Integer> RESULT_PUSH_NETTY_LOW_WRITER_BUFFER_MARK = ConfigOptions
		.key("taskmanager.result-push-netty-low-writer-buffer.mark")
		.intType()
		.defaultValue(0)
		.withDescription("Low writer buffer mark for task push results with netty client, 0 means no mark");

	public static final ConfigOption<Integer> RESULT_PUSH_NETTY_HIGH_WRITER_BUFFER_MARK = ConfigOptions
		.key("taskmanager.result-push-netty-high-writer-buffer.mark")
		.intType()
		.defaultValue(0)
		.withDescription("High writer buffer mark for task push results with netty client, 0 means no mark");

	public static final ConfigOption<Integer> NETTY_SERVER_CONNECT_BACKLOG = ConfigOptions
		.key("taskmanager.netty-server-connect.backlog")
		.intType()
		.defaultValue(0) // default: 0 => Netty's default
		.withDescription("The task executor netty server connection backlog.");

	public static final ConfigOption<Integer> NETTY_SERVER_WORKER_THREAD_COUNT = ConfigOptions
		.key("taskmanager.netty-server-worker-thread.count")
		.intType()
		.defaultValue(0)
		.withDescription("Worker thread count of netty server in task executor.");

	public static final ConfigOption<Boolean> TASK_THREAD_POOL_ENABLE = ConfigOptions
		.key("taskmanager.task-thread-pool.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("Use thread pool to manager task execute thread and related cancel thread.");

	public static final ConfigOption<Boolean> TASKMANAGER_BATCH_UPDATE_JOB_TASK_STATE_ENABLE = ConfigOptions
			.key("taskmanager.batch-update-job-task-state.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("Job tasks in one task manager will batch update task states");

	/**
	 * Option for enabling invokable constructor cache in Task.
	 */
	public static final ConfigOption<Boolean> TASK_INVOKABLE_CONSTRUCTOR_CACHE_ENABLED = ConfigOptions
		.key("taskmanager.task-invokable-constructor-cache.enabled")
		.booleanType()
		.defaultValue(false)
		.withDescription("If true, Task will cache constructor of invokable class instead of creating each time.");

	/**
	 * Max size of invokable constructor cache in Task.
	 */
	public static final ConfigOption<Long> TASK_INVOKABLE_CONSTRUCTOR_CACHE_MAX_SIZE = ConfigOptions
		.key("taskmanager.task-invokable-constructor-cache.max-size")
		.longType()
		.defaultValue(100L)
		.withDescription("Optional. Max size of constructor of invokable cache.");

	/**
	 * Time to live of invokable constructor cache in Task.
	 */
	public static final ConfigOption<Duration> TASK_INVOKABLE_CONSTRUCTOR_CACHE_TTL = ConfigOptions
		.key("taskmanager.task-invokable-constructor-cache.ttl")
		.durationType()
		.defaultValue(Duration.ofMillis(300_000))
		.withDescription("Optional. Time to live for constructor of invokable class cache.");

	public static final ConfigOption<Long> TASKMANAGER_TASK_STUCK_CHECK_THREAD_INTERVAL = ConfigOptions
		.key("taskmanager.task-stuck-check.interval")
		.longType()
		.defaultValue(-1L)
		.withDescription("Time between two task stuck check, -1 means check not enable.");

	public static final ConfigOption<Long> TASKMANAGER_TASK_STUCK_CHECK_DURATION = ConfigOptions
		.key("taskmanager.task-stuck-check.duration")
		.longType()
		.defaultValue(600000L)
		.withDescription("How long the task does not send data downstream is considered stuck.");

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private TaskManagerOptions() {}
}
