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
import org.apache.flink.annotation.docs.Documentation;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to network stack.
 */
@PublicEvolving
public class NettyShuffleEnvironmentOptions {

	// ------------------------------------------------------------------------
	//  Network General Options
	// ------------------------------------------------------------------------

	/**
	 * The default network port the task manager expects to receive transfer envelopes on. The {@code 0} means that
	 * the TaskManager searches for a free port.
	 */
	@Documentation.Section({Documentation.Sections.COMMON_HOST_PORT, Documentation.Sections.ALL_TASK_MANAGER})
	public static final ConfigOption<Integer> DATA_PORT =
		key("taskmanager.data.port")
			.defaultValue(0)
			.withDescription("The task manager’s external port used for data exchange operations.");

	/**
	 * The local network port that the task manager listen at for data exchange.
	 */
	public static final ConfigOption<Integer> DATA_BIND_PORT =
		key("taskmanager.data.bind-port")
			.intType()
			.noDefaultValue()
			.withDescription("The task manager's bind port used for data exchange operations. If not configured, '" +
				DATA_PORT.key() + "' will be used.");

	/**
	 * Config parameter to override SSL support for taskmanager's data transport.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER)
	public static final ConfigOption<Boolean> DATA_SSL_ENABLED =
		key("taskmanager.data.ssl.enabled")
			.defaultValue(true)
			.withDescription("Enable SSL support for the taskmanager data transport. This is applicable only when the" +
				" global flag for internal SSL (" + SecurityOptions.SSL_INTERNAL_ENABLED.key() + ") is set to true");

	/**
	 * Boolean flag indicating whether the shuffle data will be compressed for blocking shuffle mode.
	 *
	 * <p>Note: Data is compressed per buffer and compression can incur extra CPU overhead so it is more effective for
	 * IO bounded scenario when data compression ratio is high. Currently, shuffle data compression is an experimental
	 * feature and the config option can be changed in the future.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Boolean> BLOCKING_SHUFFLE_COMPRESSION_ENABLED =
		key("taskmanager.network.blocking-shuffle.compression.enabled")
			.defaultValue(true)
			.withDescription("Boolean flag indicating whether the shuffle data will be compressed for blocking shuffle" +
				" mode. Note that data is compressed per buffer and compression can incur extra CPU overhead, so it is" +
				" more effective for IO bounded scenario when data compression ratio is high. Currently, shuffle data " +
				"compression is an experimental feature and the config option can be changed in the future.");

	/**
	 * The codec to be used when compressing shuffle data.
	 */
	@Documentation.ExcludeFromDocumentation("Currently, LZ4 is the only legal option.")
	public static final ConfigOption<String> SHUFFLE_COMPRESSION_CODEC =
		key("taskmanager.network.compression.codec")
			.defaultValue("LZ4")
			.withDescription("The codec to be used when compressing shuffle data.");

	/**
	 * Boolean flag to enable/disable more detailed metrics about inbound/outbound network queue
	 * lengths.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Boolean> NETWORK_DETAILED_METRICS =
		key("taskmanager.network.detailed-metrics")
			.defaultValue(false)
			.withDescription("Boolean flag to enable/disable more detailed metrics about inbound/outbound network queue lengths.");

	/**
	 * Number of buffers used in the network stack. This defines the number of possible tasks and
	 * shuffles.
	 *
	 * @deprecated use {@link TaskManagerOptions#NETWORK_MEMORY_FRACTION}, {@link TaskManagerOptions#NETWORK_MEMORY_MIN},
	 * and {@link TaskManagerOptions#NETWORK_MEMORY_MAX} instead
	 */
	@Deprecated
	public static final ConfigOption<Integer> NETWORK_NUM_BUFFERS =
		key("taskmanager.network.numberOfBuffers")
			.defaultValue(2048);

	/**
	 * Fraction of JVM memory to use for network buffers.
	 *
	 * @deprecated use {@link TaskManagerOptions#NETWORK_MEMORY_FRACTION} instead
	 */
	@Deprecated
	public static final ConfigOption<Float> NETWORK_BUFFERS_MEMORY_FRACTION =
		key("taskmanager.network.memory.fraction")
			.defaultValue(0.1f)
			.withDescription("Fraction of JVM memory to use for network buffers. This determines how many streaming" +
				" data exchange channels a TaskManager can have at the same time and how well buffered the channels" +
				" are. If a job is rejected or you get a warning that the system has not enough buffers available," +
				" increase this value or the min/max values below. Also note, that \"taskmanager.network.memory.min\"" +
				"` and \"taskmanager.network.memory.max\" may override this fraction.");

	/**
	 * Minimum memory size for network buffers.
	 *
	 * @deprecated use {@link TaskManagerOptions#NETWORK_MEMORY_MIN} instead
	 */
	@Deprecated
	public static final ConfigOption<String> NETWORK_BUFFERS_MEMORY_MIN =
		key("taskmanager.network.memory.min")
			.defaultValue("64mb")
			.withDescription("Minimum memory size for network buffers.");

	/**
	 * Maximum memory size for network buffers.
	 *
	 * @deprecated use {@link TaskManagerOptions#NETWORK_MEMORY_MAX} instead
	 */
	@Deprecated
	public static final ConfigOption<String> NETWORK_BUFFERS_MEMORY_MAX =
		key("taskmanager.network.memory.max")
			.defaultValue("1gb")
			.withDescription("Maximum memory size for network buffers.");

	/**
	 * Number of network buffers to use for each outgoing/incoming channel (subpartition/input channel).
	 *
	 * <p>Reasoning: 1 buffer for in-flight data in the subpartition + 1 buffer for parallel serialization.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> NETWORK_BUFFERS_PER_CHANNEL =
		key("taskmanager.network.memory.buffers-per-channel")
			.defaultValue(2)
			.withDescription("Number of exclusive network buffers to use for each outgoing/incoming channel (subpartition/inputchannel)" +
				" in the credit-based flow control model. It should be configured at least 2 for good performance." +
				" 1 buffer is for receiving in-flight data in the subpartition and 1 buffer is for parallel serialization.");

	/**
	 * Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate).
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> NETWORK_EXTRA_BUFFERS_PER_GATE =
		key("taskmanager.network.memory.floating-buffers-per-gate")
			.defaultValue(8)
			.withDescription("Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate)." +
				" In credit-based flow control mode, this indicates how many floating credits are shared among all the input channels." +
				" The floating buffers are distributed based on backlog (real-time output buffers in the subpartition) feedback, and can" +
				" help relieve back-pressure caused by unbalanced data distribution among the subpartitions. This value should be" +
				" increased in case of higher round trip times between nodes and/or larger number of machines in the cluster.");

	/**
	 * Number of max buffers can be used for each output subparition.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	@Deprecated
	public static final ConfigOption<Integer> NETWORK_MAX_BUFFERS_PER_CHANNEL =
		key("taskmanager.network.memory.max-buffers-per-channel")
			.defaultValue(10)
			.withDescription("Number of max buffers that can be used for each channel. If a channel exceeds the number of max" +
				" buffers, it will make the task become unavailable, cause the back pressure and block the data processing. This" +
				" might speed up checkpoint alignment by preventing excessive growth of the buffered in-flight data in" +
				" case of data skew and high number of configured floating buffers. This limit is not strictly guaranteed," +
				" and can be ignored by things like flatMap operators, records spanning multiple buffers or single timer" +
				" producing large amount of data.");

	/**
	 * Number of max buffers can be used for each output subparition.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Double> NETWORK_MAX_BUFFERS_PERCENTAGE_PER_CHANNEL =
		key("taskmanager.network.memory.max-buffers-percentage-per-channel")
			.defaultValue(0.5)
			.withDescription("The percentage of max buffers for each channel.");

	/**
	 * The timeout for requesting exclusive buffers for each channel.
	 */
	@Documentation.ExcludeFromDocumentation("This option is purely implementation related, and may be removed as the implementation changes.")
	public static final ConfigOption<Long> NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS =
		key("taskmanager.network.memory.exclusive-buffers-request-timeout-ms")
			.defaultValue(30000L)
			.withDescription("The timeout for requesting exclusive buffers for each channel. Since the number of maximum buffers and " +
					"the number of required buffers is not the same for local buffer pools, there may be deadlock cases that the upstream" +
					"tasks have occupied all the buffers and the downstream tasks are waiting for the exclusive buffers. The timeout breaks" +
					"the tie by failing the request of exclusive buffers and ask users to increase the number of total buffers.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<String> NETWORK_BLOCKING_SHUFFLE_TYPE =
		key("taskmanager.network.blocking-shuffle.type")
			.defaultValue("file")
			.withDescription("The blocking shuffle type, either \"mmap\" or \"file\". The \"auto\" means selecting the property type automatically" +
					" based on system memory architecture (64 bit for mmap and 32 bit for file). Note that the memory usage of mmap is not accounted" +
					" by configured memory limits, but some resource frameworks like yarn would track this memory usage and kill the container once" +
					" memory exceeding some threshold. Also note that this option is experimental and might be changed future.");

	// ------------------------------------------------------------------------
	//  Netty Options
	// ------------------------------------------------------------------------

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> NUM_ARENAS =
		key("taskmanager.network.netty.num-arenas")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.num-arenas")
			.withDescription("The number of Netty arenas.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> NUM_THREADS_SERVER =
		key("taskmanager.network.netty.server.numThreads")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.server.numThreads")
			.withDescription("The number of Netty server threads.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> NUM_THREADS_CLIENT =
		key("taskmanager.network.netty.client.numThreads")
			.defaultValue(-1)
			.withDeprecatedKeys("taskmanager.net.client.numThreads")
			.withDescription("The number of Netty client threads.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> CONNECT_BACKLOG =
		key("taskmanager.network.netty.server.backlog")
			.defaultValue(0) // default: 0 => Netty's default
			.withDeprecatedKeys("taskmanager.net.server.backlog")
			.withDescription("The netty server connection backlog.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> CLIENT_CONNECT_TIMEOUT_SECONDS =
		key("taskmanager.network.netty.client.connectTimeoutSec")
			.defaultValue(120) // default: 120s = 2min
			.withDeprecatedKeys("taskmanager.net.client.connectTimeoutSec")
			.withDescription("The Netty client connection timeout.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> CLIENT_CONNECT_MAX_RETRY_TIMES =
		key("taskmanager.network.netty.client.connectMaxRetryTimes")
			.defaultValue(3)
			.withDeprecatedKeys("taskmanager.net.client.connectMaxRetryTimes")
			.withDescription("The Netty client connection max retry time while having some exceptions in connection establishment phase.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> SEND_RECEIVE_BUFFER_SIZE =
		key("taskmanager.network.netty.sendReceiveBufferSize")
			.defaultValue(0) // default: 0 => Netty's default
			.withDeprecatedKeys("taskmanager.net.sendReceiveBufferSize")
			.withDescription("The Netty send and receive buffer size. This defaults to the system buffer size" +
				" (cat /proc/sys/net/ipv4/tcp_[rw]mem) and is 4 MiB in modern Linux.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<String> TRANSPORT_TYPE =
		key("taskmanager.network.netty.transport")
			.defaultValue("auto")
			.withDeprecatedKeys("taskmanager.net.transport")
			.withDescription("The Netty transport type, either \"nio\" or \"epoll\". The \"auto\" means selecting the property mode automatically" +
				" based on the platform. Note that the \"epoll\" mode can get better performance, less GC and have more advanced features which are" +
				" only available on modern Linux.");

	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> CONNECTION_TCP_USER_TIMEOUT_SECONDS =
		key("taskmanager.network.netty.connection.tcp-user-timeout-seconds")
			.intType()
			.defaultValue(0) // default: 0s
			.withFallbackKeys("taskmanager.network.netty.client.tcp-user-timeout-seconds")
			.withDescription("The Netty connection socket ack timeout. The default value is zero which is no effect on the socket." +
				" Socket failure may take up to 2 hours with the system default value." +
				" Two hours is the default tcp keepalive time, which means that no TCP data is transmitted between the two parties。" +
				" However, if we only lose ack packets, the client will think that the connection is alive and will not be able to throw an exception after 2 hours" +
				" Decreasing tcp_user_timeout allows applications to fail fast." +
				" See https://man7.org/linux/man-pages/man7/tcp.7.html for more details.");

	// ------------------------------------------------------------------------
	//  Partition Request Options
	// ------------------------------------------------------------------------

	/**
	 * Minimum backoff for partition requests of input channels.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_INITIAL =
		key("taskmanager.network.request-backoff.initial")
			.defaultValue(100)
			.withDeprecatedKeys("taskmanager.net.request-backoff.initial")
			.withDescription("Minimum backoff in milliseconds for partition requests of input channels.");

	/**
	 * Maximum backoff for partition requests of input channels.
	 */
	@Documentation.Section(Documentation.Sections.ALL_TASK_MANAGER_NETWORK)
	public static final ConfigOption<Integer> NETWORK_REQUEST_BACKOFF_MAX =
		key("taskmanager.network.request-backoff.max")
			.defaultValue(10000)
			.withDeprecatedKeys("taskmanager.net.request-backoff.max")
			.withDescription("Maximum backoff in milliseconds for partition requests of input channels.");

	public static final ConfigOption<Boolean> NETWORK_BATCH_PARTITION_REQUET_ENABLE =
		key("taskmanager.network.batch-partition-request.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("When true downstream task package and send PartitionRequest data of the same connection");

	public static final ConfigOption<Boolean> NETWORK_PARTITION_REQUEST_NOTIFY_ENABLE =
		key("taskmanager.partition-request-notify.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("When true, the server will hold and listen the partition request from client until the partition is registered and notify the request, " +
				"otherwise the server will return PartitionNotFoundException to client and it will send partition request to server again.");

	public static final ConfigOption<Long> NETWORK_PARTITION_REQUEST_NOTIFY_TIMEOUT =
		key("taskmanager.partition-request-notify.timeout-ms")
			.longType()
			.defaultValue(30000L)
			.withDescription("The server will return PartitionNotFoundException if partition is still not setup after timeout time.");

	public static final ConfigOption<Long> NETWORK_PARTITION_REQUEST_NOTIFY_TIMEOUT_INTERVAL =
		key("taskmanager.partition-request-notify.timeoutInterval-ms")
			.longType()
			.defaultValue(10000L)
			.withDescription("Partition request notify check interval.");

	// ------------------------------------------------------------------------

	@Documentation.ExcludeFromDocumentation("dev use only; likely temporary")
	public static final ConfigOption<Boolean> FORCE_PARTITION_RELEASE_ON_CONSUMPTION =
		key("taskmanager.network.partition.force-release-on-consumption")
			.defaultValue(false);

	public static final ConfigOption<Boolean> FORCE_PARTITION_RECOVERABLE =
		key("taskmanager.network.partition.force-partition-recoverable")
			.defaultValue(false);

	public static final ConfigOption<Boolean> NETWORK_BUFFERS_MEMORY_LAZY_ALLOCATE =
		key("taskmanager.network.memory.lazy-allocate")
			.defaultValue(false)
			.withDescription("Whether to allocate all memory segments at startup or allocate when task request buffers.");

	public static final ConfigOption<Long> NETWORK_BUFFER_MEMORY_REQUEST_TIMEOUT_MILLS =
		key("taskmanager.network.segment.request-timeout-ms")
			.longType()
			.defaultValue(0L)
			.withDescription("Request network segment from local buffer timeout, 0 for waiting until some other segments are available.");

	public static final ConfigOption<Boolean> NETWORK_CHANNEL_REUSE_ENABLE =
		key("taskmanager.network.channel-reuse.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("TaskManager will reuse netty channel between job execution.");

	public static final ConfigOption<Long> NETWORK_CHANNEL_IDLE_TOLERANT_TIME_MS =
		key("taskmanager.network.channel-idle-tolerant-time-ms")
			.longType()
			.defaultValue(300_000L)
			.withDescription("The time (in ms) to keep the netty channel which isn't assign to any task.");

	public static final ConfigOption<Boolean> NETWORK_BUFFER_POOL_SIMPLE_REDISTRIBUTE_ENABLE =
		key("taskmanager.network-buffer-pool-simple-redistribute.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("Network buffer pool will redistribute all the local buffer pool, " +
				"and if this flag is true, the network buffer pool will be redistributed only when " +
				"localBufferPool created or networkBufferPool's availableSegments not enough.");

	public static final ConfigOption<Double> NETWORK_BUFFER_POOL_SIMPLE_REDISTRIBUTE_HIGN_WATERMARK =
		key("taskmanager.network-buffer-pool-simple-redistribute.high-watermark")
			.doubleType()
			.defaultValue(0.8)
			.withDescription("Network buffer pool will be set max value if used memory's proportion " +
				"of occupancy is less than high-watermark.");

	// ------------------------------------------------------------------------
	//  NetworkBufferPool options
	// ------------------------------------------------------------------------

	/**
	 * This option is for OLAP, and will speed up the request/recycle between
	 * LocalBufferPool and NetworkBufferPool by transferring several MemorySegment as a package at once.
	 * For this, exclusive buffers will be request/recycle by LocalBufferPool instead of NetworkBufferPool.
	 */
	public static final ConfigOption<Boolean> NETWORK_BUFFER_POOL_SEGMENT_PACKAGE_ENABLE = ConfigOptions
		.key("taskmanager.network-buffer-pool-segment-package.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("If true, network buffer pool will use segment package as the unit" +
				"consisting of several memory segments to manage all available memory segments.");

	public static final ConfigOption<Integer> NETWORK_MEMORY_SEGMENTS_PER_PACKAGE = ConfigOptions
		.key("taskmanager.network.memory.memory-segments-per-package")
			.intType()
			.defaultValue(5)
			.withDescription("The number of memory segments to form a package, this will work only if" +
				"NETWORK_BUFFER_POOL_SEGMENT_PACKAGE_ENABLE is set.");

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private NettyShuffleEnvironmentOptions() {}
}
