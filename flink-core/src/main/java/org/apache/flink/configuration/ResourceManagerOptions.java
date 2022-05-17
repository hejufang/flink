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
import org.apache.flink.configuration.description.Description;

/**
 * The set of configuration options relating to the ResourceManager.
 */
@PublicEvolving
public class ResourceManagerOptions {

	/**
	 * Timeout for jobs which don't have a job manager as leader assigned.
	 */
	public static final ConfigOption<String> JOB_TIMEOUT = ConfigOptions
		.key("resourcemanager.job.timeout")
		.defaultValue("5 minutes")
		.withDescription("Timeout for jobs which don't have a job manager as leader assigned.");

	/**
	 * This option is not used any more.
	 */
	@Deprecated
	public static final ConfigOption<Integer> LOCAL_NUMBER_RESOURCE_MANAGER = ConfigOptions
		.key("local.number-resourcemanager")
		.defaultValue(1)
		.withDescription("The number of resource managers start.");

	/**
	 * Defines the network port to connect to for communication with the resource manager.
	 * By default, the port of the JobManager, because the same ActorSystem is used. Its not
	 * possible to use this configuration key to define port ranges.
	 */
	public static final ConfigOption<Integer> IPC_PORT = ConfigOptions
		.key("resourcemanager.rpc.port")
		.defaultValue(0)
		.withDescription("Defines the network port to connect to for communication with the resource manager. By" +
			" default, the port of the JobManager, because the same ActorSystem is used." +
			" Its not possible to use this configuration key to define port ranges.");


	/**
	 * Defines the maximum number of worker (YARN / Mesos) failures can happen in WORKERS_FAILURE_INTERVAL_MS before
	 * rejecting subsequent worker requests until the failure rate falls below the maximum. It is to quickly catch
	 * external dependency caused workers failure and terminate job accordingly.
	 * Be default, -1.0 is set to disable the feature.
	 */
	public static final ConfigOption<Integer> MAXIMUM_WORKERS_FAILURE_RATE = ConfigOptions
		.key("resourcemanager.maximum-workers-failure-rate")
		.defaultValue(-1)
		.withDescription("Defines the maximum number of worker (YARN / Mesos) failures per minute before rejecting" +
			" subsequent worker requests until the failure rate falls below the maximum. It is to quickly catch" +
			" external dependency caused workers failure and terminate job accordingly." +
			" Be default, -1 is set to disable the feature.");

	/**
	 * Defines the maximum ratio of failed workers to effective workers in WORKERS_FAILURE_INTERVAL_MS.
	 * Be default, -1.0 is set to disable the feature.
	 */
	public static final ConfigOption<Double> MAXIMUM_WORKERS_FAILURE_RATE_RATIO = ConfigOptions
		.key("resourcemanager.maximum-workers-failure-rate-ratio")
		.defaultValue(-1.0)
		.withDescription("Defines the maximum ratio of failed workers to effective workers in WORKERS_FAILURE_INTERVAL_MS." +
			"Be default, -1.0 is set to disable the feature.");

	/**
	 * Defines time duration in milliseconds for counting workers (YARN / Mesos) failure happened.
	 */
	public static final ConfigOption<Long> WORKERS_FAILURE_INTERVAL_MS = ConfigOptions
		.key("resourcemanager.workers-failure-interval")
		.defaultValue(60000L)
		.withDescription("Defines time duration in milliseconds for counting workers (YARN / Mesos) failure happened."
		);

	@Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
	public static final ConfigOption<Integer> MAX_SLOT_NUM = ConfigOptions
		.key("slotmanager.number-of-slots.max")
		.intType()
		.defaultValue(Integer.MAX_VALUE)
		.withDescription("Defines the maximum number of slots that the Flink cluster allocates. This configuration option " +
			"is meant for limiting the resource consumption for batch workloads. It is not recommended to configure this option " +
			"for streaming workloads, which may fail if there are not enough slots. Note that this configuration option does not take " +
			"effect for standalone clusters, where how many slots are allocated is not controlled by Flink.");

	@Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
	public static final ConfigOption<Integer> MAX_PENDING_JOB_SLOT_REQUESTS_SIZE = ConfigOptions
			.key("slotmanager.max-pending-job-slot-requests-size")
			.intType()
			.defaultValue(Integer.MAX_VALUE)
			.withDescription("Defines the maximum number of pending job slots requests that the Flink cluster allocates.");

	/**
	 * Whether assign slot random when jobmanager request batch slots from resourcemanager.
	 */
	public static final ConfigOption<Boolean> BATCH_REQUEST_RANDOM_SLOTS_ENABLE = ConfigOptions
		.key("resourcemanager.batch.request-random-slots.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("True for assign random from free slots in batch request, false for assign slot in order.");

	/**
	 * The timeout for a slot request to be discarded, in milliseconds.
	 * @deprecated Use {@link JobManagerOptions#SLOT_REQUEST_TIMEOUT}.
	 */
	@Deprecated
	public static final ConfigOption<Long> SLOT_REQUEST_TIMEOUT = ConfigOptions
		.key("slotmanager.request-timeout")
		.defaultValue(-1L)
		.withDescription("The timeout for a slot request to be discarded.");

	/**
	 * Time in milliseconds of the start-up period of a standalone cluster.
	 * During this time, resource manager of the standalone cluster expects new task executors to be registered, and
	 * will not fail slot requests that can not be satisfied by any current registered slots.
	 * After this time, it will fail pending and new coming requests immediately that can not be satisfied by registered
	 * slots.
	 * If not set, {@link #SLOT_REQUEST_TIMEOUT} will be used by default.
	 */
	public static final ConfigOption<Long> STANDALONE_CLUSTER_STARTUP_PERIOD_TIME = ConfigOptions
		.key("resourcemanager.standalone.start-up-time")
		.defaultValue(-1L)
		.withDescription("Time in milliseconds of the start-up period of a standalone cluster. During this time, "
			+ "resource manager of the standalone cluster expects new task executors to be registered, and will not "
			+ "fail slot requests that can not be satisfied by any current registered slots. After this time, it will "
			+ "fail pending and new coming requests immediately that can not be satisfied by registered slots. If not "
			+ "set, 'slotmanager.request-timeout' will be used by default.");

	/**
	 * The timeout for an idle task manager to be released, in milliseconds.
	 * @deprecated Use {@link #TASK_MANAGER_TIMEOUT}.
	 */
	@Deprecated
	public static final ConfigOption<Long> SLOT_MANAGER_TASK_MANAGER_TIMEOUT = ConfigOptions
		.key("slotmanager.taskmanager-timeout")
		.defaultValue(30000L)
		.withDescription("The timeout for an idle task manager to be released.");

	/**
	 * The timeout for an idle task manager to be released, in milliseconds.
	 */
	public static final ConfigOption<Long> TASK_MANAGER_TIMEOUT = ConfigOptions
		.key("resourcemanager.taskmanager-timeout")
		.defaultValue(30000L)
		.withDeprecatedKeys(SLOT_MANAGER_TASK_MANAGER_TIMEOUT.key())
		.withDescription(Description.builder()
			.text("The timeout for an idle task manager to be released.")
			.build());

	/**
	 * Release task executor only when each produced result partition is either consumed or failed.
	 *
	 * <p>Currently, produced result partition is released when it fails or consumer sends close request
	 * to confirm successful end of consumption and to close the communication channel.
	 *
	 * @deprecated The default value should be reasonable enough in all cases, this option is to fallback to older behaviour
	 * which will be removed or refactored in future.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> TASK_MANAGER_RELEASE_WHEN_RESULT_CONSUMED = ConfigOptions
		.key("resourcemanager.taskmanager-release.wait.result.consumed")
		.defaultValue(true)
		.withDescription(Description.builder()
			.text("Release task executor only when each produced result partition is either consumed or failed. " +
				"'True' is default. 'False' means that idle task executor release is not blocked " +
				"by receiver confirming consumption of result partition " +
				"and can happen right away after 'resourcemanager.taskmanager-timeout' has elapsed. " +
				"Setting this option to 'false' can speed up task executor release but can lead to unexpected failures " +
				"if end of consumption is slower than 'resourcemanager.taskmanager-timeout'.")
			.build());

	public static final ConfigOption<Boolean> PREVIOUS_CONTAINER_AS_PENDING = ConfigOptions
			.key("resourcemanager.previous-container.as-pending-container")
			.booleanType()
			.defaultValue(false)
			.withDeprecatedKeys("yarn.previous-container.as-pending-container")
			.withDescription("Whether make recovered WorkerNode as pending working.");

	public static final ConfigOption<Long> PREVIOUS_CONTAINER_TIMEOUT_MS = ConfigOptions
			.key("resourcemanager.previous-container.timeout-ms")
			.longType()
			.defaultValue(120_000L)
			.withDeprecatedKeys("yarn.previous-container.timeout-ms")
			.withDescription("Timeout to wait previous containers register to ResourceManager." +
					"<= 0 means never timeout.");

	public static final ConfigOption<Boolean> SLOW_CONTAINER_ENABLED = ConfigOptions
			.key("resourcemanager.slow-container.enabled")
			.booleanType()
			.defaultValue(false)
			.withDeprecatedKeys("yarn.slow-container.enabled")
			.withDescription("Whether enable slow container mechanism.");

	public static final ConfigOption<Long> SLOW_CONTAINER_TIMEOUT_MS = ConfigOptions
			.key("resourcemanager.slow-container.timeout-ms")
			.longType()
			.defaultValue(120000L)
			.withDeprecatedKeys("yarn.slow-container.timeout-ms")
			.withDescription("Timeout in milliseconds of determine if the container is slow.");

	public static final ConfigOption<Long> SLOW_CONTAINER_CHECK_INTERVAL_MS = ConfigOptions
			.key("resourcemanager.slow-container.check-interval-ms")
			.longType()
			.defaultValue(10000L)
			.withDeprecatedKeys("yarn.slow-container.check-interval-ms")
			.withDescription("Interval in milliseconds of check if the container is slow.");

	public static final ConfigOption<Double> SLOW_CONTAINERS_QUANTILE = ConfigOptions
			.key("resourcemanager.slow-container.quantile")
			.doubleType()
			.defaultValue(0.9)
			.withDeprecatedKeys("yarn.slow-container.quantile")
			.withDescription("The quantile of slow container timeout base threshold." +
					"Means how many containers should be started before update slow container timeout threshold.");

	public static final ConfigOption<Double> SLOW_CONTAINER_THRESHOLD_FACTOR = ConfigOptions
			.key("resourcemanager.slow-container.threshold-factor")
			.doubleType()
			.defaultValue(1.3)
			.withDeprecatedKeys("yarn.slow-container.threshold-factor")
			.withDescription("The threshold factor for slow container base timeout. " +
					"This means all containers having not been registered after threshold-factor times base-timeout should be marked as slow containers.");

	public static final ConfigOption<Double> SLOW_CONTAINER_REDUNDANT_MAX_FACTOR = ConfigOptions
			.key("resourcemanager.slow-container.redundant-max-factor")
			.doubleType()
			.defaultValue(0.2)
			.withDescription("Factor of max redundant workers.");

	public static final ConfigOption<Integer> SLOW_CONTAINER_REDUNDANT_MIN_NUMBER = ConfigOptions
			.key("resourcemanager.slow-container.redundant-min-number")
			.intType()
			.defaultValue(5)
			.withDescription("Number of min redundant workers.");

	public static final ConfigOption<Boolean> SLOW_CONTAINER_RELEASE_TIMEOUT_ENABLED = ConfigOptions
			.key("resourcemanager.slow-container.release-timeout-enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether enabled stop workers that start too slowly.");

	public static final ConfigOption<Long> SLOW_CONTAINER_RELEASE_TIMEOUT_MS = ConfigOptions
			.key("resourcemanager.slow-container.release-timeout-ms")
			.longType()
			.defaultValue(5 * 60 * 1000L)
			.withDescription("Timeout for releasing slow workers.");

	/**
	 * Prefix for passing custom environment variables to Flink's master process.
	 * For example for passing LD_LIBRARY_PATH as an env variable to the AppMaster, set:
	 * containerized.master.env.LD_LIBRARY_PATH: "/usr/lib/native"
	 * in the flink-conf.yaml.
	 */
	public static final String CONTAINERIZED_MASTER_ENV_PREFIX = "containerized.master.env.";

	/**
	 * Similar to the {@see CONTAINERIZED_MASTER_ENV_PREFIX}, this configuration prefix allows
	 * setting custom environment variables for the workers (TaskManagers).
	 */
	public static final String CONTAINERIZED_TASK_MANAGER_ENV_PREFIX = "containerized.taskmanager.env.";

	// ---------------------------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private ResourceManagerOptions() {}
}
