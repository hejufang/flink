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

package org.apache.flink.yarn.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * This class holds configuration constants used by Flink's YARN runners.
 *
 * <p>These options are not expected to be ever configured by users explicitly.
 */
public class YarnConfigOptions {

	/**
	 * The hostname or address where the application master RPC system is listening.
	 */
	public static final ConfigOption<String> APP_MASTER_RPC_ADDRESS =
			key("yarn.appmaster.rpc.address")
			.noDefaultValue()
			.withDescription("The hostname or address where the application master RPC system is listening.");

	/**
	 * The port where the application master RPC system is listening.
	 */
	public static final ConfigOption<Integer> APP_MASTER_RPC_PORT =
			key("yarn.appmaster.rpc.port")
			.defaultValue(-1)
			.withDescription("The port where the application master RPC system is listening.");

	/**
	 * The vcores used by YARN application master.
	 */
	public static final ConfigOption<Integer> APP_MASTER_VCORES =
		key("yarn.appmaster.vcores")
		.defaultValue(1)
		.withDescription("The number of virtual cores (vcores) used by YARN application master.");

	/**
	 * Defines whether user-jars are included in the system class path for per-job-clusters as well as their positioning
	 * in the path. They can be positioned at the beginning ("FIRST"), at the end ("LAST"), or be positioned based on
	 * their name ("ORDER").
	 */
	public static final ConfigOption<String> CLASSPATH_INCLUDE_USER_JAR =
		key("yarn.per-job-cluster.include-user-jar")
			.defaultValue("ORDER")
			.withDescription("Defines whether user-jars are included in the system class path for per-job-clusters as" +
				" well as their positioning in the path. They can be positioned at the beginning (\"FIRST\"), at the" +
				" end (\"LAST\"), or be positioned based on their name (\"ORDER\").");

	/**
	 * The vcores exposed by YARN.
	 */
	public static final ConfigOption<Double> VCORES =
		key("yarn.containers.vcores")
			.defaultValue(-1.0)
			.withDescription(Description.builder().text(
					"The number of virtual cores (vcores) per YARN container. By default, the number of vcores" +
					" is set to the number of slots per TaskManager, if set, or to 1, otherwise. In order for this" +
					" parameter to be used your cluster must have CPU scheduling enabled. You can do this by setting" +
					" the %s.",
				code("org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"))
				.build());

	/**
	 * The maximum number of failed YARN containers before entirely stopping
	 * the YARN session / job on YARN.
	 * By default, we take the number of initially requested containers.
	 *
	 * <p>Note: This option returns a String since Integer options must have a static default value.
	 */
	public static final ConfigOption<String> MAX_FAILED_CONTAINERS =
		key("yarn.maximum-failed-containers")
		.noDefaultValue()
		.withDescription("Maximum number of containers the system is going to reallocate in case of a failure.");

	/**
	 * Set the number of retries for failed YARN ApplicationMasters/JobManagers in high
	 * availability mode. This value is usually limited by YARN.
	 * By default, it's 1 in the standalone case and 2 in the high availability case.
	 *
	 * <p>>Note: This option returns a String since Integer options must have a static default value.
	 */
	public static final ConfigOption<String> APPLICATION_ATTEMPTS =
		key("yarn.application-attempts")
		.noDefaultValue()
		.withDescription("Number of ApplicationMaster restarts. Note that that the entire Flink cluster will restart" +
			" and the YARN Client will loose the connection. Also, the JobManager address will change and you’ll need" +
			" to set the JM host:port manually. It is recommended to leave this option at 1.");

	/**
	 * The config parameter defining the attemptFailuresValidityInterval of Yarn application.
	 */
	public static final ConfigOption<Long> APPLICATION_ATTEMPT_FAILURE_VALIDITY_INTERVAL =
		key("yarn.application-attempt-failures-validity-interval")
		.defaultValue(10000L)
		.withDescription(Description.builder()
			.text("Time window in milliseconds which defines the number of application attempt failures when restarting the AM. " +
				"Failures which fall outside of this window are not being considered. " +
				"Set this value to -1 in order to count globally. " +
				"See %s for more information.", link("https://hortonworks.com/blog/apache-hadoop-yarn-hdp-2-2-fault-tolerance-features-long-running-services/", "here"))
			.build());

	/**
	 * The heartbeat interval between the Application Master and the YARN Resource Manager.
	 */
	public static final ConfigOption<Integer> HEARTBEAT_DELAY_SECONDS =
		key("yarn.heartbeat.interval")
		.defaultValue(5)
		.withDeprecatedKeys("yarn.heartbeat-delay")
		.withDescription("Time between heartbeats with the ResourceManager in seconds.");

	/**
	 * The heartbeat interval between the Application Master and the YARN Resource Manager
	 * if Flink is requesting containers.
	 */
	public static final ConfigOption<Integer> CONTAINER_REQUEST_HEARTBEAT_INTERVAL_MILLISECONDS =
		key("yarn.heartbeat.container-request-interval")
			.defaultValue(500)
			.withDescription(
				new Description.DescriptionBuilder()
					.text("Time between heartbeats with the ResourceManager in milliseconds if Flink requests containers:")
					.list(
						text("The lower this value is, the faster Flink will get notified about container allocations since requests and allocations are transmitted via heartbeats."),
						text("The lower this value is, the more excessive containers might get allocated which will eventually be released but put pressure on Yarn."))
					.text("If you observe too many container allocations on the ResourceManager, then it is recommended to increase this value. See %s for more information.", link("https://issues.apache.org/jira/browse/YARN-1902", "this link"))
					.build());

	/**
	 * When a Flink job is submitted to YARN, the JobManager's host and the number of available
	 * processing slots is written into a properties file, so that the Flink client is able
	 * to pick those details up.
	 * This configuration parameter allows changing the default location of that file (for example
	 * for environments sharing a Flink installation between users)
	 */
	public static final ConfigOption<String> PROPERTIES_FILE_LOCATION =
		key("yarn.properties-file.location")
		.noDefaultValue()
		.withDescription("When a Flink job is submitted to YARN, the JobManager’s host and the number of available" +
			" processing slots is written into a properties file, so that the Flink client is able to pick those" +
			" details up. This configuration parameter allows changing the default location of that file" +
			" (for example for environments sharing a Flink installation between users).");

	/**
	 * The config parameter defining the Akka actor system port for the ApplicationMaster and
	 * JobManager.
	 * The port can either be a port, such as "9123",
	 * a range of ports: "50100-50200"
	 * or a list of ranges and or points: "50100-50200,50300-50400,51234".
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final ConfigOption<String> APPLICATION_MASTER_PORT =
		key("yarn.application-master.port")
		.defaultValue("0")
		.withDescription("With this configuration option, users can specify a port, a range of ports or a list of ports" +
			" for the Application Master (and JobManager) RPC port. By default we recommend using the default value (0)" +
			" to let the operating system choose an appropriate port. In particular when multiple AMs are running on" +
			" the same physical host, fixed port assignments prevent the AM from starting. For example when running" +
			" Flink on YARN on an environment with a restrictive firewall, this option allows specifying a range of" +
			" allowed ports.");

	/**
	 * A non-negative integer indicating the priority for submitting a Flink YARN application. It will only take effect
	 * if YARN priority scheduling setting is enabled. Larger integer corresponds with higher priority. If priority is
	 * negative or set to '-1', Flink will set yarn priority to 0.
	 *
	 * @see <a href="https://hadoop.apache.org/docs/r2.8.5/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html">YARN Capacity Scheduling Doc</a>
	 */
	public static final ConfigOption<Integer> APPLICATION_PRIORITY =
		key("yarn.application.priority")
			.defaultValue(0)
			.withDescription("A non-negative integer indicating the priority for submitting a Flink YARN application. It" +
				" will only take effect if YARN priority scheduling setting is enabled. Larger integer corresponds" +
				" with higher priority. If priority is negative or set to '-1', Flink will set yarn priority" +
				" to 0. Please refer to YARN's official documentation for specific" +
				" settings required to enable priority scheduling for the targeted YARN version.");

	/**
	 * A comma-separated list of strings to use as YARN application tags.
	 */
	public static final ConfigOption<String> APPLICATION_TAGS =
		key("yarn.tags")
		.defaultValue("")
		.withDescription("A comma-separated list of tags to apply to the Flink YARN application.");

	/**
	 * Enable NMClientAsync when start task managers.
	 */
	public static final ConfigOption<Boolean> NMCLINETASYNC_ENABLED =
		key("nmclientasync.enabled")
			.defaultValue(true)
			.withDescription("Enable NMClientAsync when start task managers.");

	/**
	 * The number of threads to start yarn containers in yarn resource manager.
	 */
	public static final ConfigOption<Integer> CONTAINER_LAUNCHER_NUMBER =
		key("yarn.container-launcher-number")
			.defaultValue(10)
			.withDescription("The number of threads to start yarn containers in yarn resource manager.");

	public static final ConfigOption<String> NODE_SATISFY_ATTRIBUTES_EXPRESSION =
			key("yarn.node-satisfy-attributes-expression")
					.noDefaultValue()
					.withDescription("The expression of node attributes, such as has_gpu,is_gemini,pod...");

	// ------------------------------------------------------------------------
	/**
	 * Use GangScheduler when allocate containers from Yarn.
	 */
	public static final ConfigOption<Boolean> GANG_SCHEDULER =
		key("yarn.gang-scheduler.enable")
			.defaultValue(false)
			.withDescription("Use GangScheduler when allocate containers from Yarn.");

	public static final ConfigOption<Boolean> GANG_SCHEDULER_JOB_MANAGER =
		key("yarn.gang-scheduler.jobmanager.enable")
			.defaultValue(false)
			.withDescription("Use GangScheduler when allocate job manager from Yarn.");

	/**
	 * Skip nodes which load > GANG_NODE_SKIP_HIGH_LOAD * node_cores.
	 */
	public static final ConfigOption<Float> GANG_NODE_SKIP_HIGH_LOAD =
		key("yarn.gang-scheduler.node-skip-high-load")
			.defaultValue(2.5f)
			.withDescription("Skip nodes which load > GANG_NODE_SKIP_HIGH_LOAD * node_cores." +
				"0 means disable this constraints.");

	/**
	 * The weight of container-decentralized-average in GANG Scheduler.
	 * 0 means disable this constraints.
	 */
	public static final ConfigOption<Integer> GANG_CONTAINER_DECENTRALIZED_AVERAGE_WEIGHT =
		key("yarn.gang-scheduler.container-decentralized-average-weight")
			.defaultValue(0)
			.withDescription("The weight of container-decentralized-average in GANG Scheduler." +
				"0 means disable this constraints.");
	/**
	 * The weight of node-quota-usage-average in GANG Scheduler.
	 * 0 means disable this constraints.
	 */
	public static final ConfigOption<Integer> GANG_NODE_QUOTA_USAGE_AVERAGE_WEIGHT =
		key("yarn.gang-scheduler.node-quota-usage-average-weight")
			.defaultValue(1)
			.withDescription("The weight of node-quota-usage-average in GANG Scheduler." +
				"0 means disable this constraints.");

	/**
	 * Wait time before exit when GangScheduler fatal.
	 */
	public static final ConfigOption<Integer> WAIT_TIME_BEFORE_GANG_FATAL_MS =
		key("yarn.gang-scheduler.wait-time-before-fatal-ms")
			.defaultValue(300000)
			.withDescription("Wait time before exit when GangScheduler fatal.");

	/**
	 * Wait time before retry by GangScheduler.
	 */
	public static final ConfigOption<Integer> WAIT_TIME_BEFORE_GANG_RETRY_MS =
		key("yarn.gang-scheduler.wait-time-before-retry-ms")
			.defaultValue(1000)
			.withDescription("Wait time before retry by GangScheduler.");

	/**
	 * Max retry times by GangScheduler, downgrade to fairScheduler in DOWNGRADE_TIMEOUT when retry times exceeds it.
	 */
	public static final ConfigOption<Integer> GANG_MAX_RETRY_TIMES =
		key("yarn.gang-scheduler.max-retry-times")
			.defaultValue(5)
			.withDescription("Max retry times by GangScheduler, downgrade to fairScheduler if exceeds");

	public static final ConfigOption<Boolean> GANG_DOWNGRADE_ON_FAILED =
			key("yarn.gang-scheduler.gang-downgrade-on-failed")
					.defaultValue(true)
					.withDescription("Whether downgrade to FairScheduler when Gang Failed too many times." +
							"true downgrade to FairScheduler." +
							"false exit application.");

	/**
	 * Back to GangScheduler after downgrade timeout. Only work when gang scheduler enabled.
	 */
	public static final ConfigOption<Integer> GANG_DOWNGRADE_TIMEOUT_MS =
		key("yarn.gang-scheduler.downgrade-timeout-ms")
			.defaultValue(1800000)
			.withDescription("Back to GangScheduler after downgrade timeout.");

	/**
	 * Open container descheduler in gang scheduler.
	 */
	public static final ConfigOption<Boolean> GANG_CONTAINER_DESCHEDULER_ENABLE =
		key("yarn.gang-scheduler.container-descheduler.enable")
			.defaultValue(false)
			.withDescription("Enable yarn container descheduler in gang scheduler.");

	/**
	 * Enable disk type container descheduler in gang scheduler.
	 */
	public static final ConfigOption<Boolean> GANG_CONTAINER_DESCHEDULER_DISK_TYPE_ENABLE =
		key("yarn.gang-scheduler.container-descheduler.disk-type-enable")
			.defaultValue(false)
			.withDescription("Enable yarn container descheduler in gang scheduler for disk constraint type.");

	/**
	 * Enable load type container descheduler in gang scheduler.
	 */
	public static final ConfigOption<Boolean> GANG_CONTAINER_DESCHEDULER_LOAD_TYPE_ENABLE =
		key("yarn.gang-scheduler.container-descheduler.load-type-enable")
			.defaultValue(false)
			.withDescription("Enable yarn container descheduler in gang scheduler for load constraint type.");

	/**
	 * Interval minimum time for performance type(e.g. machine load problem) container descheduler in gang scheduler.
	 */
	public static final ConfigOption<Long> GANG_CONTAINER_DESCHEDULER_PERFORMANCE_TYPE_INTERVAL_MIN_MS =
		key("yarn.gang-scheduler.container-descheduler.performance-type.interval-min-ms")
			.defaultValue(6 * 60 * 60 * 1000L)
			.withDescription("Minimum time interval before next scheduling of container performance-type descheduler in gang scheduler.");

	/**
	 * Interval minimum time for usability type(e.g. disk problem) container descheduler in gang scheduler.
	 */
	public static final ConfigOption<Long> GANG_CONTAINER_DESCHEDULER_USABILITY_TYPE_INTERVAL_MIN_MS =
		key("yarn.gang-scheduler.container-descheduler.usability-type.interval-min-ms")
			.defaultValue(30 * 60 * 1000L)
			.withDescription("Minimum time interval before next scheduling of container usability-type descheduler in gang scheduler.");

	public static final ConfigOption<String> PROVIDED_LIB_DIRS =
		key("yarn.provided.lib.dirs")
			.noDefaultValue()
			.withDescription("A semicolon-separated list of provided lib directories. They should be pre-uploaded and " +
				"world-readable. Flink will use them to exclude the local Flink jars(e.g. flink-dist, lib/, plugins/)" +
				"uploading to accelerate the job submission process. Also YARN will cache them on the nodes so that " +
				"they doesn't need to be downloaded every time for each application. An example could be " +
				"hdfs://$namenode_address/path/of/flink/lib");

	public static final ConfigOption<Boolean> PROVIDED_LIB_DIRS_ENABLED =
			key("yarn.provided.lib.dirs.enabled")
					.defaultValue(false)
					.withDescription("Whether enable provided lib dirs.");

	// ------------------------------------------------------------------------

	/**
	 * Specific whether the docker image includes the Flink libs.
	 * */
	public static final ConfigOption<Boolean> IS_DOCKER_INCLUDE_LIB =
		key("docker.image.include_lib")
			.defaultValue(false)
			.withDescription("Specific whether the docker image includes Flink libs.");

	public static final ConfigOption<Boolean> IS_DOCKER_INCLUDE_USERLIB =
		key("docker.image.include_userlib")
			.defaultValue(false)
			.withDescription("Specific whether the docker image includes user libs.");

	public static final ConfigOption<Boolean> SET_TASK_MANAGER_TOKEN =
			key("yarn.taskmanager.set_token")
			.defaultValue(false)
			.withDescription("Whether set security tokens to  TaskManager YARN container context.");

	public static final ConfigOption<Boolean> SLOW_CONTAINER_ENABLED =
			key("yarn.slow-container.enabled")
					.defaultValue(false)
					.withDescription("Whether enable slow container mechanism.");

	public static final ConfigOption<Long> SLOW_CONTAINER_TIMEOUT_MS =
		key("yarn.slow-container.timeout-ms")
			.defaultValue(120000L)
			.withDescription("Timeout in milliseconds of determine if the container is slow.");

	public static final ConfigOption<Long> SLOW_CONTAINER_CHECK_INTERVAL_MS =
		key("yarn.slow-container.check-interval-ms")
		.defaultValue(10000L)
		.withDescription("Interval in milliseconds of check if the container is slow.");

	public static final ConfigOption<Double> SLOW_CONTAINERS_QUANTILE =
			key("yarn.slow-container.quantile")
					.defaultValue(0.9)
					.withDescription("The quantile of slow container timeout base threshold." +
							"Means how many containers should be started before update slow container timeout threshold.");

	public static final ConfigOption<Double> SLOW_CONTAINER_THRESHOLD_FACTOR =
			key("yarn.slow-container.threshold-factor")
					.defaultValue(1.5)
					.withDescription("Times of slow container timeout base threshold." +
							"Means containers which 1.5 times slow than the base threshold should be marked as slow containers.");

	public static final ConfigOption<Boolean> CLEANUP_RUNNING_CONTAINERS_ON_STOP =
		key("yarn.nmclient.cleanup-running-containers-on-stop")
			.defaultValue(false)
			.withDescription("Cleanup running containers on NMClient stop.");

	public static final ConfigOption<Boolean> YARN_CONF_CLUSTER_QUEUE_NAME_ENABLE =
		key("yarn.conf.cluster_queue_name.enable")
			.defaultValue(true)
			.withDescription("Enable set cluster_queue_name to yarn conf.");

	public static final ConfigOption<Boolean> ENV_INCLUDE_FLINK_CONF =
		key("yarn.env-include-flink-conf")
			.defaultValue(false)
			.withDescription("Environment include flink config to avoid uploading to hdfs while starting each container.");


	/**
	 * Yarn configuration key prefix.
	 */
	public static final String YARN_CONFIG_KEY_PREFIX = "flink.yarn.config.";
	public static final String CLIENT_YARN_CONFIG_KEY_PREFIX = "flink.client.yarn.config.";
	public static final String JOBMANAGER_YARN_CONFIG_KEY_PREFIX = "flink.jobmanager.yarn.config.";


	/** This class is not meant to be instantiated. */
	private YarnConfigOptions() {}

	/** @see YarnConfigOptions#CLASSPATH_INCLUDE_USER_JAR */
	public enum UserJarInclusion {
		FIRST,
		LAST,
		ORDER
	}
}
