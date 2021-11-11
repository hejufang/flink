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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.description.Description;

import java.util.List;

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
	 * The vcores used by YARN application master.
	 */
	public static final ConfigOption<Double> APP_MASTER_VCORES =
		key("yarn.appmaster.vcores")
		.doubleType()
		.defaultValue(1.0)
		.withDescription("The number of virtual cores (vcores) used by YARN application master.");

	/**
	 * Defines whether user-jars are included in the system class path for per-job-clusters as well as their positioning
	 * in the path. They can be positioned at the beginning ("FIRST"), at the end ("LAST"), or be positioned based on
	 * their name ("ORDER"). "DISABLED" means the user-jars are excluded from the system class path.
	 */
	public static final ConfigOption<String> CLASSPATH_INCLUDE_USER_JAR =
		key("yarn.per-job-cluster.include-user-jar")
			.defaultValue("ORDER")
			.withDescription("Defines whether user-jars are included in the system class path for per-job-clusters as" +
				" well as their positioning in the path. They can be positioned at the beginning (\"FIRST\"), at the" +
				" end (\"LAST\"), or be positioned based on their name (\"ORDER\"). \"DISABLED\" means the user-jars" +
				" are excluded from the system class path.");

	/**
	 * The vcores exposed by YARN.
	 */
	public static final ConfigOption<Double> VCORES =
		key("yarn.containers.vcores")
			.doubleType()
			.defaultValue(-1.0)
			.withDescription(Description.builder().text(
					"The number of virtual cores (vcores) per YARN container. By default, the number of vcores" +
					" is set to the number of slots per TaskManager, if set, or to 1, otherwise. In order for this" +
					" parameter to be used your cluster must have CPU scheduling enabled. You can do this by setting" +
					" the %s.",
				code("org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"))
				.build());

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
			.intType()
			.defaultValue(0)
			.withDescription("A non-negative integer indicating the priority for submitting a Flink YARN application. It" +
				" will only take effect if YARN priority scheduling setting is enabled. Larger integer corresponds" +
				" with higher priority. If priority is negative or set to '-1', Flink will set yarn priority" +
				" to 0. Please refer to YARN's official documentation for specific" +
				" settings required to enable priority scheduling for the targeted YARN version.");

	/**
	 * Yarn session client uploads flink jar and user libs to file system (hdfs/s3) as local resource for yarn
	 * application context. The replication number changes the how many replica of each of these files in hdfs/s3.
	 * It is useful to accelerate this container bootstrap, when a Flink application needs more than one hundred
	 * of containers. If it is not configured, Flink will use the default replication value in hadoop configuration.
	 */
	public static final ConfigOption<Integer> FILE_REPLICATION =
		key("yarn.file-replication")
			.intType()
			.defaultValue(-1)
			.withDescription("Number of file replication of each local resource file. If it is not configured, Flink will" +
				" use the default replication value in hadoop configuration.");

	/**
	 * A comma-separated list of strings to use as YARN application tags.
	 */
	public static final ConfigOption<String> APPLICATION_TAGS =
		key("yarn.tags")
		.defaultValue("")
		.withDescription("A comma-separated list of tags to apply to the Flink YARN application.");

	// ----------------------- YARN CLI OPTIONS ------------------------------------

	public static final ConfigOption<List<String>> SHIP_DIRECTORIES =
			key("yarn.ship-directories")
				.stringType()
				.asList()
				.noDefaultValue()
				.withDescription("A semicolon-separated list of directories to be shipped to the YARN cluster.");

	public static final ConfigOption<String> FLINK_DIST_JAR =
			key("yarn.flink-dist-jar")
				.stringType()
				.noDefaultValue()
				.withDescription("The location of the Flink dist jar.");

	public static final ConfigOption<String> APPLICATION_ID =
			key("yarn.application.id")
				.stringType()
				.noDefaultValue()
				.withDescription("The YARN application id of the running yarn cluster." +
						" This is the YARN cluster where the pipeline is going to be executed.");

	public static final ConfigOption<String> APPLICATION_QUEUE =
			key("yarn.application.queue")
				.stringType()
				.noDefaultValue()
				.withDescription("The YARN queue on which to put the current pipeline.");

	public static final ConfigOption<String> APPLICATION_NAME =
			key("yarn.application.name")
				.stringType()
				.noDefaultValue()
				.withDescription("A custom name for your YARN application.");

	public static final ConfigOption<String> APPLICATION_TYPE =
			key("yarn.application.type")
				.stringType()
				.defaultValue(ConfigConstants.YARN_STREAMING_APPLICATION_TYPE_DEFAULT)
				.withDescription("A custom type for your YARN application..");

	public static final ConfigOption<String> NODE_LABEL =
			key("yarn.application.node-label")
				.stringType()
				.noDefaultValue()
				.withDescription("Specify YARN node label for the YARN application.");

	public static final ConfigOption<Boolean> SHIP_LOCAL_KEYTAB =
			key("yarn.security.kerberos.ship-local-keytab")
					.booleanType()
					.defaultValue(true)
					.withDescription(
							"When this is true Flink will ship the keytab file configured via " +
									SecurityOptions.KERBEROS_LOGIN_KEYTAB.key() +
									" as a localized YARN resource.");

	public static final ConfigOption<String> LOCALIZED_KEYTAB_PATH =
			key("yarn.security.kerberos.localized-keytab-path")
					.stringType()
					.defaultValue("krb5.keytab")
					.withDescription(
							"Local (on NodeManager) path where kerberos keytab file will be" +
									" localized to. If " + SHIP_LOCAL_KEYTAB.key() + " set to " +
									"true, Flink willl ship the keytab file as a YARN local " +
									"resource. In this case, the path is relative to the local " +
									"resource directory. If set to false, Flink" +
									" will try to directly locate the keytab from the path itself.");

	public static final ConfigOption<List<String>> PROVIDED_LIB_DIRS =
		key("yarn.provided.lib.dirs")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("A semicolon-separated list of provided lib directories. They should be pre-uploaded and " +
				"world-readable. Flink will use them to exclude the local Flink jars(e.g. flink-dist, lib/, plugins/)" +
				"uploading to accelerate the job submission process. Also YARN will cache them on the nodes so that " +
				"they doesn't need to be downloaded every time for each application. An example could be " +
				"hdfs://$namenode_address/path/of/flink/lib");

	public static final ConfigOption<Boolean> PROVIDED_LIB_DIRS_ENABLED =
			key("yarn.provided.lib.dirs.enabled")
					.booleanType()
					.defaultValue(false)
					.withDescription("Whether enable provided lib dirs.");

	public static final ConfigOption<List<String>> PROVIDED_USER_SHARED_LIB_DIRS =
			key("yarn.provided.user.shared.libs")
					.stringType()
					.asList()
					.noDefaultValue()
					.withDescription("User may share jars between jobs such as client jar for CSS.");

	/** Defines the configuration key of that external resource in Yarn. This is used as a suffix in an actual config. */
	public static final String EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX = "yarn.config-key";

	/**
	 * If configured, Flink will add this key to the resource profile of container request to Yarn. The value will be
	 * set to {@link ExternalResourceOptions#EXTERNAL_RESOURCE_AMOUNT}.
	 *
	 * <p>It is intentionally included into user docs while unused.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<String> EXTERNAL_RESOURCE_YARN_CONFIG_KEY =
		key(ExternalResourceOptions.genericKeyWithSuffix(EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX))
			.stringType()
			.noDefaultValue()
			.withDescription("If configured, Flink will add this key to the resource profile of container request to Yarn. " +
				"The value will be set to the value of " + ExternalResourceOptions.EXTERNAL_RESOURCE_AMOUNT.key() + ".");

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
			.booleanType()
			.defaultValue(true)
			.withDescription("Use GangScheduler when allocate containers from Yarn.");

	public static final ConfigOption<Boolean> GANG_SCHEDULER_JOB_MANAGER =
		key("yarn.gang-scheduler.jobmanager.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("Use GangScheduler when allocate job manager from Yarn.");

	/**
	 * Skip nodes which load > GANG_NODE_SKIP_HIGH_LOAD * node_cores.
	 */
	public static final ConfigOption<Float> GANG_NODE_SKIP_HIGH_LOAD =
		key("yarn.gang-scheduler.node-skip-high-load")
			.floatType()
			.defaultValue(1.2f)
			.withDescription("Skip nodes which load > GANG_NODE_SKIP_HIGH_LOAD * node_cores.");

	/**
	 * The weight of container-decentralized-average in GANG Scheduler.
	 * 0 means disable this constraints.
	 */
	public static final ConfigOption<Integer> GANG_CONTAINER_DECENTRALIZED_AVERAGE_WEIGHT =
		key("yarn.gang-scheduler.container-decentralized-average-weight")
			.intType()
			.defaultValue(0)
			.withDescription("The weight of container-decentralized-average in GANG Scheduler." +
				"0 means disable this constraints.");
	/**
	 * The weight of node-quota-usage-average in GANG Scheduler.
	 * 0 means disable this constraints.
	 */
	public static final ConfigOption<Integer> GANG_NODE_QUOTA_USAGE_AVERAGE_WEIGHT =
		key("yarn.gang-scheduler.node-quota-usage-average-weight")
			.intType()
			.defaultValue(1)
			.withDescription("The weight of node-quota-usage-average in GANG Scheduler." +
				"0 means disable this constraints.");

	/**
	 * Wait time before exit when GangScheduler fatal.
	 */
	public static final ConfigOption<Integer> WAIT_TIME_BEFORE_GANG_FATAL_MS =
		key("yarn.gang-scheduler.wait-time-before-fatal-ms")
			.intType()
			.defaultValue(300000)
			.withDescription("Wait time before exit when GangScheduler fatal.");

	/**
	 * Wait time before retry by GangScheduler.
	 */
	public static final ConfigOption<Integer> WAIT_TIME_BEFORE_GANG_RETRY_MS =
		key("yarn.gang-scheduler.wait-time-before-retry-ms")
			.intType()
			.defaultValue(1000)
			.withDescription("Wait time before retry by GangScheduler.");

	/**
	 * Max retry times by GangScheduler, downgrade to fairScheduler in DOWNGRADE_TIMEOUT when retry times exceeds it.
	 */
	public static final ConfigOption<Integer> GANG_MAX_RETRY_TIMES =
		key("yarn.gang-scheduler.max-retry-times")
			.intType()
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
			.intType()
			.defaultValue(1800000)
			.withDescription("Back to GangScheduler after downgrade timeout.");

	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> CLEANUP_RUNNING_CONTAINERS_ON_STOP =
		key("yarn.nmclient.cleanup-running-containers-on-stop")
			.booleanType()
			.defaultValue(false)
			.withDescription("Cleanup running containers on NMClient stop.");

	public static final ConfigOption<Boolean> YARN_CONF_CLUSTER_QUEUE_NAME_ENABLE =
		key("yarn.conf.cluster_queue_name.enable")
			.booleanType()
			.defaultValue(true)
			.withDescription("Enable set cluster_queue_name to yarn conf.");

	public static final ConfigOption<Boolean> YARN_CHECK_APP_NAME_UNIQUE =
		key("yarn.check.application.name.unique")
			.booleanType()
			.defaultValue(false)
			.withDescription("Yarn check application name unique in cluster dimensions.");

	public static final ConfigOption<Boolean> YARN_CHECK_APP_NAME_UNIQUE_REGION =
		key("yarn.check.application.name.unique.region")
			.booleanType()
			.defaultValue(false)
			.withDescription("Yarn check application name unique in region dimensions.");

	public static final ConfigOption<Boolean> YARN_CHECK_APP_NAME_UNIQUE_IGNORE_CHECK_FAILURE =
		key("yarn.check.application.name.unique.ignore-check-failure")
			.booleanType()
			.defaultValue(false)
			.withDescription("Yarn check application name unique ignore when check failure at submission.");

	/**
	 * Yarn configuration key prefix.
	 */
	public static final String YARN_CONFIG_KEY_PREFIX = "flink.yarn.config.";
	public static final String CLIENT_YARN_CONFIG_KEY_PREFIX = "flink.client.yarn.config.";
	public static final String JOBMANAGER_YARN_CONFIG_KEY_PREFIX = "flink.jobmanager.yarn.config.";

	public static final ConfigOption<Boolean> SLOW_CONTAINER_ENABLED =
			key("yarn.slow-container.enabled")
					.defaultValue(false)
					.withDescription("Whether enable slow container mechanism.");

	public static final ConfigOption<Long> SLOW_CONTAINER_TIMEOUT_MS =
			key("yarn.slow-container.timeout-ms")
					.longType()
					.defaultValue(120000L)
					.withDescription("Timeout in milliseconds of determine if the container is slow.");

	public static final ConfigOption<Long> SLOW_CONTAINER_CHECK_INTERVAL_MS =
			key("yarn.slow-container.check-interval-ms")
					.longType()
					.defaultValue(10000L)
					.withDescription("Interval in milliseconds of check if the container is slow.");

	public static final ConfigOption<Double> SLOW_CONTAINERS_QUANTILE =
			key("yarn.slow-container.quantile")
					.doubleType()
					.defaultValue(0.9)
					.withDescription("The quantile of slow container timeout base threshold." +
							"Means how many containers should be started before update slow container timeout threshold.");

	public static final ConfigOption<Double> SLOW_CONTAINER_THRESHOLD_FACTOR =
			key("yarn.slow-container.threshold-factor")
					.doubleType()
					.defaultValue(1.5)
					.withDescription("Times of slow container timeout base threshold." +
							"Means containers which 1.5 times slow than the base threshold should be marked as slow containers.");

	/**
	 * The number of threads to start yarn containers in yarn resource manager.
	 */
	public static final ConfigOption<Integer> CONTAINER_LAUNCHER_NUMBER =
		key("yarn.container-launcher-thread-number")
			.intType()
			.defaultValue(10)
			.withDeprecatedKeys("yarn.container-launcher-number")
			.withDescription("The number of threads to start yarn containers in yarn resource manager.");

	/** Config for docker. */
	public static final ConfigOption<String> DOCKER_IMAGE =
		key("docker.image")
			.stringType()
			.defaultValue(null)
			.withDescription("Image of docker on yarn.");
	public static final ConfigOption<String> DOCKER_DEFAULT_IMAGE =
		key("docker.default_image")
			.stringType()
			.defaultValue("yarn_runtime_flink:latest")
			.withDescription("Default image of docker on yarn.");
	public static final ConfigOption<String> DOCKER_MOUNTS =
		key("docker.mount")
			.stringType()
			.defaultValue(null)
			.withDescription("Default image of docker on yarn.");
	public static final ConfigOption<Boolean> DOCKER_MOUNTS_DEFAULT_ENABLE =
		key("docker.mount_default.enable")
			.booleanType()
			.defaultValue(true)
			.withDescription("switch to use docker default mount.");
	public static final ConfigOption<String> DOCKER_LOG_MOUNTS =
		key("docker.log.mount")
			.stringType()
			.defaultValue(null)
			.withDescription("Default image of docker on yarn.");
	public static final ConfigOption<String> DOCKER_SERVER =
		key("docker.server")
			.stringType()
			.defaultValue("image-manager.byted.org");
	public static final ConfigOption<String> DOCKER_HUB =
		key("docker.hub")
			.stringType()
			.defaultValue("hub.byted.org");
	public static final ConfigOption<String> DOCKER_NAMESPACE =
		key("docker.namespace")
			.stringType()
			.defaultValue("yarn");
	public static final ConfigOption<String> DOCKER_REGION =
		key("docker.region")
			.stringType()
			.defaultValue("China-North-LF");
	public static final ConfigOption<String> DOCKER_AUTHORIZATION =
		key("docker.authorization")
			.stringType()
			.defaultValue("Basic Rmxpbms6Z2huZTZrcGdqM2RvMzcxNHF0djBrZWYxbnd3aHNra2Q=");
	public static final ConfigOption<String> DOCKER_VERSION_URL_TEMPLATE =
		key("docker.version_template_url")
			.stringType()
			.defaultValue("http://%s/api/v1/images/self-make/latest_tag/?psm=%s&region_list=%s");

	public static final ConfigOption<Boolean> SET_TASK_MANAGER_TOKEN =
		key("yarn.taskmanager.set_token")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether set security tokens to  TaskManager YARN container context.");

	public static final ConfigOption<RtQoSLevelEnum> YARN_RUNTIME_CONF_QOS_LEVEL =
		key("yarn.runtime-conf.qos-level")
			.enumType(RtQoSLevelEnum.class)
			.defaultValue(RtQoSLevelEnum.SHARE)
			.withDescription("The mode of yarn binding cpu core level." +
				" Default is (\"share\") mode, and also (\"share\") mode," +
				" (\"reserved\") mode, or \"any\") mode.");

	/** This class is not meant to be instantiated. */
	private YarnConfigOptions() {}

	/** @see YarnConfigOptions#CLASSPATH_INCLUDE_USER_JAR */
	public enum UserJarInclusion {
		DISABLED,
		FIRST,
		LAST,
		ORDER
	}

	/** @see YarnConfigOptions#YARN_RUNTIME_CONF_QOS_LEVEL */
	public enum RtQoSLevelEnum {
		RESERVED,
		SHARE,
		ANY;
	}
}
