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

package org.apache.flink.kubernetes.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.runtime.util.EnvironmentInformation;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's kubernetes runners.
 */
@PublicEvolving
public class KubernetesConfigOptions {

	private static final String KUBERNETES_SERVICE_ACCOUNT_KEY = "kubernetes.service-account";

	public static final ConfigOption<String> CONTEXT =
		key("kubernetes.context")
		.stringType()
		.noDefaultValue()
		.withDescription("The desired context from your Kubernetes config file used to configure the Kubernetes client " +
			"for interacting with the cluster. This could be helpful if one has multiple contexts configured and " +
			"wants to administrate different Flink clusters on different Kubernetes clusters/contexts.");

	public static final ConfigOption<ServiceExposedType> REST_SERVICE_EXPOSED_TYPE =
		key("kubernetes.rest-service.exposed.type")
		.enumType(ServiceExposedType.class)
		.defaultValue(ServiceExposedType.LoadBalancer)
		.withDescription("The type of the rest service (ClusterIP or NodePort or LoadBalancer). " +
			"When set to ClusterIP, the rest service will not be created.");

	public static final ConfigOption<String> JOB_MANAGER_SERVICE_ACCOUNT =
		key("kubernetes.jobmanager.service-account")
		.stringType()
		.noDefaultValue()
		.withDescription("Service account that is used by jobmanager within kubernetes cluster. " +
			"The job manager uses this service account when requesting taskmanager pods from the API server. " +
			"If not explicitly configured, config option '" + KUBERNETES_SERVICE_ACCOUNT_KEY + "' will be used.");

	public static final ConfigOption<String> TASK_MANAGER_SERVICE_ACCOUNT =
		key("kubernetes.taskmanager.service-account")
		.stringType()
		.noDefaultValue()
		.withDescription("Service account that is used by taskmanager within kubernetes cluster. " +
			"The task manager uses this service account when watching config maps on the API server to retrieve " +
			"leader address of jobmanager and resourcemanager. If not explicitly configured, config option '" +
			KUBERNETES_SERVICE_ACCOUNT_KEY + "' will be used.");

	public static final ConfigOption<String> KUBERNETES_SERVICE_ACCOUNT =
		key(KUBERNETES_SERVICE_ACCOUNT_KEY)
			.stringType()
			.defaultValue("default")
			.withDescription("Service account that is used by jobmanager and taskmanager within kubernetes cluster. " +
				"Notice that this can be overwritten by config options '" + JOB_MANAGER_SERVICE_ACCOUNT.key() +
				"' and '" + TASK_MANAGER_SERVICE_ACCOUNT.key() + "' for jobmanager and taskmanager respectively.");

	public static final ConfigOption<Double> JOB_MANAGER_CPU =
		key("kubernetes.jobmanager.cpu")
		.doubleType()
		.defaultValue(1.0)
		.withDescription("The number of cpu used by job manager");

	public static final ConfigOption<Double> TASK_MANAGER_CPU =
		key("kubernetes.taskmanager.cpu")
		.doubleType()
		.defaultValue(-1.0)
		.withDescription("The number of cpu used by task manager. By default, the cpu is set " +
			"to the number of slots per TaskManager");

	public static final ConfigOption<ImagePullPolicy> CONTAINER_IMAGE_PULL_POLICY =
		key("kubernetes.container.image.pull-policy")
		.enumType(ImagePullPolicy.class)
		.defaultValue(ImagePullPolicy.IfNotPresent)
		.withDescription("The Kubernetes container image pull policy (IfNotPresent or Always or Never). " +
			"The default policy is IfNotPresent to avoid putting pressure to image repository.");

	public static final ConfigOption<List<String>> CONTAINER_IMAGE_PULL_SECRETS =
		key("kubernetes.container.image.pull-secrets")
		.stringType()
		.asList()
		.noDefaultValue()
		.withDescription("A semicolon-separated list of the Kubernetes secrets used to access " +
			"private image registries.");

	public static final ConfigOption<String> KUBE_CONFIG_FILE =
		key("kubernetes.config.file")
		.stringType()
		.noDefaultValue()
		.withDescription("The kubernetes config file will be used to create the client. The default " +
				"is located at ~/.kube/config");

	public static final ConfigOption<String> NAMESPACE =
		key("kubernetes.namespace")
		.stringType()
		.defaultValue("default")
		.withDescription("The namespace that will be used for running the jobmanager and taskmanager pods.");

	public static final ConfigOption<String> CONTAINER_START_COMMAND_TEMPLATE =
		key("kubernetes.container-start-command-template")
		.stringType()
		.defaultValue("%java% %classpath% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%")
		.withDescription("Template for the kubernetes jobmanager and taskmanager container start invocation.");

	public static final ConfigOption<Map<String, String>> JOB_MANAGER_LABELS =
		key("kubernetes.jobmanager.labels")
		.mapType()
		.noDefaultValue()
		.withDescription("The labels to be set for JobManager pod. Specified as key:value pairs separated by commas. " +
			"For example, version:alphav1,deploy:test.");

	public static final ConfigOption<Map<String, String>> TASK_MANAGER_LABELS =
		key("kubernetes.taskmanager.labels")
		.mapType()
		.noDefaultValue()
		.withDescription("The labels to be set for TaskManager pods. Specified as key:value pairs separated by commas. " +
			"For example, version:alphav1,deploy:test.");

	public static final ConfigOption<Map<String, String>> JOB_MANAGER_NODE_SELECTOR =
		key("kubernetes.jobmanager.node-selector")
		.mapType()
		.noDefaultValue()
		.withDescription("The node selector to be set for JobManager pod. Specified as key:value pairs separated by " +
			"commas. For example, environment:production,disk:ssd.");

	public static final ConfigOption<Map<String, String>> TASK_MANAGER_NODE_SELECTOR =
		key("kubernetes.taskmanager.node-selector")
		.mapType()
		.noDefaultValue()
		.withDescription("The node selector to be set for TaskManager pods. Specified as key:value pairs separated by " +
			"commas. For example, environment:production,disk:ssd.");

	public static final ConfigOption<String> CLUSTER_ID =
		key("kubernetes.cluster-id")
		.stringType()
		.noDefaultValue()
		.withDescription("The cluster-id, which should be no more than 45 characters, is used for identifying " +
			"a unique Flink cluster. If not set, the client will automatically generate it with a random ID.");

	@Documentation.OverrideDefault("The default value depends on the actually running version. In general it looks like \"flink:<FLINK_VERSION>-scala_<SCALA_VERSION>\"")
	public static final ConfigOption<String> CONTAINER_IMAGE =
		key("kubernetes.container.image")
		.stringType()
		.defaultValue(getDefaultFlinkImage())
		.withDescription("Image to use for Flink containers. " +
			"The specified image must be based upon the same Apache Flink and Scala versions as used by the application. " +
			"Visit https://hub.docker.com/_/flink?tab=tags for the images provided by the Flink project.");

	public static final ConfigOption<String> CONTAINER_WORK_DIR =
		key("kubernetes.container.work.dir")
			.stringType()
			.defaultValue(PipelineOptions.FILE_MOUNTED_PATH.defaultValue())
			.withDescription("The working directory of jobmanager and taskmanager.");

	public static final ConfigOption<List<String>> KUBERNETES_JOB_MANAGER_POST_START_HANDLER_COMMANDS =
		key("kubernetes.jobmanager.post-start-handler.commands")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("A semicolon-separated list of the prepare commands to be executed before "
				+ "starting jobmanager. These commands will attach into jobmanager pod as the post-start handler");

	public static final ConfigOption<List<String>> KUBERNETES_TASK_MANAGER_POST_START_HANDLER_COMMANDS =
		key("kubernetes.taskmanager.post-start-handler.commands")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("A semicolon-separated list of the prepare commands to be executed before "
				+ "starting taskmanager. These commands will attach into taskmanager pod as the post-start handler");

	public static final ConfigOption<Integer> KUBERNETES_TASK_MANAGER_MINIMAL_NUM =
		key("kubernetes.taskmanager.minimal-number")
			.intType()
			.defaultValue(0)
			.withDescription("The number of task managers to be reserved even if they are expired. "
				+ "0 for not reserve task managers.");

	public static final ConfigOption<Boolean> KUBERNETES_WEB_SHELL_ENABLED =
		key("kubernetes.web-shell.enabled")
			.booleanType()
			.defaultValue(Boolean.FALSE)
			.withDescription("Whether to enable web shell for kubernetes.");

	public static final ConfigOption<Map<String, String>> KUBERNETES_DEPLOYMENT_ANNOTATIONS =
		key("kubernetes.deployment.annotations")
			.mapType()
			.noDefaultValue()
			.withDescription("The user-specified annotations that are set to the JobManager Deployment. "
				+ "The value could be in the form of a1:v1,a2:v2");

	public static final ConfigOption<Boolean> KUBERNETES_INGRESS_ENABLE =
		key("kubernetes.ingress.enable")
			.booleanType()
			.defaultValue(Boolean.FALSE)
			.withDescription("Whether to enable ingress in kubernetes.");

	public static final ConfigOption<Map<String, String>> KUBERNETES_INGRESS_ANNOTATIONS =
		key("kubernetes.ingress.annotations")
			.mapType()
			.noDefaultValue()
			.withDescription("The user-specified annotations that are set to the Ingress. "
				+ "The value could be in the form of a1:v1,a2:v2");

	public static final ConfigOption<String> KUBERNETES_INGRESS_HOST =
		key("kubernetes.ingress.host")
			.stringType()
			.noDefaultValue()
			.withDescription("The user-specified host that are set to the Ingress. "
				+ "e.g. lf-cloudnative.byted.org");

	public static final ConfigOption<Boolean> KUBERNETES_PODGROUP_ENABLE =
		key("kubernetes.podgroup.enable")
			.booleanType()
			.defaultValue(Boolean.FALSE)
			.withDescription("Whether to enable pod group in kubernetes. This option should only be enabled in application mode");

	/**
	 * The following config options need to be set according to the image.
	 */
	public static final ConfigOption<String> KUBERNETES_ENTRY_PATH =
		key("kubernetes.entry.path")
		.stringType()
		.defaultValue("/opt/flink/bin/kubernetes-entry.sh")
		.withDescription("The entrypoint script of kubernetes in the image. It will be used as command for jobmanager " +
			"and taskmanager container.");

	public static final ConfigOption<String> FLINK_CONF_DIR =
		key("kubernetes.flink.conf.dir")
		.stringType()
		.defaultValue("/opt/flink/conf")
		.withDescription("The flink conf directory that will be mounted in pod. The flink-conf.yaml, log4j.properties, " +
			"logback.xml in this path will be overwritten from config map.");

	public static final ConfigOption<List<String>> FLINK_MOUNTED_HOST_PATH =
		key("kubernetes.flink.mounted-host-path")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("The host path list that will be mounted by Flink task manager and job manager. The list is " +
				"separated by semicolon, each item contains three elements: volume name, path in host, path to be mounted in container. " +
				"E.g. conf-volume1,/path1/in/host,/path1/in/container;conf-volume2,/path2/in/host,/path2/in/container");

	public static final ConfigOption<String> FLINK_LOG_DIR =
		key("kubernetes.flink.log.dir")
		.stringType()
		.defaultValue("/opt/flink/log")
		.withDescription("The directory that logs of jobmanager and taskmanager be saved in the pod.");

	public static final ConfigOption<String> HADOOP_CONF_CONFIG_MAP =
		key("kubernetes.hadoop.conf.config-map.name")
		.stringType()
		.noDefaultValue()
		.withDescription("Specify the name of an existing ConfigMap that contains custom Hadoop configuration " +
			"to be mounted on the JobManager(s) and TaskManagers.");

	public static final ConfigOption<String> HADOOP_CONF_MOUNTED_HOST_PATH_VOLUME =
		key("kubernetes.hadoop.conf.mounted-host-path-volume.name")
			.stringType()
			.noDefaultValue()
			.withDescription("Specify the name of an existing host path type volume that contains custom Hadoop configuration " +
				"to be mounted on the JobManager(s) and TaskManagers.");

	public static final ConfigOption<String> KUBERNETES_SCHEDULER_NAME =
		key("kubernetes.scheduler-name")
			.stringType()
			.noDefaultValue()
			.withDescription(
				Description.builder()
					.text("The user-specified scheduler name that will be used into Flink Pod, including the JM and the TM pod.")
					.build());

	public static final ConfigOption<Map<String, String>> JOB_MANAGER_ANNOTATIONS =
		key("kubernetes.jobmanager.annotations")
		.mapType()
		.noDefaultValue()
		.withDescription("The user-specified annotations that are set to the JobManager pod. The value could be " +
			"in the form of a1:v1,a2:v2");

	public static final ConfigOption<Map<String, String>> TASK_MANAGER_ANNOTATIONS =
		key("kubernetes.taskmanager.annotations")
		.mapType()
		.noDefaultValue()
		.withDescription("The user-specified annotations that are set to the TaskManager pod. The value could be " +
			"in the form of a1:v1,a2:v2");

	public static final ConfigOption<List<Map<String, String>>> JOB_MANAGER_TOLERATIONS =
		key("kubernetes.jobmanager.tolerations")
			.mapType()
			.asList()
			.noDefaultValue()
			.withDescription("The user-specified tolerations to be set to the JobManager pod. The value should be " +
				"in the form of key:key1,operator:Equal,value:value1,effect:NoSchedule;" +
				"key:key2,operator:Exists,effect:NoExecute,tolerationSeconds:6000");

	public static final ConfigOption<List<Map<String, String>>> TASK_MANAGER_TOLERATIONS =
		key("kubernetes.taskmanager.tolerations")
			.mapType()
			.asList()
			.noDefaultValue()
			.withDescription("The user-specified tolerations to be set to the TaskManager pod. The value should be " +
				"in the form of key:key1,operator:Equal,value:value1,effect:NoSchedule;" +
				"key:key2,operator:Exists,effect:NoExecute,tolerationSeconds:6000");

	public static final ConfigOption<Map<String, String>> REST_SERVICE_ANNOTATIONS =
		key("kubernetes.rest-service.annotations")
			.mapType()
			.noDefaultValue()
			.withDescription("The user-specified annotations that are set to the rest Service. The value should be " +
				"in the form of a1:v1,a2:v2");

	/** Defines the configuration key of that external resource in Kubernetes. This is used as a suffix in an actual config. */
	public static final String EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX = "kubernetes.config-key";

	public static final ConfigOption<Map<String, String>> KUBERNETES_SECRETS =
		key("kubernetes.secrets")
			.mapType()
			.noDefaultValue()
			.withDescription(
				Description.builder()
					.text("The user-specified secrets that will be mounted into Flink container. The value should be in " +
						"the form of %s.", TextElement.code("foo:/opt/secrets-foo,bar:/opt/secrets-bar"))
					.build());

	public static final ConfigOption<List<Map<String, String>>> KUBERNETES_ENV_SECRET_KEY_REF =
		key("kubernetes.env.secretKeyRef")
			.mapType()
			.asList()
			.noDefaultValue()
			.withDescription(
				Description.builder()
					.text("The user-specified secrets to set env variables in Flink container. The value should be in " +
						"the form of %s.", TextElement.code("env:FOO_ENV,secret:foo_secret,key:foo_key;env:BAR_ENV,secret:bar_secret,key:bar_key"))
					.build());

	/**
	 * If configured, Flink will add "resources.limits.&gt;config-key&lt;" and "resources.requests.&gt;config-key&lt;" to the main
	 * container of TaskExecutor and set the value to {@link ExternalResourceOptions#EXTERNAL_RESOURCE_AMOUNT}.
	 *
	 * <p>It is intentionally included into user docs while unused.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<String> EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY =
		key(ExternalResourceOptions.genericKeyWithSuffix(EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX))
			.stringType()
			.noDefaultValue()
			.withDescription("If configured, Flink will add \"resources.limits.<config-key>\" and \"resources.requests.<config-key>\" " +
				"to the main container of TaskExecutor and set the value to the value of " + ExternalResourceOptions.EXTERNAL_RESOURCE_AMOUNT.key() + ".");

	public static final ConfigOption<Integer> KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES =
		key("kubernetes.transactional-operation.max-retries")
			.intType()
			.defaultValue(5)
			.withDescription(
				Description.builder()
					.text("Defines the number of Kubernetes transactional operation retries before the " +
					"client gives up. For example, %s.", TextElement.code("FlinkKubeClient#checkAndUpdateConfigMap"))
					.build());

	public static final ConfigOption<Integer> KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE =
		key("kubernetes.client.io-pool.size")
			.intType()
			.defaultValue(10)
			.withDescription(
				"The size of the IO executor pool used by the Kubernetes client to execute blocking IO operations "
					+ "(e.g. start/stop TaskManager pods, update leader related ConfigMaps, etc.). "
					+ "Increasing the pool size allows to run more IO operations concurrently.");

	public static final String POD_NAME_KEY = "#POD_NAME#";

	public static final ConfigOption<Boolean> STREAM_LOG_ENABLED =
			key("kubernetes.stream-log.enabled")
					.booleanType()
					.defaultValue(true)
					.withDescription("Whether enable redirect to stream log platform.");

	public static final ConfigOption<String> STREAM_LOG_URL_TEMPLATE =
		key("kubernetes.stream-log.url-template")
			.stringType()
			.defaultValue("https://%s/argos/streamlog/tenant_query?query=%s&region=%s&searchview=%s&start_time=%s&end_time=%s")
			.withDescription("Template of stream log platform." +
					"it must work with STREAM_LOG_DOMAIN/STREAM_LOG_QUERY_TEMPLATE/STREAM_LOG_SEARCH_VIEW");

	public static final ConfigOption<String> STREAM_LOG_DOMAIN =
		key("kubernetes.stream-log.domain")
			.stringType()
			.noDefaultValue()
			.withDescription("Domain of stream log platform.");

	public static final ConfigOption<String> STREAM_LOG_QUERY_TEMPLATE =
		key("kubernetes.stream-log.query-template")
			.stringType()
			.defaultValue("kubernetes_pod_name='" + POD_NAME_KEY + "'")
			.withDescription("Template of stream log query, #POD_NAME# will be replaced with real pod name.");

	public static final ConfigOption<String> STREAM_LOG_SEARCH_VIEW =
		key("kubernetes.stream-log.search-view")
			.stringType()
			.defaultValue("2::godel")
			.withDescription("Search view of stream log query.");

	public static final ConfigOption<Integer> STREAM_LOG_QUERY_RANGE_SECONDS =
		key("kubernetes.stream-log.query-range-seconds")
			.intType()
			.defaultValue(900)
			.withDescription("Time range to query.");

	public static final ConfigOption<Boolean> KUBERNETES_HOST_NETWORK_ENABLED =
		key("kubernetes.hostnetwork.enabled")
			.booleanType()
			.defaultValue(Boolean.FALSE)
			.withDescription("whether to enable using host network for clusters in Kubernetes.");

	private static String getDefaultFlinkImage() {
		// The default container image that ties to the exact needed versions of both Flink and Scala.
		boolean snapshot = EnvironmentInformation.getVersion().toLowerCase(Locale.ENGLISH).contains("snapshot");
		String tag = snapshot ? "latest" : EnvironmentInformation.getVersion() + "-scala_" + EnvironmentInformation.getScalaVersion();
		return "flink:" + tag;
	}

	/**
	 * The flink rest service exposed type.
	 */
	public enum ServiceExposedType {
		ClusterIP,
		NodePort,
		LoadBalancer
	}

	/**
	 * The container image pull policy.
	 */
	public enum ImagePullPolicy {
		IfNotPresent,
		Always,
		Never
	}

	/** This class is not meant to be instantiated. */
	private KubernetesConfigOptions() {}
}
