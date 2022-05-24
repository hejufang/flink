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

package org.apache.flink.kubernetes.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Constants for kubernetes.
 */
public class Constants {

	// Kubernetes api version
	public static final String API_VERSION = "v1";
	public static final String APPS_API_VERSION = "apps/v1";
	public static final String INGRESS_API_VERSION = "networking.k8s.io/v1beta1";

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";
	public static final String CONFIG_FILE_LOG4J_CLIENT_NAME = "log4j-cli.properties";

	public static final String FLINK_CONF_VOLUME = "flink-config-volume";
	public static final String CONFIG_MAP_PREFIX = "flink-config-";

	public static final String HADOOP_CONF_VOLUME = "hadoop-config-volume";
	public static final String HADOOP_CONF_CONFIG_MAP_PREFIX = "hadoop-config-";
	public static final String HADOOP_CONF_DIR_IN_POD = "/opt/hadoop/conf";
	public static final String ENV_HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
	public static final String ENV_HADOOP_HOME = "HADOOP_HOME";

	public static final String FLINK_REST_SERVICE_SUFFIX = "-rest";
	public static final String FLINK_INGRESS_SUFFIX = "-ingress";

	public static final String NAME_SEPARATOR = "-";

	// Constants for label builder
	public static final String LABEL_TYPE_KEY = "type";
	public static final String LABEL_TYPE_NATIVE_TYPE = "flink-native-kubernetes";
	public static final String LABEL_APP_KEY = "app";
	public static final String LABEL_COMPONENT_KEY = "component";
	public static final String LABEL_COMPONENT_JOB_MANAGER = "jobmanager";
	public static final String LABEL_COMPONENT_TASK_MANAGER = "taskmanager";
	public static final String LABEL_CONFIGMAP_TYPE_KEY = "configmap-type";
	public static final String LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY = "high-availability";

	// Use fixed port in kubernetes, it needs to be exposed.
	public static final int REST_PORT = 8081;
	public static final int SOCKET_PORT = 8091;
	public static final int BLOB_SERVER_PORT = 6124;
	public static final int TASK_MANAGER_RPC_PORT = 6122;

	public static final String JOB_MANAGER_RPC_PORT_NAME = "jobmanager-rpc";
	public static final String BLOB_SERVER_PORT_NAME = "blobserver";
	public static final String REST_PORT_NAME = "rest";
	public static final String SOCKET_PORT_NAME = "socket";
	public static final String TASK_MANAGER_RPC_PORT_NAME = "taskmanager-rpc";
	public static final String TASK_MANAGER_NETTY_SERVER_NAME = "netty-server";
	public static final String FLINK_METRICS_PORT_NAME = "flink-metrics";

	public static final String DNS_POLICY_DEFAULT = "ClusterFirst";
	public static final String DNS_POLICY_HOSTNETWORK = "ClusterFirstWithHostNet";

	public static final String RESOURCE_NAME_MEMORY = "memory";

	public static final String RESOURCE_NAME_CPU = "cpu";

	public static final String RESOURCE_UNIT_MB = "Mi";

	public static final String ENV_FLINK_CLASSPATH = "FLINK_CLASSPATH";

	public static final String ENV_FLINK_POD_NAME = "_FLINK_POD_NAME";

	public static final String ENV_FLINK_POD_IP_ADDRESS = "_POD_IP_ADDRESS";

	public static final String POD_IP_FIELD_PATH = "status.podIP";

	public static final String ENV_FLINK_COMPONENT = "_FLINK_COMPONENT";

	public static final String POD_NAME_FIELD_PATH = "metadata.name";

	public static final String HEADLESS_SERVICE_CLUSTER_IP = "None";

	public static final int MAXIMUM_CHARACTERS_OF_CLUSTER_ID = 45;

	public static final String RESTART_POLICY_OF_NEVER = "Never";

	public static final String ENV_POD_HOST_IP = "MY_HOST_IP";

	public static final String ENV_POD_NAME = "MY_POD_NAME";

	public static final String JVM_HS_ERROR_PATH = "/var/log/tiger/";

	public static final String NATIVE_KUBERNETES_COMMAND = "native-k8s";

	// Constants for Kubernetes high availability
	public static final String LEADER_ADDRESS_KEY = "address";
	public static final String LEADER_SESSION_ID_KEY = "sessionId";
	public static final String JOB_GRAPH_STORE_KEY_PREFIX = "jobGraph-";
	public static final String SUBMITTED_JOBGRAPH_FILE_PREFIX = "submittedJobGraph";
	public static final String RUNNING_JOBS_REGISTRY_KEY_PREFIX = "runningJobsRegistry-";
	public static final String CHECKPOINT_COUNTER_KEY = "counter";
	public static final String CHECKPOINT_ID_KEY_PREFIX = "checkpointID-";
	public static final String COMPLETED_CHECKPOINT_FILE_SUFFIX = "completedCheckpoint";

	public static final String FILE_DOWNLOAD_VOLUME = "file-download-volume";

	public static final String WEB_SHELL_TEMPLATE = "https://webshellauth.byted.org/webshell/auth?zoneName=%s&idc=%s&clusterName=%s&podName=%s&namespace=%s";
	public static final String ZONE_ENV_KEY = "TCE_ZONE";
	public static final String INTERNAL_IDC_ENV_KEY = "TCE_INTERNAL_IDC";
	public static final String PHYSICAL_CLUSTER_ENV_KEY = "TCE_PHYSICAL_CLUSTER";

	// This is the default hostPath type for a hostPath volume.
	// Some example values of the type: DirectoryOrCreate, FileOrCreate, File...
	// Empty string (default) is for backward compatibility, which means that no checks will be performed before
	// mounting the hostPath volume. We set empty string as default because we do not know which type that user will mount.
	public static final String DEFAULT_HOST_PATH_TYPE = "";
	public static final String POD_GROUP_MINMEMBER_ANNOTATION_KEY = "godel.bytedance.com/pod-group-minmember";
	public static final String KUBERNETES_ANNOTATION_QUEUE_KEY = "godel.bytedance.com/queue-name";

	public static final String PORT0_ENV = "PORT0";
	public static final String PORT1_ENV = "PORT1";
	public static final String PORT2_ENV = "PORT2";
	public static final String PORT3_ENV = "PORT3";
	public static final String PORT4_ENV = "PORT4";
	// We use these two lists to ensure the order of container port definition for jobmanager and taskmanager
	public static final List<String> JOB_MANAGER_CONTAINER_PORT_LIST = Arrays.asList(
		Constants.FLINK_METRICS_PORT_NAME,
		Constants.REST_PORT_NAME,
		Constants.JOB_MANAGER_RPC_PORT_NAME,
		Constants.BLOB_SERVER_PORT_NAME,
		Constants.SOCKET_PORT_NAME
	);
	public static final List<String> TASK_MANAGER_CONTAINER_PORT_LIST = Arrays.asList(
		Constants.FLINK_METRICS_PORT_NAME,
		Constants.TASK_MANAGER_RPC_PORT_NAME,
		Constants.TASK_MANAGER_NETTY_SERVER_NAME
	);
	// We use this map to know the generated environment variables of the allocated ports in container.
	// This map is shared by jobmananger and taskmanagers. The common container port should be defined
	// first (for example, flink-metrics) and then followed by other ports.
	static final Map<String, String> FLINK_PORT_NAME_TO_ENV_NAME = new HashMap<>();

	static {
		// common ports in job manager and task manager
		FLINK_PORT_NAME_TO_ENV_NAME.put(Constants.FLINK_METRICS_PORT_NAME, PORT0_ENV);
		// job manager specific ports
		FLINK_PORT_NAME_TO_ENV_NAME.put(Constants.REST_PORT_NAME, PORT1_ENV);
		FLINK_PORT_NAME_TO_ENV_NAME.put(Constants.JOB_MANAGER_RPC_PORT_NAME, PORT2_ENV);
		FLINK_PORT_NAME_TO_ENV_NAME.put(Constants.BLOB_SERVER_PORT_NAME, PORT3_ENV);
		FLINK_PORT_NAME_TO_ENV_NAME.put(Constants.SOCKET_PORT_NAME, PORT4_ENV);
		// task manager specific ports
		FLINK_PORT_NAME_TO_ENV_NAME.put(Constants.TASK_MANAGER_RPC_PORT_NAME, PORT1_ENV);
		FLINK_PORT_NAME_TO_ENV_NAME.put(Constants.TASK_MANAGER_NETTY_SERVER_NAME, PORT2_ENV);
	}

}
