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

package org.apache.flink.yarn;

/**
 * The Yarn environment variables used for settings of the containers.
 */
public class YarnConfigKeys {

	// ------------------------------------------------------------------------
	//  Environment variable names
	// ------------------------------------------------------------------------

	public static final String ENV_APP_ID = "_APP_ID";
	public static final String ENV_CLIENT_HOME_DIR = "_CLIENT_HOME_DIR";
	public static final String ENV_CLIENT_SHIP_FILES = "_CLIENT_SHIP_FILES";

	public static final String ENV_FLINK_CLASSPATH = "_FLINK_CLASSPATH";

	public static final String FLINK_DIST_JAR = "_FLINK_DIST_JAR"; // the Flink jar resource location (in HDFS).
	public static final String FLINK_YARN_FILES = "_FLINK_YARN_FILES"; // the root directory for all yarn application files

	public static final String REMOTE_KEYTAB_PATH = "_REMOTE_KEYTAB_PATH";
	public static final String LOCAL_KEYTAB_PATH = "_LOCAL_KEYTAB_PATH";
	public static final String KEYTAB_PRINCIPAL = "_KEYTAB_PRINCIPAL";
	public static final String ENV_HADOOP_USER_NAME = "HADOOP_USER_NAME";
	public static final String ENV_ZOOKEEPER_NAMESPACE = "_ZOOKEEPER_NAMESPACE";

	public static final String ENV_KRB5_PATH = "_KRB5_PATH";
	public static final String ENV_YARN_SITE_XML_PATH = "_YARN_SITE_XML_PATH";

	public static final String ENV_FLINK_YARN_JOB = "_FLINK_YARN_JOB";
	public static final String ENV_FLINK_YARN_QUEUE = "_FLINK_YARN_QUEUE";
	public static final String ENV_FLINK_YARN_CLUSTER = "_FLINK_YARN_CLUSTER";
	public static final String ENV_FLINK_YARN_DC = "_FLINK_YARN_DC"; // Used for metrics reporter (Java Flink And PyFlink)
	public static final String ENV_LOAD_SERVICE_PSM = "LOAD_SERVICE_PSM";
	public static final String ENV_PSM_PREFIX = "inf.dayu";

	public static final String LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR = ";";
	public static final String ENV_SEC_KV_AUTH = "SEC_KV_AUTH";

	public static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";
	public static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	public static final String ENV_RUNTIME_UNSET = "YARN_CONTAINER_RUNTIME_UNSET_ENV";
	public static final String ENV_IPV6_SUPPORT = "BYTED_HOST_IPV6;MY_HOST_IPV6";

	// ------------------------------------------------------------------------

	// ---------------------------- Config for docker ---------------------------
	public static final String ENV_YARN_CONTAINER_RUNTIME_TYPE_KEY = "YARN_CONTAINER_RUNTIME_TYPE";
	public static final String ENV_YARN_CONTAINER_RUNTIME_TYPE_DEFAULT = "docker";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_KEY =
		"YARN_CONTAINER_RUNTIME_DOCKER_IMAGE";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS_KEY =
		"YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS_DEFAULT =
		"/opt/tiger/ss_conf:/opt/tiger/ss_conf;/opt/tiger/ss_lib:/opt/tiger/ss_lib";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_LOG_MOUNTS_KEY =
		"ENV_DOCKER_CONTAINER_LOG_DIR_MOUNT";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_LOG_MOUNTS_DEFAULT =
		"/var/log/tiger";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_CAP_ADD_KEY =
		"ENV_DOCKER_CONTAINER_CAP_ADD";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_CAP_ADD_DEFAULT =
		"SYS_ADMIN;NET_ADMIN;SYS_PTRACE";

	public static final String DOCKER_VERSION_LATEST = "latest";
	public static final String DOCKER_NAMESPACE_KEY = "docker.namespace";
	public static final String DOCKER_HTTP_HEADER_AUTHORIZATION_KEY = "Authorization";



	/** Private constructor to prevent instantiation. */
	private YarnConfigKeys() {}

}
