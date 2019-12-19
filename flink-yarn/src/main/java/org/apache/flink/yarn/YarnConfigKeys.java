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

	public static final String ENV_TM_MEMORY = "_CLIENT_TM_MEMORY";
	public static final String ENV_TM_COUNT = "_CLIENT_TM_COUNT";
	public static final String ENV_APP_ID = "_APP_ID";
	public static final String ENV_CLIENT_HOME_DIR = "_CLIENT_HOME_DIR";
	public static final String ENV_CLIENT_SHIP_FILES = "_CLIENT_SHIP_FILES";
	public static final String ENV_SLOTS = "_SLOTS";
	public static final String ENV_DETACHED = "_DETACHED";
	public static final String ENV_DYNAMIC_PROPERTIES = "_DYNAMIC_PROPERTIES";

	public static final String ENV_FLINK_CLASSPATH = "_FLINK_CLASSPATH";

	public static final String FLINK_JAR_PATH = "_FLINK_JAR_PATH"; // the Flink jar resource location (in HDFS).
	public static final String FLINK_YARN_FILES = "_FLINK_YARN_FILES"; // the root directory for all yarn application files

	public static final String KEYTAB_PATH = "_KEYTAB_PATH";
	public static final String KEYTAB_PRINCIPAL = "_KEYTAB_PRINCIPAL";
	public static final String ENV_HADOOP_USER_NAME = "HADOOP_USER_NAME";
	public static final String ENV_ZOOKEEPER_NAMESPACE = "_ZOOKEEPER_NAMESPACE";

	public static final String ENV_KRB5_PATH = "_KRB5_PATH";
	public static final String ENV_YARN_SITE_XML_PATH = "_YARN_SITE_XML_PATH";

	public static final String ENV_FLINK_YARN_JOB = "_FLINK_YARN_JOB";
	public static final String ENV_FLINK_YARN_QUEUE = "_FLINK_YARN_QUEUE";
	public static final String ENV_FLINK_YARN_DC = "_FLINK_YARN_DC"; // Used for metrics reporter (Java Flink And PyFlink)
	public static final String ENV_LOAD_SERVICE_PSM = "LOAD_SERVICE_PSM";
	public static final String ENV_PSM_PREFIX = "inf.dayu";

	public static final String ENV_LD_LIBRARY_PATH = "LD_LIBRARY_PATH";

	/** Default value of LD_LIBRARY_PATH. */
	public static final String LD_LIBRARY_PATH_DEFAULT = "/opt/tiger/ss_lib/so:" +
		"/opt/tiger/yarn_deploy/hadoop-2.6.0-cdh5.4.4/lib/native:/usr/local/hadoop/lzo/lib";

	public static final String SPT_NOENV = "ANY_VALUE_BUT_EMPTY";

	// ---------------------------- Config for docker ---------------------------
	public static final String IS_IN_DOCKER_MODE_KEY = "isInDockerMode";
	public static final String ENV_YARN_CONTAINER_RUNTIME_TYPE_KEY = "YARN_CONTAINER_RUNTIME_TYPE";
	public static final String ENV_YARN_CONTAINER_RUNTIME_TYPE_DEFAULT = "docker";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_KEY =
		"YARN_CONTAINER_RUNTIME_DOCKER_IMAGE";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS_KEY =
		"YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS";
	public static final String ENV_YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS_DEFAULT =
		"/opt/tiger/ss_conf:/opt/tiger/ss_conf;/opt/tiger/ss_lib:/opt/tiger/ss_lib";

	public static final String DOCKER_IMAGE_KEY = "docker.image";
	public static final String DOCKER_DEFAULT_IMAGE_KEY = "docker.default_image";
	public static final String DOCKER_IMAGE_DEFAULT = "yarn_runtime_flink:latest";
	public static final String DOCKER_MOUNTS_KEY = "docker.mount";
	public static final String DOCKER_VERSION_LATEST = "latest";
	public static final String DOCKER_SERVER_KEY = "docker.server";
	public static final String DOCKER_SERVER_DEFAULT = "image-manager.byted.org";
	public static final String DOCKER_HUB_KEY = "docker.hub";
	public static final String DOCKER_HUB_DEFAULT = "hub.byted.org";
	public static final String DOCKER_NAMESPACE_KEY = "docker.namespace";
	public static final String DOCKER_NAMESPACE_DEFAULT = "yarn";
	public static final String DOCKER_REGION_KEY = "docker.region";
	public static final String DOCKER_REGION_DEFAULT = "China-North-LF";
	public static final String DOCKER_AUTHORIZATION_KEY = "docker.authorization";
	public static final String DOCKER_AUTHORIZATION_DEFAULT = "Basic Rmxpbms6Z2huZTZrcGdqM2RvMzcxNHF0djBrZWYxbnd3aHNra2Q=";
	public static final String DOCKER_VERSION_URL_TEMPLATE_KEY = "docker.version_template_url";
	public static final String DOCKER_VERSION_URL_TEMPLATE_DEFAULT =
		"http://%s/api/v1/images/self-make/latest_tag/?psm=%s&region_list=%s";
	public static final String DOCKER_HTTP_HEADER_AUTHORIZATION_KEY = "Authorization";
	public static final String ENV_SPT_NOENV = "SPT_NOENV";
	public static final String ENV_CORE_DUMP_PROC_NAME = "CORE_DUMP_PROC_NAME";

	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation. */
	private YarnConfigKeys() {}

}
