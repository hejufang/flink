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

package org.apache.flink.runtime.util.docker;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's YARN & KUBERNETES runners.
 */
public class DockerConfigOptions {
	/** Config for docker. */
	public static final ConfigOption<Boolean> DOCKER_ENABLED =
		key("docker.enabled")
			.booleanType()
			.defaultValue(true)
			.withDescription("Whether enable docker image.");

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
			.noDefaultValue();

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
			.noDefaultValue();

	public static final ConfigOption<String> DOCKER_AUTHORIZATION =
		key("docker.authorization")
			.stringType()
			.defaultValue("Basic Rmxpbms6Z2huZTZrcGdqM2RvMzcxNHF0djBrZWYxbnd3aHNra2Q=");

	public static final ConfigOption<String> DOCKER_VERSION_URL_TEMPLATE =
		key("docker.version_template_url")
			.stringType()
			.defaultValue("http://%s/api/v1/images/self-make/latest_tag/?psm=%s&region_list=%s");
}
