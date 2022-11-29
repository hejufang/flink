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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.HttpUtil;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class that provides helper methods to work with Docker.
 */
public class DockerUtils {

	private static final Logger LOG = LoggerFactory.getLogger(DockerUtils.class);

	/**
	 * Default docker image used for docker on yarn.
	 */
	private static final String DEFAULT_IMAGE = "default";

	/**
	 * Url path separator.
	 */
	private static final String URL_SEP = "/";

	/**
	 * Add docker hub before image and replace 'latest' docker version with the real version.
	 *
	 * @param flinkConfiguration flink configuration.
	 * @return the real docker version.
	 */
	public static String getDockerImage(String dockerImage, Configuration flinkConfiguration) throws IOException {
		if (StringUtils.isNullOrWhitespaceOnly(dockerImage)) {
			return null;
		}
		if (DEFAULT_IMAGE.equalsIgnoreCase(dockerImage)) {
			dockerImage = flinkConfiguration.getString(DockerConfigOptions.DOCKER_DEFAULT_IMAGE);
		}
		String[] items = dockerImage.trim().split(":");
		if (items.length != 2) {
			throw new RuntimeException("docker.image must be {psm}:{version}.");
		}
		String dockerPsm = items[0].trim();
		String dockerVersion = items[1].trim();
		if (!DockerConfigKeys.DOCKER_VERSION_LATEST.equalsIgnoreCase(dockerVersion)) {
			// this image contains a hash tag, check if it starts with a hub name
			if (dockerImage.contains(URL_SEP)) {
				return dockerImage;
			} else {
				String dockerHub = flinkConfiguration.getString(DockerConfigOptions.DOCKER_HUB);
				if (flinkConfiguration.containsKey(DockerConfigKeys.DOCKER_NAMESPACE_KEY)) {
					String dockerNamespace = flinkConfiguration.getString(DockerConfigOptions.DOCKER_NAMESPACE);
					dockerImage = dockerHub + URL_SEP + dockerNamespace + URL_SEP + dockerImage;
				} else {
					dockerImage = dockerHub + URL_SEP + dockerImage;
				}
				return dockerImage;
			}
		}
		LOG.info("Replace 'latest' version with real latest version id.");
		// don't need the hub when querying the real image version.
		String dockerPsmWithoutHub = dockerPsm;
		int psmIndex = dockerPsmWithoutHub.lastIndexOf(URL_SEP);
		if (psmIndex != -1) {
			dockerPsmWithoutHub = dockerPsm.substring(psmIndex + 1);
		}
		String dockerServer = flinkConfiguration.getString(DockerConfigOptions.DOCKER_SERVER);
		String dockerRegion = flinkConfiguration.getString(DockerConfigOptions.DOCKER_REGION);
		String dockerAuthorization = flinkConfiguration.getString(DockerConfigOptions.DOCKER_AUTHORIZATION);
		String dockerUrlTemplate = flinkConfiguration.getString(DockerConfigOptions.DOCKER_VERSION_URL_TEMPLATE);
		String dockerUrl = String.format(dockerUrlTemplate, dockerServer, dockerPsmWithoutHub, dockerRegion);
		Map<String, String> headers = new HashMap<>();
		headers.put(DockerConfigKeys.DOCKER_HTTP_HEADER_AUTHORIZATION_KEY, dockerAuthorization);
		HttpUtil.HttpResponsePojo response = HttpUtil.sendGet(dockerUrl, headers);
		String content = response.getContent();
		JsonNode respJson = new ObjectMapper().readTree(content);
		String image = respJson.hasNonNull(dockerRegion) ? respJson.get(dockerRegion).asText() : null;
		LOG.info("Get image from {}, image is: {}", dockerUrl, image);
		return image;
	}
}
