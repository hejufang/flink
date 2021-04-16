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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Utils}.
 */
public class UtilsTest extends TestLogger {

	private static final String DEFAULT_IMAGE_PREFIX = "hub.byted.org/yarn/yarn_runtime_flink";

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testDeleteApplicationFiles() throws Exception {
		final Path applicationFilesDir = temporaryFolder.newFolder(".flink").toPath();
		Files.createFile(applicationFilesDir.resolve("flink.jar"));
		try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
			assertThat(files.count(), equalTo(1L));
		}
		try (Stream<Path> files = Files.list(applicationFilesDir)) {
			assertThat(files.count(), equalTo(1L));
		}

		Utils.deleteApplicationFiles(Collections.singletonMap(
			YarnConfigKeys.FLINK_YARN_FILES,
			applicationFilesDir.toString()));
		try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
			assertThat(files.count(), equalTo(0L));
		}
	}

	/**
	 * Test for get docker image.
	 *
	 * @throws IOException
	 */
	@Test
	public void getDockerImage() throws IOException {
		Configuration flinkConf = new Configuration();
		flinkConf.setString(YarnConfigOptions.DOCKER_NAMESPACE, "yarn");
		String imageName = Utils.getDockerImage("default", flinkConf);
		Assert.assertTrue(imageName.startsWith(DEFAULT_IMAGE_PREFIX));
	}

	/**
	 * Set docker env.
	 */
	@Test
	public void setDockerEnv() {
		Configuration flinkConf = new Configuration();
		flinkConf.setString(YarnConfigOptions.DOCKER_NAMESPACE, "yarn");
		flinkConf.setString(YarnConfigOptions.DOCKER_IMAGE, "default");
		Map<String, String> envMap = new HashMap<>();
		Utils.setDockerEnv(flinkConf, envMap);
		envMap.get(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_KEY);
		envMap.get(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_TYPE_KEY);
		assertTrue(envMap.get(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_KEY).startsWith(DEFAULT_IMAGE_PREFIX));
		assertEquals(envMap.get(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_CAP_ADD_KEY), YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_CAP_ADD_DEFAULT);
		assertEquals(envMap.get(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS_KEY), YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS_DEFAULT);
		assertEquals(envMap.get(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_TYPE_KEY), YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_TYPE_DEFAULT);
		assertEquals(envMap.get(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_LOG_MOUNTS_KEY), "/var/log/tiger");

	}
}
