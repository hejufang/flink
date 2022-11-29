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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test for DockerUtils.
 */
public class DockerUtilsTest {

	public static final String DEFAULT_IMAGE_PREFIX = "hub.byted.org/yarn/yarn_runtime_flink";

	/**
	 * Test for get docker image.
	 */
	@Test
	public void testGetDefaultDockerImage() throws IOException {
		Configuration flinkConf = getDockerFlinkConf();
		String imageName = DockerUtils.getDockerImage("default", flinkConf);
		Assert.assertNotNull(imageName);
		Assert.assertTrue(imageName.startsWith(DEFAULT_IMAGE_PREFIX));
		Assert.assertFalse(imageName.endsWith("latest"));
	}

	@Test
	public void testGetDockerImageWithoutHubAndWithHashTag() throws IOException {
		Configuration flinkConf = getDockerFlinkConf();
		String imageName = DockerUtils.getDockerImage("yarn_runtime_flink:hash", flinkConf);
		assertEquals(DEFAULT_IMAGE_PREFIX + ":hash", imageName);
	}

	@Test
	public void testGetDockerImageWithHubAndHashTag() throws IOException {
		Configuration flinkConf = getDockerFlinkConf();
		String imageName = DockerUtils.getDockerImage(DEFAULT_IMAGE_PREFIX + ":hash", flinkConf);
		assertEquals(DEFAULT_IMAGE_PREFIX + ":hash", imageName);
	}

	@Test
	public void testGetDockerImageWithHubAndLatestTag() throws IOException {
		Configuration flinkConf = getDockerFlinkConf();
		String imageName = DockerUtils.getDockerImage(DEFAULT_IMAGE_PREFIX + ":latest", flinkConf);
		Assert.assertNotNull(imageName);
		Assert.assertTrue(imageName.startsWith(DEFAULT_IMAGE_PREFIX));
		Assert.assertFalse(imageName.endsWith("latest"));
	}

	@Test
	public void testGetDockerImageWithHubAndHashTagAndDifferentNamespace() throws IOException {
		Configuration flinkConf = getDockerFlinkConf();
		flinkConf.setString(DockerConfigKeys.DOCKER_NAMESPACE_KEY, "base");
		String imageName = DockerUtils.getDockerImage(DEFAULT_IMAGE_PREFIX + ":hash", flinkConf);
		assertEquals(DEFAULT_IMAGE_PREFIX + ":hash", imageName);
	}

	/**
	 * Helper method to set necessary configurations in flink conf.
	 */
	public static Configuration getDockerFlinkConf() {
		Configuration flinkConf = new Configuration();
		flinkConf.setString(DockerConfigOptions.DOCKER_NAMESPACE, "yarn");
		flinkConf.setString(DockerConfigOptions.DOCKER_IMAGE, "default");
		flinkConf.setString(DockerConfigOptions.DOCKER_SERVER, "image-manager.byted.org");
		flinkConf.setString(DockerConfigOptions.DOCKER_REGION, "China-North-LF");
		return flinkConf;
	}
}
