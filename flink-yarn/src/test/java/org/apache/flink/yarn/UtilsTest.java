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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link Utils}.
 */
public class UtilsTest extends TestLogger {

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

	@Test
	public void testGetDockerImageWithNamespace() throws IOException {
		Configuration flinkConfig = new Configuration();
		String imageId = "docker_test:123321";
		String hub = "hub.xx.org";
		String namespace = "xx";
		flinkConfig.setString(YarnConfigKeys.DOCKER_IMAGE_KEY, imageId);
		flinkConfig.setString(YarnConfigKeys.DOCKER_HUB_KEY, hub);
		flinkConfig.setString(YarnConfigKeys.DOCKER_NAMESPACE_KEY, namespace);

		String image = Utils.getDockerImage(imageId, flinkConfig);
		assertEquals(hub + "/" + namespace + "/" + imageId, image);
	}
}
