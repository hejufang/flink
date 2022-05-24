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

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link KubernetesUtils}.
 */
public class KubernetesUtilsTest extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testParsePortRange() {
		final Configuration cfg = new Configuration();
		cfg.set(BlobServerOptions.PORT, "50100-50200");
		try {
			KubernetesUtils.parsePort(cfg, BlobServerOptions.PORT);
			fail("Should fail with an exception.");
		} catch (FlinkRuntimeException e) {
			assertThat(
				e.getMessage(),
				containsString(BlobServerOptions.PORT.key() + " should be specified to a fixed port. Do not support a range of ports."));
		}
	}

	@Test
	public void testParsePortNull() {
		final Configuration cfg = new Configuration();
		ConfigOption<String> testingPort = ConfigOptions.key("test.port").stringType().noDefaultValue();
		try {
			KubernetesUtils.parsePort(cfg, testingPort);
			fail("Should fail with an exception.");
		} catch (NullPointerException e) {
			assertThat(
				e.getMessage(),
				containsString(testingPort.key() + " should not be null."));
		}
	}

	@Test
	public void testCheckWithDynamicPort() {
		testCheckAndUpdatePortConfigOption("0", "6123", "6123");
	}

	@Test
	public void testCheckWithFixedPort() {
		testCheckAndUpdatePortConfigOption("6123", "16123", "6123");
	}

	@Test
	public void testUploadDiskFilesWithOnlyRemoteFiles() throws IOException {
		final Configuration config = new Configuration();
		File uploadDir = temporaryFolder.newFolder().getAbsoluteFile();

		config.setString(PipelineOptions.JARS.key(), "hdfs:///path/of/user.jar");
		config.set(PipelineOptions.EXTERNAL_RESOURCES,
			Arrays.asList("hdfs:///path/of/file1.jar", "hdfs:///path/file2.jar", "hdfs:///path/file3.jar"));
		KubernetesUtils.uploadLocalDiskFilesToRemote(config, new Path(uploadDir.toURI()));
		assertEquals("all remote files need to be added in external-resource list",
			4, config.get(PipelineOptions.EXTERNAL_RESOURCES).size());
	}

	@Test
	public void testUploadDiskFilesContainsDiskFile() throws IOException {
		final Configuration config = new Configuration();
		File uploadDir = temporaryFolder.newFolder().getAbsoluteFile();
		File resourceFolder = temporaryFolder.newFolder().getAbsoluteFile();
		KubernetesTestUtils.createTemporyFile("some data", resourceFolder, "user.jar");
		KubernetesTestUtils.createTemporyFile("some data", resourceFolder, "file1.jar");

		String userJar = new File(resourceFolder, "user.jar").toString();
		String file1 = new File(resourceFolder, "file1.jar").toString();
		config.setString(PipelineOptions.JARS.key(), userJar);
		config.set(PipelineOptions.EXTERNAL_RESOURCES,
			Arrays.asList(file1, "hdfs:///path/file2.jar", "hdfs:///path/file3.jar"));
		KubernetesUtils.uploadLocalDiskFilesToRemote(config, new Path(uploadDir.toURI()));
		assertEquals("all remote files need to be added in external-resource list",
			4, config.get(PipelineOptions.EXTERNAL_RESOURCES).size());
		assertFalse("disk file should be uploaded",
			config.get(PipelineOptions.EXTERNAL_RESOURCES).contains(userJar));
		assertFalse("disk file should be uploaded",
			config.get(PipelineOptions.EXTERNAL_RESOURCES).contains(file1));
	}

	@Test
	public void testUploadDiskFilesOnlyContainsUserJarInDisk() throws IOException {
		final Configuration config = new Configuration();
		File uploadDir = temporaryFolder.newFolder().getAbsoluteFile();
		File resourceFolder = temporaryFolder.newFolder().getAbsoluteFile();
		KubernetesTestUtils.createTemporyFile("some data", resourceFolder, "user.jar");
		String userJar = new File(resourceFolder, "user.jar").toString();

		config.setString(PipelineOptions.JARS.key(), userJar);
		KubernetesUtils.uploadLocalDiskFilesToRemote(config, new Path(uploadDir.toURI()));
		assertEquals("all remote files need to be added in external-resource list",
			1, config.get(PipelineOptions.EXTERNAL_RESOURCES).size());
		assertFalse("disk file should be uploaded",
			config.get(PipelineOptions.EXTERNAL_RESOURCES).contains(userJar));
	}

	@Test
	public void testUploadDiskFilesOnlyContainsUserJarInRemote() throws IOException {
		final Configuration config = new Configuration();
		File uploadDir = temporaryFolder.newFolder().getAbsoluteFile();

		config.setString(PipelineOptions.JARS.key(), "hdfs:///path/of/user.jar");
		KubernetesUtils.uploadLocalDiskFilesToRemote(config, new Path(uploadDir.toURI()));
		assertEquals("all remote files need to be added in external-resource list",
			1, config.get(PipelineOptions.EXTERNAL_RESOURCES).size());
	}

	private void testCheckAndUpdatePortConfigOption(String port, String fallbackPort, String expectedPort) {
		final Configuration cfg = new Configuration();
		cfg.setString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE, port);
		KubernetesUtils.checkAndUpdatePortConfigOption(
			cfg,
			HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE,
			Integer.valueOf(fallbackPort));
		assertEquals(expectedPort, cfg.get(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE));
	}

	@Test
	public void testLogUrl() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.set(KubernetesConfigOptions.STREAM_LOG_DOMAIN, "foo.bar");
		String streamLogUrlTemplate = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_URL_TEMPLATE);
		String streamLogQueryTemplate = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_QUERY_TEMPLATE);
		String streamLogSearchView = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_SEARCH_VIEW);
		String region = flinkConfig.getString(ConfigConstants.DC_KEY, ConfigConstants.DC_DEFAULT);
		int streamLogQueryRange = flinkConfig.getInteger(KubernetesConfigOptions.STREAM_LOG_QUERY_RANGE_SECONDS);

		String domain = "foo.bar";
		String podName = "JobManagerPod";

		String jmLog = KubernetesUtils.genLogUrl(streamLogUrlTemplate, domain, streamLogQueryRange, streamLogQueryTemplate, podName, region, streamLogSearchView);
		String jmLogWanted = "https://foo.bar/argos/streamlog/tenant_query?query=kubernetes_pod_name%3D%27JobManagerPod%27&region=cn&searchview=2%3A%3Agodel";
		assertTrue(jmLog.startsWith(jmLogWanted));
	}

	@Test
	public void testStartCommandWithoutPrePostfix(){
		Configuration flinkConfig = new Configuration();
		String startCommand = KubernetesUtils.addPrePostfixToStartCommand(flinkConfig, "echo 'hello world'");
		String expectedCommand = "echo 'hello world'";
		assertEquals(expectedCommand, startCommand);
	}

	@Test
	public void testAddPrefixForStartCommand(){
		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_PREFIX, "unset 1;unset 2");
		String startCommand = KubernetesUtils.addPrePostfixToStartCommand(flinkConfig, "echo 'hello world'");
		String expectedCommand = "unset 1;unset 2;echo 'hello world'";
		assertEquals(expectedCommand, startCommand);
	}

	@Test
	public void testAddPostfixForStartCommand(){
		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_POSTFIX, "echo done");
		String startCommand = KubernetesUtils.addPrePostfixToStartCommand(flinkConfig, "echo 'hello world'");
		String expectedCommand = "echo 'hello world';echo done";
		assertEquals(expectedCommand, startCommand);
	}

	@Test
	public void testAddBothPreAndPostfixForStartCommand(){
		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_PREFIX, "unset 1;unset 2");
		flinkConfig.setString(KubernetesConfigOptions.CONTAINER_START_COMMAND_POSTFIX, "echo done");
		String startCommand = KubernetesUtils.addPrePostfixToStartCommand(flinkConfig, "echo 'hello world'");
		String expectedCommand = "unset 1;unset 2;echo 'hello world';echo done";
		assertEquals(expectedCommand, startCommand);
	}

	@Test
	public void testGetExternalFiles() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(PipelineOptions.FILE_MOUNTED_PATH, "/opt/tiger/workdir");
		flinkConfig.set(PipelineOptions.JARS, Collections.singletonList("hdfs://job/user.jar"));
		flinkConfig.set(PipelineOptions.EXTERNAL_RESOURCES, Arrays.asList("hdfs://job/file1.jar", "hdfs://job/file2.jar"));
		List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
		String[] expectedPath = new String[] {
				"/opt/tiger/workdir/file1.jar",
				"/opt/tiger/workdir/file2.jar"
		};
		assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
	}

	@Test
	public void testGetExternalFilesWithEmptyParameter() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.set(PipelineOptions.JARS, Collections.singletonList("local:///opt/usrlib/user.jar"));
		List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
		String[] expectedPath = new String[] {};
		assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
	}

	@Test
	public void testGetExternalFilesWithRepeatedUserJar() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(PipelineOptions.FILE_MOUNTED_PATH, "/opt/tiger/workdir");
		flinkConfig.set(PipelineOptions.JARS, Collections.singletonList("hdfs://job/user.jar"));
		flinkConfig.set(PipelineOptions.EXTERNAL_RESOURCES,
				Arrays.asList("hdfs://job/file1.jar", "hdfs://job/file2.jar", "hdfs://job/user.jar"));
		List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
		String[] expectedPath = new String[] {
				"/opt/tiger/workdir/file1.jar",
				"/opt/tiger/workdir/file2.jar"
		};
		assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
	}

	@Test
	public void testGetExternalFilesWithSameNameFiles() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(PipelineOptions.FILE_MOUNTED_PATH, "/opt/tiger/workdir");
		flinkConfig.set(PipelineOptions.EXTERNAL_RESOURCES,
				Arrays.asList("hdfs://job/file1.jar", "hdfs://job/flink/file1.jar"));

		Map<String, String> pathToFileName = new HashMap<>();
		pathToFileName.put("hdfs://job/file1.jar", "file1.jar");
		pathToFileName.put("hdfs://job/flink/file1.jar", "0_file1.jar");
		flinkConfig.set(ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING, pathToFileName);
		List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
		String[] expectedPath = new String[] {
				"/opt/tiger/workdir/file1.jar",
				"/opt/tiger/workdir/0_file1.jar"
		};
		assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
	}

	@Test
	public void testGetExternalFilesWithNonJarFiles() {
		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(PipelineOptions.FILE_MOUNTED_PATH, "/opt/tiger/workdir");
		flinkConfig.set(PipelineOptions.EXTERNAL_RESOURCES,
				Arrays.asList("hdfs://job/file1.jar", "hdfs://job/file2.jar", "hdfs://job/sqlFile"));

		Map<String, String> pathToFileName = new HashMap<>();
		pathToFileName.put("hdfs://job/file1.jar", "file1.jar");
		flinkConfig.set(ApplicationConfiguration.EXTERNAL_RESOURCES_NAME_MAPPING, pathToFileName);
		List<URL> urls = KubernetesUtils.getExternalFiles(flinkConfig);
		String[] expectedPath = new String[] {
				"/opt/tiger/workdir/file1.jar",
				"/opt/tiger/workdir/file2.jar"
		};
		assertArrayEquals(expectedPath, urls.stream().map(URL::getPath).toArray());
	}
}
