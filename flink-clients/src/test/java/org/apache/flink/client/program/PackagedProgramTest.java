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

package org.apache.flink.client.program;

import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;

/**
 * Tests for the {@link PackagedProgram}.
 */
public class PackagedProgramTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testExtractContainedLibraries() throws Exception {
		String s = "testExtractContainedLibraries";
		byte[] nestedJarContent = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
		File fakeJar = temporaryFolder.newFile("test.jar");
		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(fakeJar))) {
			ZipEntry entry = new ZipEntry("lib/internalTest.jar");
			zos.putNextEntry(entry);
			zos.write(nestedJarContent);
			zos.closeEntry();
		}

		final List<File> files = PackagedProgram.extractContainedLibraries(fakeJar.toURI().toURL());
		Assert.assertEquals(1, files.size());
		Assert.assertArrayEquals(nestedJarContent, Files.readAllBytes(files.iterator().next().toPath()));
	}

	@Test
	public void testNotThrowExceptionWhenJarFileIsNull() throws Exception {
		PackagedProgram.newBuilder()
			.setUserClassPaths(Collections.singletonList(new File(CliFrontendTestUtils.getTestJarPath()).toURI().toURL()))
			.setEntryPointClassName(TEST_JAR_MAIN_CLASS);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBuilderThrowExceptionIfjarFileAndEntryPointClassNameAreBothNull() throws ProgramInvocationException {
		PackagedProgram.newBuilder().build();
	}

	@Test
	public void testGetJobJarAndDependencies() throws Exception {
		final String testJarPath = getTestJarPath();
		final String externalJars = "file:///testFile.jar";
		final File jarFile = new File(testJarPath);
		final List<URL> expected = Arrays.asList(jarFile.toURI().toURL(), new URI(externalJars).toURL());

		final Configuration configuration = new Configuration();
		configuration.set(PipelineOptions.JARS, Collections.singletonList(externalJars));

		PackagedProgram build = PackagedProgram.newBuilder()
			.setJarFile(jarFile)
			.setConfiguration(configuration)
			.build();
		final List<URL> jobJarAndDependencies = build.getJobJarAndDependencies();

		Assert.assertEquals(expected, jobJarAndDependencies);
	}
}
