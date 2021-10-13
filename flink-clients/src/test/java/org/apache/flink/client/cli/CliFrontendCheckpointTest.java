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

package org.apache.flink.client.cli;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.org.apache.commons.cli.Options;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for checkpoint options.
 */
public class CliFrontendCheckpointTest extends CliFrontendTestBase {
	private GenericCLI cliUnderTest;
	private Configuration configuration;

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Before
	public void setup() throws Exception {
		Options testOptions = new Options();
		configuration = new Configuration();

		cliUnderTest = new GenericCLI(
			configuration,
			tmp.getRoot().getAbsolutePath());

		cliUnderTest.addGeneralOptions(testOptions);
	}

	@Test
	public void testCheckpointParams() throws CliArgsException {
		String[] parameters = {"-anl", "/tmp/_metadata"};
		CheckpointOptions options = new CheckpointOptions(CliFrontendParser.parse(
				CliFrontendParser.getCheckpointCommandOptions(), parameters, true));
		assertTrue(options.isAnalyzation());
	}

	@Test
	public void testClearCheckpointThrowsException() throws Exception {
		String[] args = {
			"-cn", "test",
			"-t", "remote",
			"-DclusterName=flink"
		};
		final Throwable expectedRootCause = new CliArgsException("Missing JobID and jobUID. Specify one of them.");
		try {
			verifyCliFrontendWithCheckpoint(configuration, args, cliUnderTest, "parent-first", FlinkUserCodeClassLoaders.ParentFirstClassLoader.class.getName());
		} catch (CliArgsException e) {
			final Throwable cause = ExceptionUtils.stripExecutionException(e);
			assertTrue(cause instanceof CliArgsException);
			assertEquals(expectedRootCause.getMessage(), cause.getMessage());
		}
	}

	public static void verifyCliFrontendWithCheckpoint(
		Configuration configuration,
		String[] parameters,
		GenericCLI cliUnderTest,
		String expectedResolveOrderOption,
		String userCodeClassLoaderClassName) throws Exception {
		TestingCliFrontend testFrontend =
			new TestingCliFrontend(configuration, cliUnderTest, expectedResolveOrderOption, userCodeClassLoaderClassName);
		testFrontend.checkpoint(parameters); // verifies the expected values (see below)
	}

	private static final class TestingCliFrontend extends CliFrontend {

		private final String expectedResolveOrder;

		private final String userCodeClassLoaderClassName;

		private TestingCliFrontend(
			Configuration configuration,
			GenericCLI cliUnderTest,
			String expectedResolveOrderOption,
			String userCodeClassLoaderClassName) {
			super(
				configuration,
				Collections.singletonList(cliUnderTest));
			this.expectedResolveOrder = expectedResolveOrderOption;
			this.userCodeClassLoaderClassName = userCodeClassLoaderClassName;
		}

		@Override
		protected void executeProgram(Configuration configuration, PackagedProgram program) {
			assertEquals(TEST_JAR_MAIN_CLASS, program.getMainClassName());
			assertEquals(expectedResolveOrder, configuration.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER));
			assertEquals(userCodeClassLoaderClassName, program.getUserCodeClassLoader().getClass().getName());
		}

		@Override
		public MetricRegistryImpl createMetricRegistry(Configuration config) {
			return null;
		}
	}
}
