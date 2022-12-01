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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.util.ChildFirstClassLoader;
import org.apache.flink.util.FlinkUserCodeClassLoaders.ParentFirstClassLoader;

import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/** Tests for the RUN command with Dynamic Properties. */
class CliFrontendDynamicPropertiesTest {

    private GenericCLI cliUnderTest;
    private Configuration configuration;

    @BeforeAll
    static void init() {
        CliFrontendTestUtils.pipeSystemOutToNull();
    }

    @AfterAll
    static void shutdown() {
        CliFrontendTestUtils.restoreSystemOut();
    }

    @BeforeEach
    void setup(@TempDir java.nio.file.Path tmp) {
        Options testOptions = new Options();
        configuration = new Configuration();
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        cliUnderTest = new GenericCLI(configuration, tmp.toAbsolutePath().toString());

        cliUnderTest.addGeneralOptions(testOptions);
    }

    @Test
    public void testExternalResources() throws Exception {
        final String testJarPath = getTestJarPath();
        final String externalJars = getTestJarPath() + "_test";
        final String[] args = {
            "-t",
            "remote",
            "-D" + PipelineOptions.EXTERNAL_RESOURCES.key() + "=file:" + externalJars,
            testJarPath
        };

        final List<String> expectedJars =
                Arrays.asList("file:" + testJarPath, "file:" + externalJars);
        final List<String> expectedResources = Collections.singletonList("file:" + externalJars);

        final Configuration configuration =
                verifyCliFrontend(
                        this.configuration,
                        args,
                        cliUnderTest,
                        "child-first",
                        ChildFirstClassLoader.class.getName());

        Assert.assertEquals(expectedJars, configuration.get(PipelineOptions.JARS));
        Assert.assertEquals(
                expectedResources, configuration.get(PipelineOptions.EXTERNAL_RESOURCES));
    }

    @Test
    void testDynamicPropertiesWithParentFirstClassloader() throws Exception {

        String[] args = {
            "-e",
            "test-executor",
            "-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
            "-D" + "classloader.resolve-order=parent-first",
            getTestJarPath(),
            "-a",
            "--debug",
            "true",
            "arg1",
            "arg2"
        };

        Map<String, String> expectedConfigValues = new HashMap<>();
        expectedConfigValues.put("parallelism.default", "5");
        expectedConfigValues.put("classloader.resolve-order", "parent-first");
        verifyCliFrontendWithDynamicProperties(
                configuration,
                args,
                cliUnderTest,
                expectedConfigValues,
                (configuration, program) ->
                        assertThat(ParentFirstClassLoader.class.getName())
                                .isEqualTo(program.getUserCodeClassLoader().getClass().getName()));
    }

    @Test
    void testDynamicPropertiesWithDefaultChildFirstClassloader() throws Exception {

        String[] args = {
            "-e",
            "test-executor",
            "-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
            getTestJarPath(),
            "-a",
            "--debug",
            "true",
            "arg1",
            "arg2"
        };

        Map<String, String> expectedConfigValues = new HashMap<>();
        expectedConfigValues.put("parallelism.default", "5");
        verifyCliFrontendWithDynamicProperties(
                configuration,
                args,
                cliUnderTest,
                expectedConfigValues,
                (configuration, program) ->
                        assertThat(ChildFirstClassLoader.class.getName())
                                .isEqualTo(program.getUserCodeClassLoader().getClass().getName()));
    }

    @Test
    void testDynamicPropertiesWithChildFirstClassloader() throws Exception {

        String[] args = {
            "-e",
            "test-executor",
            "-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
            "-D" + "classloader.resolve-order=child-first",
            getTestJarPath(),
            "-a",
            "--debug",
            "true",
            "arg1",
            "arg2"
        };

        Map<String, String> expectedConfigValues = new HashMap<>();
        expectedConfigValues.put("parallelism.default", "5");
        expectedConfigValues.put("classloader.resolve-order", "child-first");
        verifyCliFrontendWithDynamicProperties(
                configuration,
                args,
                cliUnderTest,
                expectedConfigValues,
                (configuration, program) ->
                        assertThat(ChildFirstClassLoader.class.getName())
                                .isEqualTo(program.getUserCodeClassLoader().getClass().getName()));
    }

    @Test
    public void testDynamicPropertiesWithClientTimeoutAndDefaultParallelism() throws Exception {

        String[] args = {
            "-e",
            "test-executor",
            "-Dclient.timeout=10min",
            "-Dparallelism.default=12",
            getTestJarPath(),
        };
        Map<String, String> expectedConfigValues = new HashMap<>();
        expectedConfigValues.put("client.timeout", "10min");
        expectedConfigValues.put("parallelism.default", "12");
        verifyCliFrontendWithDynamicProperties(
                configuration, args, cliUnderTest, expectedConfigValues);
    }

    // --------------------------------------------------------------------------------------------
    /** @return configuration after execution. */
    public static Configuration verifyCliFrontend(
            Configuration configuration,
            String[] parameters,
            GenericCLI cliUnderTest,
            String expectedResolveOrderOption,
            String userCodeClassLoaderClassName)
            throws Exception {
        TestingCliFrontend testFrontend =
                new TestingCliFrontend(
                        configuration,
                        cliUnderTest,
                        expectedResolveOrderOption,
                        userCodeClassLoaderClassName);
        testFrontend.run(parameters); // verifies the expected values (see below)
        return testFrontend.getConfigurationAfterExecution();
    }

    public static void verifyCliFrontendWithDynamicProperties(
            Configuration configuration,
            String[] parameters,
            GenericCLI cliUnderTest,
            Map<String, String> expectedConfigValues)
            throws Exception {
        verifyCliFrontendWithDynamicProperties(
                configuration, parameters, cliUnderTest, expectedConfigValues, null);
    }

    public static void verifyCliFrontendWithDynamicProperties(
            Configuration configuration,
            String[] parameters,
            GenericCLI cliUnderTest,
            Map<String, String> expectedConfigValues,
            TestingCliFrontendWithDynamicProperties.CustomTester customTester)
            throws Exception {
        TestingCliFrontendWithDynamicProperties testFrontend =
                new TestingCliFrontendWithDynamicProperties(
                        configuration, cliUnderTest, expectedConfigValues, customTester);
        testFrontend.run(parameters); // verifies the expected values (see below)
    }

    private static final class TestingCliFrontendWithDynamicProperties extends CliFrontend {
        private final Map<String, String> expectedConfigValues;

        private final CustomTester tester;

        private TestingCliFrontendWithDynamicProperties(
                Configuration configuration,
                GenericCLI cli,
                Map<String, String> expectedConfigValues,
                CustomTester customTester) {
            super(configuration, Collections.singletonList(cli));
            this.expectedConfigValues = expectedConfigValues;
            this.tester = customTester;
        }

        @FunctionalInterface
        private interface CustomTester {
            void test(Configuration configuration, PackagedProgram program);
        }

        @Override
        protected void executeProgram(Configuration configuration, PackagedProgram program) {
            expectedConfigValues.forEach(
                    (key, value) -> assertThat(configuration.toMap()).containsEntry(key, value));
            if (tester != null) {
                tester.test(configuration, program);
            }
        }
    }

    private static final class TestingCliFrontend extends CliFrontend {

        private final String expectedResolveOrder;

        private final String userCodeClassLoaderClassName;

        private Configuration configurationAfterExecution;

        private TestingCliFrontend(
                Configuration configuration,
                GenericCLI cliUnderTest,
                String expectedResolveOrderOption,
                String userCodeClassLoaderClassName) {
            super(configuration, Collections.singletonList(cliUnderTest));
            this.expectedResolveOrder = expectedResolveOrderOption;
            this.userCodeClassLoaderClassName = userCodeClassLoaderClassName;
        }

        @Override
        protected void run(String[] args) throws Exception {
            super.run(args);
        }

        @Override
        protected void executeProgram(Configuration configuration, PackagedProgram program) {
            assertEquals(TEST_JAR_MAIN_CLASS, program.getMainClassName());
            assertEquals(
                    expectedResolveOrder, configuration.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER));
            assertEquals(
                    userCodeClassLoaderClassName,
                    program.getUserCodeClassLoader().getClass().getName());
            this.configurationAfterExecution = configuration;
        }

        public Configuration getConfigurationAfterExecution() {
            return this.configurationAfterExecution;
        }
    }
}
