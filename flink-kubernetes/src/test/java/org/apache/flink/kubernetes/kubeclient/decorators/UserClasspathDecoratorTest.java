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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

/**
 * Test for UserClasspathDecorator.
 */
public class UserClasspathDecoratorTest extends KubernetesJobManagerTestBase {

	private UserClasspathDecorator userClasspathDecorator;

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();
		flinkConfig.setBoolean(PipelineOptions.USER_CLASSPATH_COMPATIBLE, true);
		userClasspathDecorator = new UserClasspathDecorator(kubernetesJobManagerParameters);
	}

	@Test
	public void testCompatibleModeOn() {
		flinkConfig.set(PipelineOptions.JARS, Collections.singletonList("local:///test.jar"));
		FlinkPod flinkPod = userClasspathDecorator.decorateFlinkPod(baseFlinkPod);
		Assert.assertNotEquals(baseFlinkPod, flinkPod);
		Assert.assertTrue(
				flinkPod.getMainContainer().getEnv().stream()
						.anyMatch(envVar -> Objects.equals(envVar.getName(), Constants.FLINK_USER_CLASSPATH_ENV_KEY) &&
								Objects.equals(envVar.getValue(), "/test.jar")));
	}

	@Test
	public void testCompatibleModeOff() {
		flinkConfig.setBoolean(PipelineOptions.USER_CLASSPATH_COMPATIBLE, false);
		FlinkPod flinkPod = userClasspathDecorator.decorateFlinkPod(baseFlinkPod);
		Assert.assertEquals(baseFlinkPod, flinkPod);
	}

	@Test
	public void testGetFlinkUserClasspathWithoutAnyJar() {
		Configuration configuration = new Configuration();
		String userClasspath = userClasspathDecorator.getFlinkUserClasspath(configuration);
		Assert.assertEquals("", userClasspath);
	}

	@Test
	public void testGetFlinkUserClasspathWithEmptyJar() {
		Configuration configuration = new Configuration();
		configuration.set(PipelineOptions.JARS, Collections.emptyList());
		configuration.set(PipelineOptions.EXTERNAL_RESOURCES, Collections.emptyList());
		configuration.set(PipelineOptions.CLASSPATHS, Collections.emptyList());
		String userClasspath = userClasspathDecorator.getFlinkUserClasspath(configuration);
		Assert.assertEquals("", userClasspath);
	}

	@Test
	public void testGetFlinkUserClasspath() {
		Configuration configuration = new Configuration();
		configuration.set(PipelineOptions.JARS, Collections.singletonList("hdfs:///test.jar"));
		String userClasspath = userClasspathDecorator.getFlinkUserClasspath(configuration);
		Assert.assertEquals("/opt/tiger/workdir/test.jar", userClasspath);
	}

	@Test
	public void testGetFlinkUserClasspathWithExternalFiles() {
		Configuration configuration = new Configuration();
		configuration.set(PipelineOptions.JARS, Collections.singletonList("local:///test.jar"));
		configuration.set(PipelineOptions.EXTERNAL_RESOURCES, Arrays.asList("local:///ext1.jar", "hdfs:///ext2.jar"));
		String userClasspath = userClasspathDecorator.getFlinkUserClasspath(configuration);
		Assert.assertEquals("/test.jar:/ext1.jar:/opt/tiger/workdir/ext2.jar", userClasspath);
	}

	@Test
	public void testGetFlinkUserClasspathWithConnector() {
		Configuration configuration = new Configuration();
		configuration.set(PipelineOptions.JARS, Collections.singletonList("local:///test.jar"));
		configuration.set(PipelineOptions.CLASSPATHS,
				Arrays.asList("file:///connector1.jar", "file://%FLINK_HOME%/connector2.jar"));
		// FLINK_HOME hint
		flinkConfig.setString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, "/opt/tiger/flink_deploy/bin/kubernetes-entry.sh");
		String userClasspath = userClasspathDecorator.getFlinkUserClasspath(configuration);
		Assert.assertEquals("/test.jar:/connector1.jar:/opt/tiger/flink_deploy/connector2.jar", userClasspath);
	}
}
