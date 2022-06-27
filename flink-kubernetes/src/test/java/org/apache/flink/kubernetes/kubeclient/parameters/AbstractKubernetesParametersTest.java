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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.Quantity;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the {@link AbstractKubernetesParameters}.
 */
public class AbstractKubernetesParametersTest extends TestLogger {

	private final Configuration flinkConfig = new Configuration();
	private final TestingKubernetesParameters testingKubernetesParameters = new TestingKubernetesParameters(flinkConfig);

	@Test
	public void testClusterIdMustNotBeBlank() {
		flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, "  ");
		assertThrows(
			"must not be blank",
			IllegalArgumentException.class,
			testingKubernetesParameters::getClusterId
		);
	}

	@Test
	public void testClusterIdLengthLimitation() {
		final String stringWithIllegalLength =
			StringUtils.generateRandomAlphanumericString(new Random(), Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID + 1);
		flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, stringWithIllegalLength);
		assertThrows(
			"must be no more than " + Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID + " characters",
			IllegalArgumentException.class,
			testingKubernetesParameters::getClusterId
		);
	}

	@Test
	public void getConfigDirectory() {
		final String confDir = "/path/of/flink-conf";
		flinkConfig.set(DeploymentOptionsInternal.CONF_DIR, confDir);
		assertThat(testingKubernetesParameters.getConfigDirectory(), is(confDir));
	}

	@Test
	public void getMountedPath(){
		flinkConfig.setString(KubernetesConfigOptions.FLINK_MOUNTED_HOST_PATH.key(),
			"hadoop-config-volume,/opt/tiger/yarn_deploy, /opt/tiger/yarn_deploy;");
		Map<String, Tuple2<String, String>> mountedHostPathMap = testingKubernetesParameters.getMountedHostPath();
		assertThat(mountedHostPathMap.size(), is(1));
		assertTrue(mountedHostPathMap.containsKey("hadoop-config-volume"));
	}

	@Test
	public void getMultipleMountedPath(){
		flinkConfig.setString(
			KubernetesConfigOptions.FLINK_MOUNTED_HOST_PATH.key(),
			"hadoop-config-volume,/opt/tiger/yarn_deploy, /opt/tiger/yarn_deploy;" +
				"hadoop-config-volume2,/opt/tiger/yarn_deploy2, /opt/tiger/yarn_deploy2;");
		Map<String, Tuple2<String, String>> mountedHostPathMap = testingKubernetesParameters.getMountedHostPath();
		assertThat(mountedHostPathMap.size(), is(2));
		assertTrue(mountedHostPathMap.containsKey("hadoop-config-volume"));
		assertTrue(mountedHostPathMap.containsKey("hadoop-config-volume2"));
	}

	@Test
	public void getWrongFormatsMountedPath(){
		flinkConfig.setString(KubernetesConfigOptions.FLINK_MOUNTED_HOST_PATH.key(),
			"hadoop-config-volume,/opt/tiger/yarn_deploy;");
		assertThrows(
			"Duplicated volume name or illegal formats",
			IllegalArgumentException.class,
			testingKubernetesParameters::getMountedHostPath
		);
	}

	@Test
	public void getBlankMountedPath(){
		flinkConfig.setString(KubernetesConfigOptions.FLINK_MOUNTED_HOST_PATH.key(),
			"hadoop-config-volume,,/opt/tiger/yarn_deploy;");
		assertThrows(
			"path in host is blank",
			IllegalArgumentException.class,
			testingKubernetesParameters::getMountedHostPath
		);
	}

	@Test
	public void getIllegalUserPorts() {
		flinkConfig.setString(KubernetesConfigOptions.FLINK_JOBMANAGER_USER_PORTS.key(), "port1=1;port1:2");
		assertThrows(
				"Config of port1=1 format error.",
				IllegalArgumentException.class,
				testingKubernetesParameters::getJobManagerUserDefinedPorts);
		flinkConfig.setString(KubernetesConfigOptions.FLINK_JOBMANAGER_USER_PORTS.key(), "port1:1;port1:2");
		assertThrows(
				"Duplicate port name of port1",
				IllegalArgumentException.class,
				testingKubernetesParameters::getJobManagerUserDefinedPorts);

		flinkConfig.setString(KubernetesConfigOptions.FLINK_JOBMANAGER_USER_PORTS.key(), "port1:1;port2:1");
		assertThrows(
				"Duplicate port of 1",
				IllegalArgumentException.class,
				testingKubernetesParameters::getJobManagerUserDefinedPorts);
	}

	@Test
	public void getConfigDirectoryFallbackToPodConfDir() {
		final String confDirInPod = flinkConfig.get(KubernetesConfigOptions.FLINK_CONF_DIR);
		assertThat(testingKubernetesParameters.getConfigDirectory(), is(confDirInPod));
	}

	@Test
	public void getDnsPolicyWhenHaModeAndHostNetworkDisabled() {
		flinkConfig.setString(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
		flinkConfig.setBoolean(KubernetesConfigOptions.KUBERNETES_HOST_NETWORK_ENABLED, Boolean.FALSE);
		assertThat(testingKubernetesParameters.getDnsPolicy(), is(Constants.DNS_POLICY_DEFAULT));
	}

	@Test
	public void getDnsPolicyWhenHaModeAndHostNetworkEnabled() {
		flinkConfig.setString(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
		flinkConfig.setBoolean(KubernetesConfigOptions.KUBERNETES_HOST_NETWORK_ENABLED, Boolean.TRUE);
		assertThat(testingKubernetesParameters.getDnsPolicy(), is(Constants.DNS_POLICY_DEFAULT));
	}

	@Test
	public void getDnsPolicyWhenNonHaModeAndHostNetworkDisabled() {
		flinkConfig.setString(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.NONE.name());
		flinkConfig.setBoolean(KubernetesConfigOptions.KUBERNETES_HOST_NETWORK_ENABLED, Boolean.FALSE);
		assertThat(testingKubernetesParameters.getDnsPolicy(), is(Constants.DNS_POLICY_DEFAULT));
	}

	@Test
	public void getDnsPolicyWhenNonHaModeAndHostNetworkEnabled() {
		flinkConfig.setString(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.NONE.name());
		flinkConfig.setBoolean(KubernetesConfigOptions.KUBERNETES_HOST_NETWORK_ENABLED, Boolean.TRUE);
		assertThat(testingKubernetesParameters.getDnsPolicy(), is(Constants.DNS_POLICY_HOSTNETWORK));
	}

	@Test
	public void getCsiResourceRequirement() {
		Map<String, Quantity> resourceRequirement = testingKubernetesParameters.getCsiDiskResourceRequirement();
		// by default, this should be empty
		assertTrue(resourceRequirement.isEmpty());
		// set the resource requirement value to 1 means enable using device plugin to allocate disk
		flinkConfig.setString(KubernetesConfigOptions.CSI_DISK_RESOURCE_VALUE, "1");
		resourceRequirement = testingKubernetesParameters.getCsiDiskResourceRequirement();
		final String defaultResourceRequirementKey = "bytedance.com/local-disk";
		assertTrue(resourceRequirement.size() == 1 && resourceRequirement.containsKey(defaultResourceRequirementKey));
		assertThat(resourceRequirement.get(defaultResourceRequirementKey), is(Quantity.parse("1")));
	}

	private class TestingKubernetesParameters extends AbstractKubernetesParameters {

		public TestingKubernetesParameters(Configuration flinkConfig) {
			super(flinkConfig);
		}

		@Override
		public Map<String, String> getLabels() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public Map<String, String> getNodeSelector() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public Map<String, String> getEnvironments() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public Map<String, String> getAnnotations() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public List<Map<String, String>> getTolerations() {
			throw new UnsupportedOperationException("NOT supported");
		}

		@Override
		public String getPostStartHandlerCommand() {
			throw new UnsupportedOperationException("NOT supported");
		}
	}
}
