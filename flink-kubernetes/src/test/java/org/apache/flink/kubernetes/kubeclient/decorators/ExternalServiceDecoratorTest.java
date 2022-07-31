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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.networking.v1beta1.Ingress;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * General tests for the {@link ExternalServiceDecorator}.
 */
public class ExternalServiceDecoratorTest extends KubernetesJobManagerTestBase {

	private ExternalServiceDecorator externalServiceDecorator;

	private Map<String, String> customizedAnnotations = new HashMap<String, String>() {
		{
			put("annotation1", "annotation-value1");
			put("annotation2", "annotation-value2");
		}
	};

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS, customizedAnnotations);
		this.externalServiceDecorator = new ExternalServiceDecorator(this.kubernetesJobManagerParameters);
	}

	@Test
	public void testBuildAccompanyingKubernetesResources() throws IOException {
		final List<HasMetadata> resources = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(1, resources.size());

		final Service restService = (Service) resources.get(0);

		assertEquals(Constants.API_VERSION, restService.getApiVersion());

		assertEquals(ExternalServiceDecorator.getExternalServiceName(CLUSTER_ID), restService.getMetadata().getName());

		final Map<String, String> expectedLabels = getCommonLabels();
		assertEquals(expectedLabels, restService.getMetadata().getLabels());

		assertEquals(KubernetesConfigOptions.ServiceExposedType.LoadBalancer.name(), restService.getSpec().getType());

		final List<ServicePort> expectedServicePorts = Arrays.asList(
			new ServicePortBuilder()
				.withName(Constants.REST_PORT_NAME)
				.withPort(REST_PORT)
				.withNewTargetPort(Constants.REST_PORT_NAME)
				.build(),
			new ServicePortBuilder()
				.withName(Constants.SOCKET_PORT_NAME)
				.withPort(SOCKET_PORT)
				.withNewTargetPort(Constants.SOCKET_PORT_NAME)
				.build());
		assertEquals(expectedServicePorts, restService.getSpec().getPorts());

		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
		expectedLabels.putAll(userLabels);
		assertEquals(expectedLabels, restService.getSpec().getSelector());

		final Map<String, String> resultAnnotations = restService.getMetadata().getAnnotations();
		assertThat(resultAnnotations, is(equalTo(customizedAnnotations)));
	}

	@Test
	public void testSetServiceExposedType() throws IOException {
		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, KubernetesConfigOptions.ServiceExposedType.NodePort);
		final List<HasMetadata> resources = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(KubernetesConfigOptions.ServiceExposedType.NodePort.name(),
			((Service) resources.get(0)).getSpec().getType());

		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, KubernetesConfigOptions.ServiceExposedType.ClusterIP);
		final List<HasMetadata> servicesWithClusterIP = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(KubernetesConfigOptions.ServiceExposedType.ClusterIP.name(), ((Service) servicesWithClusterIP.get(0)).getSpec().getType());
	}

	@Test
	public void testIngress() throws IOException {
		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, KubernetesConfigOptions.ServiceExposedType.ClusterIP);
		this.flinkConfig.set(KubernetesConfigOptions.KUBERNETES_INGRESS_HOST, "lf-cloudnative.byted.org");
		this.flinkConfig.set(KubernetesConfigOptions.KUBERNETES_INGRESS_ENABLE, true);
		final List<HasMetadata> resources = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(2, resources.size());
		Ingress ingress = (Ingress) resources.get(1);
		assertEquals(ExternalServiceDecorator.getIngressHost(CLUSTER_ID, "lf-cloudnative.byted.org"),
			ingress.getSpec().getRules().get(0).getHost());
		assertEquals(ExternalServiceDecorator.getExternalServiceName(CLUSTER_ID),
			ingress.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getServiceName());
		assertEquals(Integer.valueOf(kubernetesJobManagerParameters.getRestPort()),
			ingress.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getServicePort().getIntVal());
	}
}
