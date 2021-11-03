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

package org.apache.flink.kubernetes;

import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.fabric8.mockwebserver.Context;
import io.fabric8.mockwebserver.ServerRequest;
import io.fabric8.mockwebserver.ServerResponse;
import io.fabric8.mockwebserver.dsl.MockServerExpectation;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.rules.ExternalResource;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * The mock server that host MixedDispatcher.
 */
public class MixedKubernetesServer extends ExternalResource {

	private KubernetesMockServer mock;
	private NamespacedKubernetesClient client;

	private final boolean https;

	private final boolean crudMode;

	private final MockWebServer mockWebServer;

	public MixedKubernetesServer(boolean https, boolean crudMode) {
		this.https = https;
		this.crudMode = crudMode;
		mockWebServer = new MockWebServer();
	}

	public void before() {
		HashMap<ServerRequest, Queue<ServerResponse>> response = new HashMap<>();
		mock = crudMode
			? new KubernetesMockServer(new Context(), mockWebServer, response, new MixedDispatcher(response), true)
			: new KubernetesMockServer(mockWebServer, response, https);
		mock.init();
		client = mock.createClient();
	}

	public void after() {
		mock.destroy();
		client.close();
	}

	public NamespacedKubernetesClient getClient() {
		return client;
	}

	public RecordedRequest takeRequest(long timeout, TimeUnit unit) throws Exception {
		return mockWebServer.takeRequest(timeout, unit);
	}

	public MockServerExpectation expect() {
		return mock.expect();
	}
}
