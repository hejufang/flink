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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.SharedClientHAServices;
import org.apache.flink.runtime.highavailability.SharedClientHAServicesFactory;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;

import java.util.concurrent.Executor;

/**
 * Factory for creating Kubernetes high availability services.
 */
public class KubernetesHaServicesFactory implements SharedClientHAServicesFactory {

	@Override
	public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) throws Exception {
		final boolean useOldHaServices =
			configuration.get(HighAvailabilityOptions.USE_OLD_HA_SERVICES);

		if (useOldHaServices) {
			return new KubernetesHaServices(
				KubeClientFactory.fromConfiguration(configuration),
				executor,
				configuration,
				BlobUtils.createBlobStoreFromConfig(configuration));
		} else {
			return new KubernetesMultipleComponentLeaderElectionHaServices(
				KubeClientFactory.fromConfiguration(configuration),
				executor,
				configuration,
				BlobUtils.createBlobStoreFromConfig(configuration),
				error ->
					FatalExitExceptionHandler.INSTANCE.uncaughtException(
						Thread.currentThread(), error));
		}
	}

	/**
	 * Kubernetes client high availability service, used by flink-sql-gateway to discover flink service.
	 * @param configuration configuration
	 * @return SharedClientHAServices
	 * @throws Exception
	 */
	@Override
	public SharedClientHAServices createSharedClientHAServices(Configuration configuration) throws Exception {
		FlinkKubeClient kubeClient = KubeClientFactory.fromConfiguration(configuration);

		return new KubernetesSharedClientHAServices(kubeClient, configuration);
	}
}
