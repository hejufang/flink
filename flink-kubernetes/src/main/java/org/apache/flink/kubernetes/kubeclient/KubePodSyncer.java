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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This wrapper Informer and send onAdded/onDeleted/onUpdate message to ResourceManager.
 * And this will check whether Informer is watching, is out of sync to long time, this will trigger a fatal error.
 * All functions need to run in informerExecutorService(Single thread executor service to avoid concurrency issues).
 */
public class KubePodSyncer {

	private static final Logger LOG = LoggerFactory.getLogger(KubePodSyncer.class);

	private final FlinkKubeClient kubeClient;
	private final Map<String, String> podLabels;
	private final KubernetesPodSyncerCallback kubernetesPodSyncerCallback;
	private final PodSyncerEventHandler podSyncerEventHandler;
	private final ScheduledExecutorService informerExecutorService;
	private final long maxOutOfSyncTimeMs;
	private final long checkInformerRunningIntervalMs;
	private final Clock clock;

	@Nullable
	private SharedIndexInformer<Pod> sharedIndexInformer;

	long lastSyncTime = -1;

	public KubePodSyncer(
			Clock clock,
			FlinkKubeClient kubeClient,
			Map<String, String> podLabels,
			KubernetesPodSyncerCallback kubernetesPodSyncerCallback,
			long maxOutOfSyncTimeMs,
			long checkInformerRunningIntervalMs) {
		this.kubeClient = kubeClient;
		this.podLabels = podLabels;
		this.kubernetesPodSyncerCallback = kubernetesPodSyncerCallback;
		this.maxOutOfSyncTimeMs = maxOutOfSyncTimeMs;
		this.checkInformerRunningIntervalMs = checkInformerRunningIntervalMs;
		Preconditions.checkArgument(maxOutOfSyncTimeMs > checkInformerRunningIntervalMs * 2,
				"The value of maxOutOfSyncTimeMs must be twice or more than the value of checkInformerRunningIntervalMs");
		this.clock = clock;
		this.informerExecutorService = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("PodSyncerScheduled"));
		this.podSyncerEventHandler = new PodSyncerEventHandler();
	}

	public void start() {
		informerExecutorService.scheduleWithFixedDelay(
				this::checkRunningAndTryStartInformer,
				0,
				checkInformerRunningIntervalMs,
				TimeUnit.MILLISECONDS);
	}

	public void close() {
		informerExecutorService.execute(
				() -> {
					if (sharedIndexInformer != null) {
						sharedIndexInformer.close();
					}});
		informerExecutorService.shutdown();
	}

	public double getOutOfSyncTime() {
		if (!isSynced() && lastSyncTime > 0) {
			// lastSyncTime will be updated when each check attempt, so it may be one interval behind.
			return Math.max(0, clock.absoluteTimeMillis() - lastSyncTime - checkInformerRunningIntervalMs);
		} else {
			return 0;
		}
	}

	private boolean isSynced() {
		return sharedIndexInformer != null &&
				sharedIndexInformer.isRunning() &&
				sharedIndexInformer.isWatching() &&
				sharedIndexInformer.hasSynced();
	}

	/**
	 * The informer will auto-reconnect to ApiServer if local resource version is too old than ApiServer.
	 * if reconnect failed, the informer will set watching to false.
	 * We check whether informer is watching to tolerate the fail of ApiServer,
	 * But if out-of-sync too long, it will trigger Fatal error.
	 */
	@VisibleForTesting
	public void checkRunningAndTryStartInformer() {
		try {
			LOG.debug("Start to checkRunningAndTryStartInformer");
			if (lastSyncTime < 0) {
				lastSyncTime = clock.absoluteTimeMillis();
			}

			if (clock.absoluteTimeMillis() - lastSyncTime > maxOutOfSyncTimeMs) {
				kubernetesPodSyncerCallback.onFatalError(
						new IllegalStateException("Kubernetes Pod Informer out of sync more than " + maxOutOfSyncTimeMs + " ms"));
				LOG.error("Kubernetes Pod Informer out of sync more than {} ms.", maxOutOfSyncTimeMs);
				return;
			}

			if (sharedIndexInformer != null) {
				if (sharedIndexInformer.isRunning() && sharedIndexInformer.isWatching()) {
					if (sharedIndexInformer.hasSynced()) {
						lastSyncTime = clock.absoluteTimeMillis();
					}
					return;
				} else {
					sharedIndexInformer.close();
					LOG.info("Pod Informer is not running or not watching, will start new one");
				}
			}

			sharedIndexInformer = kubeClient.getInformable(podLabels).inform(podSyncerEventHandler);
		} catch (Throwable throwable) {
			LOG.error("Running checkRunningAndTryStartInformer failed, will retry", throwable);
		}
	}

	private class PodSyncerEventHandler implements ResourceEventHandler<Pod> {

		@Override
		public void onAdd(Pod pod) {
			final List<KubernetesPod> pods = Collections.singletonList(new KubernetesPod(pod));
			KubePodSyncer.this.kubernetesPodSyncerCallback.onAdded(pods);
		}

		@Override
		public void onUpdate(Pod oldPod, Pod newPod) {
			if (!Objects.equals(oldPod.getMetadata().getResourceVersion(), newPod.getMetadata().getResourceVersion())) {
				final List<KubernetesPod> pods = Collections.singletonList(new KubernetesPod(newPod));
				KubePodSyncer.this.kubernetesPodSyncerCallback.onModified(pods);
			}
		}

		@Override
		public void onDelete(Pod pod, boolean unknownFinalState) {
			final List<KubernetesPod> pods = Collections.singletonList(new KubernetesPod(pod));
			KubePodSyncer.this.kubernetesPodSyncerCallback.onDeleted(pods);
		}
	}

	/**
	 * callback.
	 */
	public interface KubernetesPodSyncerCallback{
		void onAdded(List<KubernetesPod> pods);

		void onModified(List<KubernetesPod> pods);

		void onDeleted(List<KubernetesPod> pods);

		void onFatalError(Throwable t);
	}
}
