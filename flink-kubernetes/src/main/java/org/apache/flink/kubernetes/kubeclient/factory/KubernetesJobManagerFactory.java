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

package org.apache.flink.kubernetes.kubeclient.factory;

import org.apache.flink.kubernetes.entrypoint.KubernetesApplicationClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractFileDownloadDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.EnvSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.HadoopConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InitJobManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.JavaCmdJobManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.MountSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.StringUtils;

import com.bytedance.openplatform.arcee.resources.v1alpha1.AdmissionConfig;
import com.bytedance.openplatform.arcee.resources.v1alpha1.AppMasterSpec;
import com.bytedance.openplatform.arcee.resources.v1alpha1.ApplicationType;
import com.bytedance.openplatform.arcee.resources.v1alpha1.ArceeApplication;
import com.bytedance.openplatform.arcee.resources.v1alpha1.ArceeApplicationSpec;
import com.bytedance.openplatform.arcee.resources.v1alpha1.DeployMode;
import com.bytedance.openplatform.arcee.resources.v1alpha1.RestartPolicy;
import com.bytedance.openplatform.arcee.resources.v1alpha1.RestartPolicyType;
import com.bytedance.openplatform.arcee.resources.v1alpha1.SchedulingConfig;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for constructing all the Kubernetes components on the client-side. This can
 * include the Deployment, the ConfigMap(s), and the Service(s).
 */
public class KubernetesJobManagerFactory {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesJobManagerFactory.class);

	public static KubernetesJobManagerSpecification buildKubernetesJobManagerSpecification(
			KubernetesJobManagerParameters kubernetesJobManagerParameters) throws IOException {
		FlinkPod flinkPod = new FlinkPod.Builder().build();
		List<HasMetadata> accompanyingResources = new ArrayList<>();

		final KubernetesStepDecorator[] stepDecorators = new KubernetesStepDecorator[] {
			new InitJobManagerDecorator(kubernetesJobManagerParameters),
			new EnvSecretsDecorator(kubernetesJobManagerParameters),
			new MountSecretsDecorator(kubernetesJobManagerParameters),
			new JavaCmdJobManagerDecorator(kubernetesJobManagerParameters),
			new InternalServiceDecorator(kubernetesJobManagerParameters),
			new ExternalServiceDecorator(kubernetesJobManagerParameters),
			new HadoopConfMountDecorator(kubernetesJobManagerParameters),
			new FlinkConfMountDecorator(kubernetesJobManagerParameters),
			AbstractFileDownloadDecorator.create(kubernetesJobManagerParameters)
		};

		for (KubernetesStepDecorator stepDecorator: stepDecorators) {
			flinkPod = stepDecorator.decorateFlinkPod(flinkPod);
			accompanyingResources.addAll(stepDecorator.buildAccompanyingKubernetesResources());
		}

		final Deployment deployment = createJobManagerDeployment(flinkPod, kubernetesJobManagerParameters);

		final ArceeApplication application = createApplication(deployment, kubernetesJobManagerParameters);

		return new KubernetesJobManagerSpecification(deployment, application, accompanyingResources);
	}

	private static Deployment createJobManagerDeployment(
			FlinkPod flinkPod,
			KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		final Container resolvedMainContainer = flinkPod.getMainContainer();

		final Pod resolvedPod;

		if (StringUtils.isNullOrWhitespaceOnly(kubernetesJobManagerParameters.getSchedulerName())) {
			resolvedPod = new PodBuilder(flinkPod.getPod())
				.editOrNewSpec()
				.addToContainers(resolvedMainContainer)
				.endSpec()
				.build();
		} else {
			LOG.info("Set the schedulerName of JM pod to {}.", kubernetesJobManagerParameters.getSchedulerName());
			resolvedPod = new PodBuilder(flinkPod.getPod())
				.editOrNewSpec()
				.addToContainers(resolvedMainContainer)
				.withSchedulerName(kubernetesJobManagerParameters.getSchedulerName())
				.endSpec()
				.build();
		}

		final Map<String, String> labels = resolvedPod.getMetadata().getLabels();

		return new DeploymentBuilder()
			.withApiVersion(Constants.APPS_API_VERSION)
			.editOrNewMetadata()
				.withName(KubernetesUtils.getDeploymentName(kubernetesJobManagerParameters.getClusterId()))
				.withLabels(kubernetesJobManagerParameters.getLabels())
				.withAnnotations(kubernetesJobManagerParameters.getDeploymentAnnotations())
				.endMetadata()
			.editOrNewSpec()
				.withReplicas(1)
				.editOrNewTemplate()
					.withMetadata(resolvedPod.getMetadata())
					.withSpec(resolvedPod.getSpec())
					.endTemplate()
				.editOrNewSelector()
					.addToMatchLabels(labels)
					.endSelector()
				.endSpec()
			.build();
	}

	private static ArceeApplication createApplication(
		Deployment deployment,
		KubernetesJobManagerParameters kubernetesJobManagerParameters) {

		if (!kubernetesJobManagerParameters.isArceeEnabled()) {
			return null;
		}

		Map<String, String> appAnnotation = kubernetesJobManagerParameters.getArceeApplicationAnnotations();
		String appName = kubernetesJobManagerParameters.getArceeApplicationName();

		DeployMode entryPointMode = KubernetesApplicationClusterEntrypoint.class.getName().equals(
			kubernetesJobManagerParameters.getEntrypointClass()) ?
			DeployMode.Application : DeployMode.Session;

		AdmissionConfig admissionConfig = AdmissionConfig.builder()
			.account(kubernetesJobManagerParameters.getArceeAdmissionConfigAccount())
			.user(kubernetesJobManagerParameters.getArceeAdmissionConfigUser())
			.group(kubernetesJobManagerParameters.getArceeAdmissionConfigGroup())
			.build();

		SchedulingConfig schedulingConfig = SchedulingConfig.builder()
			.queue(kubernetesJobManagerParameters.getArceeSchedulingConfigQueue())
			.priorityClassName(kubernetesJobManagerParameters.getArceeSchedulingConfigPriorityClassName())
			.scheduleTimeoutSeconds(kubernetesJobManagerParameters.getArceeSchedulingConfigScheduleTimeoutSec())
			.build();

		RestartPolicy restartPolicy = RestartPolicy.builder()
			.type(RestartPolicyType.valueOf(kubernetesJobManagerParameters.getArceeRestartPolicyType()))
			.maxRetries(kubernetesJobManagerParameters.getArceeRestartPolicyMaxRetries())
			.retryIntervalSecond(kubernetesJobManagerParameters.getArceeRestartPolicyInterval())
			.build();

		AppMasterSpec amSpec = AppMasterSpec.builder()
			.replicas(deployment.getSpec().getReplicas())
			.podSpec(deployment.getSpec().getTemplate())
			.build();

		ArceeApplicationSpec spec = ArceeApplicationSpec.builder()
			.type(ApplicationType.Flink)
			.mode(entryPointMode)
			.name(appName)
			.admissionConfig(admissionConfig)
			.restartPolicy(restartPolicy)
			.schedulingConfig(schedulingConfig)
			.amSpec(amSpec)
			.build();

		ArceeApplication application = ArceeApplication.builder().spec(spec).build();
		application.setMetadata(deployment.getMetadata());
		application.getMetadata().getAnnotations().putAll(appAnnotation);

		return application;
	}
}
