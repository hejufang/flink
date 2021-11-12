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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.highavailability.KubernetesCheckpointStoreUtil;
import org.apache.flink.kubernetes.highavailability.KubernetesJobGraphStoreUtil;
import org.apache.flink.kubernetes.highavailability.KubernetesStateHandleStore;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.decorators.FileDownloadDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStore;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.DefaultJobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.NoOpJobGraphStoreWatcher;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.FunctionUtils;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.utils.Constants.CHECKPOINT_ID_KEY_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.COMPLETED_CHECKPOINT_FILE_SUFFIX;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.JOB_GRAPH_STORE_KEY_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.JVM_HS_ERROR_PATH;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_ADDRESS_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LEADER_SESSION_ID_KEY;
import static org.apache.flink.kubernetes.utils.Constants.SUBMITTED_JOBGRAPH_FILE_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common utils for Kubernetes.
 */
public class KubernetesUtils {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

	/**
	 * Check whether the port config option is a fixed port. If not, the fallback port will be set to configuration.
	 * @param flinkConfig flink configuration
	 * @param port config option need to be checked
	 * @param fallbackPort the fallback port that will be set to the configuration
	 */
	public static void checkAndUpdatePortConfigOption(
			Configuration flinkConfig,
			ConfigOption<String> port,
			int fallbackPort) {
		if (KubernetesUtils.parsePort(flinkConfig, port) == 0) {
			flinkConfig.setString(port, String.valueOf(fallbackPort));
			LOG.info(
				"Kubernetes deployment requires a fixed port. Configuration {} will be set to {}",
				port.key(),
				fallbackPort);
		}
	}

	/**
	 * Parse a valid port for the config option. A fixed port is expected, and do not support a range of ports.
	 *
	 * @param flinkConfig flink config
	 * @param port port config option
	 * @return valid port
	 */
	public static Integer parsePort(Configuration flinkConfig, ConfigOption<String> port) {
		checkNotNull(flinkConfig.get(port), port.key() + " should not be null.");

		try {
			return Integer.parseInt(flinkConfig.get(port));
		} catch (NumberFormatException ex) {
			throw new FlinkRuntimeException(
				port.key() + " should be specified to a fixed port. Do not support a range of ports.",
				ex);
		}
	}

	/**
	 * Generate name of the Deployment.
	 */
	public static String getDeploymentName(String clusterId) {
		return clusterId;
	}

	/**
	 * Get task manager labels for the current Flink cluster. They could be used to watch the pods status.
	 *
	 * @return Task manager labels.
	 */
	public static Map<String, String> getTaskManagerLabels(String clusterId) {
		final Map<String, String> labels = getCommonLabels(clusterId);
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		return Collections.unmodifiableMap(labels);
	}

	/**
	 * Get the common labels for Flink native clusters. All the Kubernetes resources will be set with these labels.
	 *
	 * @param clusterId cluster id
	 *
	 * @return Return common labels map
	 */
	public static Map<String, String> getCommonLabels(String clusterId) {
		final Map<String, String> commonLabels = new HashMap<>();
		commonLabels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		commonLabels.put(Constants.LABEL_APP_KEY, clusterId);

		return commonLabels;
	}

	/**
	 * Get ConfigMap labels for the current Flink cluster. They could be used to filter and clean-up the resources.
	 *
	 * @param clusterId cluster id
	 * @param type the config map use case. It could only be {@link Constants#LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY}
	 * now.
	 *
	 * @return Return ConfigMap labels.
	 */
	public static Map<String, String> getConfigMapLabels(String clusterId, String type) {
		final Map<String, String> labels = new HashMap<>(getCommonLabels(clusterId));
		labels.put(Constants.LABEL_CONFIGMAP_TYPE_KEY, type);
		return Collections.unmodifiableMap(labels);
	}

	/**
	 * Check the ConfigMap list should only contain the expected one.
	 *
	 * @param configMaps ConfigMap list to check
	 * @param expectedConfigMapName expected ConfigMap Name
	 *
	 * @return Return the expected ConfigMap
	 */
	public static KubernetesConfigMap checkConfigMaps(
		List<KubernetesConfigMap> configMaps,
		String expectedConfigMapName) {
		assert(configMaps.size() == 1);
		assert(configMaps.get(0).getName().equals(expectedConfigMapName));
		return configMaps.get(0);
	}

	/**
	 * Get the {@link LeaderInformation} from ConfigMap.
	 * @param configMap ConfigMap contains the leader information
	 * @return Parsed leader information. It could be {@link LeaderInformation#empty()} if there is no corresponding
	 * data in the ConfigMap.
	 */
	public static LeaderInformation getLeaderInformationFromConfigMap(KubernetesConfigMap configMap) {
		final String leaderAddress = configMap.getData().get(LEADER_ADDRESS_KEY);
		final String sessionIDStr = configMap.getData().get(LEADER_SESSION_ID_KEY);
		final UUID sessionID = sessionIDStr == null ? null : UUID.fromString(sessionIDStr);
		if (leaderAddress == null && sessionIDStr == null) {
			return LeaderInformation.empty();
		}
		return LeaderInformation.known(sessionID, leaderAddress);
	}

	/**
	 * Create a {@link DefaultJobGraphStore} with {@link NoOpJobGraphStoreWatcher}.
	 *
	 * @param configuration configuration to build a RetrievableStateStorageHelper
	 * @param flinkKubeClient flink kubernetes client
	 * @param configMapName ConfigMap name
	 * @param lockIdentity lock identity to check the leadership
	 *
	 * @return a {@link DefaultJobGraphStore} with {@link NoOpJobGraphStoreWatcher}
	 * @throws Exception when create the storage helper
	 */
	public static JobGraphStore createJobGraphStore(
			Configuration configuration,
			FlinkKubeClient flinkKubeClient,
			String configMapName,
			String lockIdentity) throws Exception {

		final KubernetesStateHandleStore<JobGraph> stateHandleStore = createJobGraphStateHandleStore(
			configuration, flinkKubeClient, configMapName, lockIdentity);
		return new DefaultJobGraphStore<>(
			stateHandleStore,
			NoOpJobGraphStoreWatcher.INSTANCE,
			KubernetesJobGraphStoreUtil.INSTANCE);
	}

	/**
	 * Create a {@link KubernetesStateHandleStore} which storing {@link JobGraph}.
	 *
	 * @param configuration configuration to build a RetrievableStateStorageHelper
	 * @param flinkKubeClient flink kubernetes client
	 * @param configMapName ConfigMap name
	 * @param lockIdentity lock identity to check the leadership
	 *
	 * @return a {@link KubernetesStateHandleStore} which storing {@link JobGraph}.
	 *
	 * @throws Exception when create the storage helper
	 */
	public static KubernetesStateHandleStore<JobGraph> createJobGraphStateHandleStore(
			Configuration configuration,
			FlinkKubeClient flinkKubeClient,
			String configMapName,
			String lockIdentity) throws Exception {

		final RetrievableStateStorageHelper<JobGraph> stateStorage =
			new FileSystemStateStorageHelper<>(HighAvailabilityServicesUtils
				.getClusterHighAvailableStoragePath(configuration), SUBMITTED_JOBGRAPH_FILE_PREFIX);

		return new KubernetesStateHandleStore<>(
			flinkKubeClient,
			configMapName,
			stateStorage,
			k -> k.startsWith(JOB_GRAPH_STORE_KEY_PREFIX),
			lockIdentity);
	}

	/**
	 * Create a {@link DefaultCompletedCheckpointStore} with {@link KubernetesStateHandleStore}.
	 *
	 * @param configuration configuration to build a RetrievableStateStorageHelper
	 * @param kubeClient flink kubernetes client
	 * @param configMapName ConfigMap name
	 * @param executor executor to run blocking calls
	 * @param lockIdentity lock identity to check the leadership
	 * @param maxNumberOfCheckpointsToRetain max number of checkpoints to retain on state store handle
	 *
	 * @return a {@link DefaultCompletedCheckpointStore} with {@link KubernetesStateHandleStore}.
	 *
	 * @throws Exception when create the storage helper failed
	 */
	public static CompletedCheckpointStore createCompletedCheckpointStore(
			Configuration configuration,
			FlinkKubeClient kubeClient,
			Executor executor,
			String configMapName,
			String lockIdentity,
			int maxNumberOfCheckpointsToRetain) throws Exception {

		final RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage =
			new FileSystemStateStorageHelper<>(HighAvailabilityServicesUtils
				.getClusterHighAvailableStoragePath(configuration), COMPLETED_CHECKPOINT_FILE_SUFFIX);
		final KubernetesStateHandleStore<CompletedCheckpoint> stateHandleStore = new KubernetesStateHandleStore<>(
			kubeClient, configMapName, stateStorage, k -> k.startsWith(CHECKPOINT_ID_KEY_PREFIX), lockIdentity);
		return new DefaultCompletedCheckpointStore<>(
			maxNumberOfCheckpointsToRetain,
			stateHandleStore,
			KubernetesCheckpointStoreUtil.INSTANCE,
			executor);
	}

	/**
	 * Get resource requirements from memory and cpu.
	 *
	 * @param mem Memory in mb.
	 * @param cpu cpu.
	 * @param externalResources external resources
	 * @return KubernetesResource requirements.
	 */
	public static ResourceRequirements getResourceRequirements(int mem, double cpu, Map<String, Long> externalResources) {
		final Quantity cpuQuantity = new Quantity(String.valueOf(cpu));
		final Quantity memQuantity = new Quantity(mem + Constants.RESOURCE_UNIT_MB);

		ResourceRequirementsBuilder resourceRequirementsBuilder = new ResourceRequirementsBuilder()
			.addToRequests(Constants.RESOURCE_NAME_MEMORY, memQuantity)
			.addToRequests(Constants.RESOURCE_NAME_CPU, cpuQuantity)
			.addToLimits(Constants.RESOURCE_NAME_MEMORY, memQuantity)
			.addToLimits(Constants.RESOURCE_NAME_CPU, cpuQuantity);

		// Add the external resources to resource requirement.
		for (Map.Entry<String, Long> externalResource: externalResources.entrySet()) {
			final Quantity resourceQuantity = new Quantity(String.valueOf(externalResource.getValue()));
			resourceRequirementsBuilder
				.addToRequests(externalResource.getKey(), resourceQuantity)
				.addToLimits(externalResource.getKey(), resourceQuantity);
			LOG.info("Request external resource {} with config key {}.", resourceQuantity.getAmount(), externalResource.getKey());
		}

		return resourceRequirementsBuilder.build();
	}

	public static String getCommonStartCommand(
			Configuration flinkConfig,
			ClusterComponent mode,
			String jvmMemOpts,
			String configDirectory,
			String logDirectory,
			boolean hasLogback,
			boolean hasLog4j,
			String mainClass,
			@Nullable String mainArgs) {
		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("classpath", "-classpath " + "$" + Constants.ENV_FLINK_CLASSPATH);

		startCommandValues.put("jvmmem", jvmMemOpts);

		final String opts;
		final String logFileName;
		if (mode == ClusterComponent.JOB_MANAGER) {
			opts = getJavaOpts(flinkConfig, CoreOptions.FLINK_JM_JVM_OPTIONS);
			logFileName = "jobmanager";
		} else {
			opts = getJavaOpts(flinkConfig, CoreOptions.FLINK_TM_JVM_OPTIONS);
			logFileName = "taskmanager";
		}

		// add gc thread for jvm configuration
		final String jvmOpts = getJavaOptsWithParallelGCThreads(opts, flinkConfig, mode == ClusterComponent.JOB_MANAGER);

		startCommandValues.put("jvmopts", jvmOpts);

		startCommandValues.put("logging",
			getLogging(logDirectory + "/" + logFileName + ".log", configDirectory, hasLogback, hasLog4j, flinkConfig));

		startCommandValues.put("class", mainClass);

		startCommandValues.put("args", mainArgs != null ? mainArgs : "");

		startCommandValues.put("redirects",
			"1> " + logDirectory + "/" + logFileName + ".out " +
			"2> " + logDirectory + "/" + logFileName + ".err");

		final String commandTemplate = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE);
		return BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
	}

	public static List<File> checkJarFileForApplicationMode(Configuration configuration) {
		return configuration.get(PipelineOptions.JARS).stream().map(
			FunctionUtils.uncheckedFunction(
				uri -> {
					final URI jarURI = PackagedProgramUtils.resolveURI(uri);
					if (jarURI.getScheme().equals(Constants.LOCAL_SCHEME) && jarURI.isAbsolute()) {
						return new File(jarURI.getPath());
					} else if (!jarURI.getScheme().equals(Constants.FILE_SCHEME)) {
						// for remote file, return downloaded path
						String jarFile = FileDownloadDecorator.getDownloadedPath(jarURI, configuration);
						return new File(jarFile);
					}
					// local disk file (scheme: file) should be uploaded to external storage and save its remote URI in this key
					throw new IllegalArgumentException("Only \"local\" or remote storage (hdfs/s3 etc) is supported as schema for application mode." +
							" This assumes that the jar is located in the image, not the Flink client. Or the jar could be downloaded." +
							" If you provide a disk file, have you specified a correct upload path on external storage so Flink could upload your disk file to that place?" +
							" An example of such local path is: local:///opt/flink/examples/streaming/WindowJoin.jar" +
							" An example of remote path is: s3://test-client-log/WindowJoin.jar");
				})
		).collect(Collectors.toList());
	}

	/**
	 * Upload local disk files in JARS and RESOURCES to remote storage system.
	 * <\p>
	 * This method will rewrite the JARS and RESOURCES URIs into the configuration.
	 * For the files that need to upload ("file" scheme), it will save the remote URI.
	 * For the files that has been already in remote storage or in the image, it will save the original URI.
	 *
	 * @param flinkConfig
	 */
	public static void uploadLocalDiskFilesToRemote(Configuration flinkConfig, Path targetDir) {
		// count number of files that need to upload
		long numOfDiskFiles = Stream.concat(
			flinkConfig.getOptional(PipelineOptions.JARS).orElse(Collections.emptyList()).stream(),
			flinkConfig.getOptional(PipelineOptions.EXTERNAL_RESOURCES).orElse(Collections.emptyList()).stream()
		).filter(uri -> {
				final URI jarURI;
				try {
					jarURI = PackagedProgramUtils.resolveURI(uri);
					return jarURI.getScheme().equals(Constants.FILE_SCHEME);
				} catch (URISyntaxException e) {
					LOG.warn("Can not resolve URI for path: {}", uri, e);
					return false;
				}
			}
		).count();
		if (numOfDiskFiles <= 0) {
			// don't need to upload any file
			return;
		}
		try {
			FileSystem fileSystem = targetDir.getFileSystem();
			if (!fileSystem.exists(targetDir)) {
				fileSystem.mkdirs(targetDir);
			}
		} catch (IOException e) {
			LOG.error("create target dir {} failed:", targetDir, e);
			return;
		}
		// upload user jar
		List<String> uploadedJars = new ArrayList<>();
		List<String> jars = flinkConfig.get(PipelineOptions.JARS)
			.stream()
			.map(FunctionUtils.uncheckedFunction(
				uri -> {
					final URI jarURI = PackagedProgramUtils.resolveURI(uri);
					if (jarURI.getScheme().equals(Constants.FILE_SCHEME)) {
						// upload to target dir
						String uploadedPath = copyFileToTargetRemoteDir(jarURI, targetDir);
						uploadedJars.add(uploadedPath);
						return uploadedPath;
					} else {
						// return directly if it is a local (located inside the image) or remote file path
						return jarURI.toString();
					}
				}
				)
			).collect(Collectors.toList());
		// replace path of user jar by the uploaded path
		flinkConfig.set(PipelineOptions.JARS, jars);
		// upload resources
		List<String> resources = flinkConfig.getOptional(PipelineOptions.EXTERNAL_RESOURCES)
			.orElse(Collections.emptyList())
			.stream()
			.map(FunctionUtils.uncheckedFunction(
				uri -> {
					final URI jarURI = PackagedProgramUtils.resolveURI(uri);
					if (jarURI.getScheme().equals(Constants.FILE_SCHEME)) {
						return copyFileToTargetRemoteDir(jarURI, targetDir);
					} else {
						// return directly if it is a local (located inside the image) or remote file path
						return jarURI.toString();
					}
				}
				)
			).collect(Collectors.toList());
		// add remote files in "JARS" to "EXTERNAL_RESOURCES", FileDownloadDecorator will setup to download these files.
		resources.addAll(uploadedJars);
		// replace path of resources by the uploaded path
		flinkConfig.set(PipelineOptions.EXTERNAL_RESOURCES, resources);
	}

	/**
	 * Copy one file to target directory.
	 * @param uri The uri of this file
	 * @param targetDir destination directory of this file
	 * @return The copied path of the file if copy succeed otherwise return the original path
	 */
	private static String copyFileToTargetRemoteDir(URI uri, Path targetDir) {
		Path path = new Path(uri);
		try {
			String targetPath = String.join("/", targetDir.toUri().toString(), path.getName());
			LOG.info("upload local file {} into remote dir {}", path, targetPath);
			FileUtils.copy(path, new Path(targetPath), false);
			return targetPath;
		} catch (IOException e) {
			LOG.error("upload local file {} into remote dir {} failed:", path, targetDir, e);
		}
		// will return the original path if copy failed
		return uri.toString();
	}

	private static String getJavaOpts(Configuration flinkConfig, ConfigOption<String> configOption) {
		String baseJavaOpts = flinkConfig.getString(CoreOptions.FLINK_JVM_OPTIONS);

		if (flinkConfig.getBoolean(CoreOptions.FLINK_GC_G1_ENABLE)) {
			baseJavaOpts += " -XX:+UseG1GC";
			baseJavaOpts += " -XX:MaxGCPauseMillis=" + flinkConfig.getInteger(CoreOptions.FLINK_MAX_GC_PAUSE_MILLIS);
		}

		String logDirectory = flinkConfig.getString(KubernetesConfigOptions.FLINK_LOG_DIR);
		if (flinkConfig.getString(CoreOptions.FLINK_GC_LOG_OPTS).length() > 0) {
			baseJavaOpts += " " + flinkConfig.getString(CoreOptions.FLINK_GC_LOG_OPTS);
			baseJavaOpts += " -Xloggc:" + logDirectory + "/gc.log";
		}

		if (flinkConfig.getBoolean(CoreOptions.FLINK_JVM_ERROR_FILE_ENABLED)) {
			/* Because the default PID number under K8S is 1, it is distinguished by time stamp */
			baseJavaOpts += " -XX:ErrorFile=" + JVM_HS_ERROR_PATH + "hs_err_ts_" + System.currentTimeMillis() + ".log";
		}

		if (flinkConfig.getString(configOption).length() > 0) {
			return baseJavaOpts + " " + flinkConfig.getString(configOption);
		} else {
			return baseJavaOpts;
		}
	}

	private static String getJavaOptsWithParallelGCThreads(String baseJavaOpts, Configuration flinkConfig, boolean isJobManager) {

		// use cores as gc.thread.num
		if (flinkConfig.getBoolean(CoreOptions.FLINK_GC_THREAD_NUM_USE_CORES)) {
			if (!baseJavaOpts.contains("-XX:ParallelGCThreads=")) {

				double containerVcores;
				if (isJobManager) {
					containerVcores = flinkConfig.getDouble(KubernetesConfigOptions.JOB_MANAGER_CPU);
				} else {
					containerVcores = flinkConfig.getDouble(KubernetesConfigOptions.TASK_MANAGER_CPU);
				}

				if (containerVcores <= 0) {
					baseJavaOpts += " -XX:ParallelGCThreads=" + ConfigConstants.FLINK_GC_THREAD_NUM_DEFAULT;
				} else {
					baseJavaOpts += " -XX:ParallelGCThreads=" + (int) Math.ceil(containerVcores);
				}
			}
		}
		return baseJavaOpts;
	}

	private static String getLogging(String logFile, String confDir, boolean hasLogback, boolean hasLog4j, Configuration flinkConfig) {
		StringBuilder logging = new StringBuilder();
		if (hasLogback || hasLog4j) {
			logging.append("-Dlog.file=").append(logFile);
			logging.append(" -Dlog.level=").append(flinkConfig.get(CoreOptions.FLINK_LOG_LEVEL));
			if (hasLogback) {
				logging.append(" -Dlogback.configurationFile=file:").append(confDir).append("/").append(CONFIG_FILE_LOGBACK_NAME);
			}
			if (hasLog4j) {
				logging.append(" -Dlog4j.configurationFile=file:").append(confDir).append("/").append(CONFIG_FILE_LOG4J_NAME);
				//enable child threads to inherit the Thread Context Map
				logging.append(" -Dlog4j2.isThreadContextMapInheritable=true");
			}

			// databus channel configuration for log collection.
			String databusChannel = flinkConfig.getString(ConfigConstants.FLINK_LOG_DATABUS_CHANNEL_KEY,
					ConfigConstants.FLINK_LOG_DATABUS_CHANNEL_DEFAULT);
			String databusLevel = flinkConfig.getString(ConfigConstants.FLINK_LOG_DATABUS_LEVEL_KEY,
					ConfigConstants.FLINK_LOG_DATABUS_LEVEL_DEFAULT);
			Long permitsPerSecond = flinkConfig.getLong(ConfigConstants.FLINK_LOG_DATABUS_PERMITS_PER_SECOND_KEY,
					ConfigConstants.FLINK_LOG_DATABUS_PERMITS_PER_SECOND_DEFAULT);
			logging.append(" -Dlog.databus.channel=").append(databusChannel);
			logging.append(" -Dlog.databus.level=").append(databusLevel);
			logging.append(" -Dlog.databus.permitsPerSecond=").append(permitsPerSecond);

			// streamlog configuration for log collection.
			if (flinkConfig.getBoolean(ConfigConstants.FLINK_LOG_STREAMLOG_ENABLED_KEY,
					ConfigConstants.FLINK_LOG_STREAMLOG_ENABLED_DEFAULT)) {
				String streamlogPsm = flinkConfig.getString(ConfigConstants.FLINK_LOG_STREAMLOG_PSM_KEY,
						ConfigConstants.FLINK_LOG_STREAMLOG_PSM_DEFAULT);
				logging.append(" -Dlog.streamlog.psm=").append(streamlogPsm);
				String streamlogLevel = flinkConfig.getString(ConfigConstants.FLINK_LOG_STREAMLOG_LEVEL_KEY,
						ConfigConstants.FLINK_LOG_STREAMLOG_LEVEL_DEFAULT);
				logging.append(" -Dlog.streamlog.level=").append(streamlogLevel);
			} else {
				logging.append(" -Dlog.streamlog.level=").append(ConfigConstants.FLINK_LOG_STREAMLOG_OFF_LEVEL);
			}
		}
		return logging.toString();
	}

	public static String getWebShell(String podName, String namespace) {
		String zoneName = System.getenv(Constants.ZONE_ENV_KEY);
		String idc = System.getenv(Constants.INTERNAL_IDC_ENV_KEY);
		String clusterName = System.getenv(Constants.PHYSICAL_CLUSTER_ENV_KEY);
		return String.format(Constants.WEB_SHELL_TEMPLATE, zoneName, idc, clusterName, podName, namespace);
	}

	/**
	 * Cluster components.
	 */
	public enum ClusterComponent {
		JOB_MANAGER,
		TASK_MANAGER
	}

	private KubernetesUtils() {}
}
