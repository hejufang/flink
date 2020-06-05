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

package org.apache.flink.yarn;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.configuration.HdfsConfigOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.BtraceUtil;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_PLUGINS_DIR;
import static org.apache.flink.configuration.ConfigConstants.STREAMING_JOB_KEY_PREFIX;
import static org.apache.flink.configuration.ConfigConstants.YARN_APPLICATION_TYPE;
import static org.apache.flink.configuration.ConfigConstants.YARN_STREAMING_APPLICATION_TYPE_DEFAULT;
import static org.apache.flink.configuration.JobManagerOptions.CHECK_JOB_UNIQUE;
import static org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever.JOB_GRAPH_FILE_PATH;
import static org.apache.flink.yarn.YarnConfigKeys.SPT_NOENV;
import static org.apache.flink.yarn.cli.FlinkYarnSessionCli.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.yarn.cli.FlinkYarnSessionCli.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.yarn.cli.FlinkYarnSessionCli.getDynamicProperties;

/**
 * The descriptor with deployment information for deploying a Flink cluster on Yarn.
 */
public abstract class AbstractYarnClusterDescriptor implements ClusterDescriptor<ApplicationId> {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractYarnClusterDescriptor.class);

	private final YarnConfiguration yarnConfiguration;

	private final YarnClient yarnClient;

	/** True if the descriptor must not shut down the YarnClient. */
	private final boolean sharedYarnClient;

	private String yarnQueue;

	private String configurationDirectory;

	private Path flinkJarPath;

	private String dynamicPropertiesEncoded;

	/** Lazily initialized list of files to ship. */
	protected List<File> shipFiles = new LinkedList<>();

	private final Configuration flinkConfiguration;

	private boolean detached;

	private String customName;

	private String zookeeperNamespace;

	private String nodeLabel;

	private String applicationType;

	/** Optional Jar file to include in the system class loader of all application nodes
	 * (for per-job submission). */
	private final Set<File> userJarFiles = new HashSet<>();

	private YarnConfigOptions.UserJarInclusion userJarInclusion;

	public AbstractYarnClusterDescriptor(
			Configuration flinkConfiguration,
			YarnConfiguration yarnConfiguration,
			String configurationDirectory,
			YarnClient yarnClient,
			boolean sharedYarnClient) {

		this.yarnConfiguration = Preconditions.checkNotNull(yarnConfiguration);
		this.yarnClient = Preconditions.checkNotNull(yarnClient);
		this.sharedYarnClient = sharedYarnClient;

		this.flinkConfiguration = Preconditions.checkNotNull(flinkConfiguration);
		userJarInclusion = getUserJarInclusionMode(flinkConfiguration);

		this.configurationDirectory = Preconditions.checkNotNull(configurationDirectory);
	}

	public YarnClient getYarnClient() {
		return yarnClient;
	}

	/**
	 * The class to start the application master with. This class runs the main
	 * method in case of session cluster.
	 */
	protected abstract String getYarnSessionClusterEntrypoint();

	/**
	 * The class to start the application master with. This class runs the main
	 * method in case of the job cluster.
	 */
	protected abstract String getYarnJobClusterEntrypoint();

	public Configuration getFlinkConfiguration() {
		return flinkConfiguration;
	}

	@Override
	public void setDefaultConfigurationForStream() {
		GlobalConfiguration.reloadConfigWithSpecificProperties(flinkConfiguration, STREAMING_JOB_KEY_PREFIX);
	}

	public void setQueue(String queue) {
		this.yarnQueue = queue;
	}

	public void setLocalJarPath(Path localJarPath) {
		if (!localJarPath.toString().endsWith("jar")) {
			throw new IllegalArgumentException("The passed jar path ('" + localJarPath + "') does not end with the 'jar' extension");
		}
		this.flinkJarPath = localJarPath;
	}

	/**
	 * Sets the user jar which is included in the system classloader of all nodes.
	 */
	public void setProvidedUserJarFiles(List<URL> userJarFiles) {
		for (URL jarFile : userJarFiles) {
			try {
				this.userJarFiles.add(new File(jarFile.toURI()));
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException("Couldn't add local user jar: " + jarFile
					+ " Currently only file:/// URLs are supported.");
			}
		}
	}

	/**
	 * Adds the given files to the list of files to ship.
	 *
	 * <p>Note that any file matching "<tt>flink-dist*.jar</tt>" will be excluded from the upload by
	 * {@link #uploadAndRegisterFiles(Collection, FileSystem, Path, ApplicationId, List, Map, StringBuilder)}
	 * since we upload the Flink uber jar ourselves and do not need to deploy it multiple times.
	 *
	 * @param shipFiles files to ship
	 */
	public void addShipFiles(List<File> shipFiles) {
		this.shipFiles.addAll(shipFiles);
	}

	public void setDynamicPropertiesEncoded(String dynamicPropertiesEncoded) {
		this.dynamicPropertiesEncoded = dynamicPropertiesEncoded;
	}

	public String getDynamicPropertiesEncoded() {
		return this.dynamicPropertiesEncoded;
	}

	private void isReadyForDeployment(ClusterSpecification clusterSpecification) throws YarnDeploymentException {

		if (clusterSpecification.getNumberTaskManagers() <= 0) {
			throw new YarnDeploymentException("Taskmanager count must be positive");
		}
		if (this.flinkJarPath == null) {
			throw new YarnDeploymentException("The Flink jar path is null");
		}
		if (this.configurationDirectory == null) {
			throw new YarnDeploymentException("Configuration directory not set");
		}
		if (this.flinkConfiguration == null) {
			throw new YarnDeploymentException("Flink configuration object has not been set");
		}

		// check if required Hadoop environment variables are set. If not, warn user
		if (System.getenv("HADOOP_CONF_DIR") == null &&
			System.getenv("YARN_CONF_DIR") == null) {
			LOG.warn("Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set. " +
				"The Flink YARN Client needs one of these to be set to properly load the Hadoop " +
				"configuration for accessing YARN.");
		}
	}

	private static boolean allocateResource(int[] nodeManagers, int toAllocate) {
		for (int i = 0; i < nodeManagers.length; i++) {
			if (nodeManagers[i] >= toAllocate) {
				nodeManagers[i] -= toAllocate;
				return true;
			}
		}
		return false;
	}

	/**
	 * @deprecated The cluster descriptor should not know about this option.
	 */
	@Deprecated
	public void setDetachedMode(boolean detachedMode) {
		this.detached = detachedMode;
	}

	/**
	 * @deprecated The cluster descriptor should not know about this option.
	 */
	@Deprecated
	public boolean isDetachedMode() {
		return detached;
	}

	public String getZookeeperNamespace() {
		return zookeeperNamespace;
	}

	public void setZookeeperNamespace(String zookeeperNamespace) {
		this.zookeeperNamespace = zookeeperNamespace;
	}

	public String getNodeLabel() {
		return nodeLabel;
	}

	public void setNodeLabel(String nodeLabel) {
		this.nodeLabel = nodeLabel;
	}

	// -------------------------------------------------------------
	// Lifecycle management
	// -------------------------------------------------------------

	@Override
	public void close() {
		if (!sharedYarnClient) {
			yarnClient.stop();
		}
	}

	// -------------------------------------------------------------
	// ClusterClient overrides
	// -------------------------------------------------------------

	@Override
	public ClusterClient<ApplicationId> retrieve(ApplicationId applicationId) throws ClusterRetrieveException {

		try {
			// check if required Hadoop environment variables are set. If not, warn user
			if (System.getenv("HADOOP_CONF_DIR") == null &&
				System.getenv("YARN_CONF_DIR") == null) {
				LOG.warn("Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set." +
					"The Flink YARN Client needs one of these to be set to properly load the Hadoop " +
					"configuration for accessing YARN.");
			}

			final ApplicationReport appReport = yarnClient.getApplicationReport(applicationId);

			if (appReport.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
				// Flink cluster is not running anymore
				LOG.error("The application {} doesn't run anymore. It has previously completed with final status: {}",
					applicationId, appReport.getFinalApplicationStatus());
				throw new RuntimeException("The Yarn application " + applicationId + " doesn't run anymore.");
			}

			final String host = appReport.getHost();
			final int rpcPort = appReport.getRpcPort();

			LOG.info("Found application JobManager host name '{}' and port '{}' from supplied application id '{}'",
				host, rpcPort, applicationId);

			flinkConfiguration.setString(JobManagerOptions.ADDRESS, host);
			flinkConfiguration.setInteger(JobManagerOptions.PORT, rpcPort);

			flinkConfiguration.setString(RestOptions.ADDRESS, host);
			flinkConfiguration.setInteger(RestOptions.PORT, rpcPort);

			return createYarnClusterClient(
				this,
				-1, // we don't know the number of task managers of a started Flink cluster
				-1, // we don't know how many slots each task manager has for a started Flink cluster
				appReport,
				flinkConfiguration,
				false);
		} catch (Exception e) {
			throw new ClusterRetrieveException("Couldn't retrieve Yarn cluster", e);
		}
	}

	@Override
	public ClusterClient<ApplicationId> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
		try {
			return deployInternal(
				clusterSpecification,
				"Flink session cluster",
				getYarnSessionClusterEntrypoint(),
				null,
				false);
		} catch (Exception e) {
			throw new ClusterDeploymentException("Couldn't deploy Yarn session cluster", e);
		}
	}

	@Override
	public ClusterClient<ApplicationId> deploySessionCluster(ClusterSpecification clusterSpecification, boolean detached) throws ClusterDeploymentException {
		try {
			return deployInternal(
					clusterSpecification,
					"Flink session cluster",
					getYarnSessionClusterEntrypoint(),
					null,
					detached);
		} catch (Exception e) {
			throw new ClusterDeploymentException("Couldn't deploy Yarn session cluster", e);
		}
	}

	@Override
	public void killCluster(ApplicationId applicationId) throws FlinkException {
		try {
			yarnClient.killApplication(applicationId);
			Utils.deleteApplicationFiles(Collections.singletonMap(
				YarnConfigKeys.FLINK_YARN_FILES,
				getYarnFilesDir(applicationId).toUri().toString()));
		} catch (YarnException | IOException e) {
			throw new FlinkException("Could not kill the Yarn Flink cluster with id " + applicationId + '.', e);
		}
	}

	/**
	 * Method to validate cluster specification before deploy it, it will throw
	 * an {@link FlinkException} if the {@link ClusterSpecification} is invalid.
	 *
	 * @param clusterSpecification cluster specification to check against the configuration of the
	 *                             AbstractYarnClusterDescriptor
	 * @throws FlinkException if the cluster cannot be started with the provided {@link ClusterSpecification}
	 */
	private void validateClusterSpecification(ClusterSpecification clusterSpecification) throws FlinkException {
		try {
			final long taskManagerMemorySize = clusterSpecification.getTaskManagerMemoryMB();
			// We do the validation by calling the calculation methods here
			// Internally these methods will check whether the cluster can be started with the provided
			// ClusterSpecification and the configured memory requirements
			final long cutoff = ContaineredTaskManagerParameters.calculateCutoffMB(flinkConfiguration, taskManagerMemorySize);
			TaskManagerServices.calculateHeapSizeMB(taskManagerMemorySize - cutoff, flinkConfiguration);
		} catch (IllegalArgumentException iae) {
			throw new FlinkException("Cannot fulfill the minimum memory requirements with the provided " +
				"cluster specification. Please increase the memory of the cluster.", iae);
		}
	}

	/**
	 * This method will block until the ApplicationMaster/JobManager have been deployed on YARN.
	 *
	 * @param clusterSpecification Initial cluster specification for the Flink cluster to be deployed
	 * @param applicationName name of the Yarn application to start
	 * @param yarnClusterEntrypoint Class name of the Yarn cluster entry point.
	 * @param jobGraph A job graph which is deployed with the Flink cluster, {@code null} if none
	 * @param detached True if the cluster should be started in detached mode
	 */
	protected ClusterClient<ApplicationId> deployInternal(
			ClusterSpecification clusterSpecification,
			String applicationName,
			String yarnClusterEntrypoint,
			@Nullable JobGraph jobGraph,
			boolean detached) throws Exception {

		// ------------------ Check if configuration is valid --------------------
		validateClusterSpecification(clusterSpecification);

		if (UserGroupInformation.isSecurityEnabled()) {
			// note: UGI::hasKerberosCredentials inaccurately reports false
			// for logins based on a keytab (fixed in Hadoop 2.6.1, see HADOOP-10786),
			// so we check only in ticket cache scenario.
			boolean useTicketCache = flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);

			UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
			if (loginUser.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.KERBEROS
				&& useTicketCache && !loginUser.hasKerberosCredentials()) {
				LOG.error("Hadoop security with Kerberos is enabled but the login user does not have Kerberos credentials");
				throw new RuntimeException("Hadoop security with Kerberos is enabled but the login user " +
					"does not have Kerberos credentials");
			}
		}

		// ------------------ Add dynamic properties to local flinkConfiguraton ------
		Map<String, String> dynProperties = getDynamicProperties(dynamicPropertiesEncoded);
		for (Map.Entry<String, String> dynProperty : dynProperties.entrySet()) {
			flinkConfiguration.setString(dynProperty.getKey(), dynProperty.getValue());
		}

		isReadyForDeployment(clusterSpecification);

		// ------------------ Check if the specified queue exists --------------------

		checkYarnQueues(yarnClient);

		// hack fs.defaultFs in yanrConfiguration
		if (flinkConfiguration.contains(HdfsConfigOptions.HDFS_DEFAULT_FS)) {
			String defaultFS = flinkConfiguration.getString(HdfsConfigOptions.HDFS_DEFAULT_FS);
			if (defaultFS.length() > 0) {
				LOG.info("Using new fsDefault={}", defaultFS);
				yarnConfiguration.set(HdfsConfigOptions.HDFS_DEFAULT_FS.key(), defaultFS);
			}
		}

		// ------------------ Check if the YARN ClusterClient has the requested resources --------------

		// Create application via yarnClient
		final YarnClientApplication yarnApplication = yarnClient.createApplication();
		final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();

		Resource maxRes = appResponse.getMaximumResourceCapability();

		final int yarnMinAllocationMB = yarnConfiguration.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);

		final ClusterSpecification validClusterSpecification;
		try {
			validClusterSpecification = validateClusterResources(
				clusterSpecification,
				yarnMinAllocationMB,
				maxRes);
		} catch (YarnDeploymentException yde) {
			failSessionDuringDeployment(yarnClient, yarnApplication);
			throw yde;
		}

		LOG.info("Cluster specification: {}", clusterSpecification);

		final ClusterEntrypoint.ExecutionMode executionMode = detached ?
			ClusterEntrypoint.ExecutionMode.DETACHED
			: ClusterEntrypoint.ExecutionMode.NORMAL;

		flinkConfiguration.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());

		ApplicationReport report = startAppMaster(
			flinkConfiguration,
			applicationName,
			yarnClusterEntrypoint,
			jobGraph,
			yarnClient,
			yarnApplication,
			validClusterSpecification);

		String host = report.getHost();
		int port = report.getRpcPort();

		// Correctly initialize the Flink config
		flinkConfiguration.setString(JobManagerOptions.ADDRESS, host);
		flinkConfiguration.setInteger(JobManagerOptions.PORT, port);

		flinkConfiguration.setString(RestOptions.ADDRESS, host);
		flinkConfiguration.setInteger(RestOptions.PORT, port);

		// the Flink cluster is deployed in YARN. Represent cluster
		return createYarnClusterClient(
			this,
			validClusterSpecification.getNumberTaskManagers(),
			validClusterSpecification.getSlotsPerTaskManager(),
			report,
			flinkConfiguration,
			true);
	}

	protected ClusterSpecification validateClusterResources(
		ClusterSpecification clusterSpecification,
		int yarnMinAllocationMB,
		Resource maximumResourceCapability) throws YarnDeploymentException {

		int taskManagerCount = clusterSpecification.getNumberTaskManagers();
		int jobManagerMemoryMb = clusterSpecification.getMasterMemoryMB();
		int taskManagerMemoryMb = clusterSpecification.getTaskManagerMemoryMB();

		if (jobManagerMemoryMb < yarnMinAllocationMB || taskManagerMemoryMb < yarnMinAllocationMB) {
			LOG.warn("The JobManager or TaskManager memory is below the smallest possible YARN Container size. "
				+ "The value of 'yarn.scheduler.minimum-allocation-mb' is '" + yarnMinAllocationMB + "'. Please increase the memory size." +
				"YARN will allocate the smaller containers but the scheduler will account for the minimum-allocation-mb, maybe not all instances " +
				"you requested will start.");
		}

		// set the memory to minAllocationMB to do the next checks correctly
		if (jobManagerMemoryMb < yarnMinAllocationMB) {
			jobManagerMemoryMb =  yarnMinAllocationMB;
		}
		if (taskManagerMemoryMb < yarnMinAllocationMB) {
			taskManagerMemoryMb =  yarnMinAllocationMB;
		}

		final String note = "Please check the 'yarn.scheduler.maximum-allocation-mb' and the 'yarn.nodemanager.resource.memory-mb' configuration values\n";
		if (jobManagerMemoryMb > maximumResourceCapability.getMemory()) {
			throw new YarnDeploymentException("The cluster does not have the requested resources for the JobManager available!\n"
				+ "Maximum Memory: " + maximumResourceCapability.getMemory() + "MB Requested: " + jobManagerMemoryMb + "MB. " + note);
		}

		if (taskManagerMemoryMb > maximumResourceCapability.getMemory()) {
			throw new YarnDeploymentException("The cluster does not have the requested resources for the TaskManagers available!\n"
				+ "Maximum Memory: " + maximumResourceCapability.getMemory() + " Requested: " + taskManagerMemoryMb + "MB. " + note);
		}

		return new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterVcores(clusterSpecification.getMasterVcores())
			.setMasterMemoryMB(jobManagerMemoryMb)
			.setTaskManagerMemoryMB(taskManagerMemoryMb)
			.setNumberTaskManagers(clusterSpecification.getNumberTaskManagers())
			.setSlotsPerTaskManager(clusterSpecification.getSlotsPerTaskManager())
			.createClusterSpecification();

	}

	private void checkYarnQueues(YarnClient yarnClient) {
		try {
			List<QueueInfo> queues = yarnClient.getAllQueues();
			if (queues.size() > 0 && this.yarnQueue != null) { // check only if there are queues configured in yarn and for this session.
				boolean queueFound = false;
				for (QueueInfo queue : queues) {
					if (queue.getQueueName().equals(this.yarnQueue)) {
						queueFound = true;
						break;
					}
				}
				if (!queueFound) {
					String queueNames = "";
					for (QueueInfo queue : queues) {
						queueNames += queue.getQueueName() + ", ";
					}
					LOG.warn("The specified queue '" + this.yarnQueue + "' does not exist. " +
						"Available queues: " + queueNames);
				}
			} else {
				LOG.debug("The YARN cluster does not have any queues configured");
			}
		} catch (Throwable e) {
			LOG.warn("Error while getting queue information from YARN: " + e.getMessage());
			if (LOG.isDebugEnabled()) {
				LOG.debug("Error details", e);
			}
		}
	}

	public ApplicationReport startAppMaster(
			Configuration configuration,
			String applicationName,
			String yarnClusterEntrypoint,
			JobGraph jobGraph,
			YarnClient yarnClient,
			YarnClientApplication yarnApplication,
			ClusterSpecification clusterSpecification) throws Exception {

		// ------------------ Initialize the file systems -------------------------

		org.apache.flink.core.fs.FileSystem.initialize(
			configuration,
			PluginUtils.createPluginManagerFromRootFolder(configuration));

		boolean isDockerImageIncludeLib =
			flinkConfiguration.getBoolean(YarnConfigOptions.IS_DOCKER_INCLUDE_LIB);
		boolean isDockerImageIncludeUserLib =
			flinkConfiguration.getBoolean(YarnConfigOptions.IS_DOCKER_INCLUDE_USERLIB);
		String dockerImage =
			flinkConfiguration.getString(YarnConfigKeys.DOCKER_IMAGE_KEY, "");

		boolean isInDockerMode = false;
		if (dockerImage != null && !dockerImage.isEmpty()) {
			isInDockerMode = true;
		}
		flinkConfiguration.setBoolean(YarnConfigKeys.IS_IN_DOCKER_MODE_KEY, isInDockerMode);

		// initialize file system
		// Copy the application master jar to the filesystem
		// Create a local resource to point to the destination jar path
		FileSystem fs = null;
		if (configuration.getBoolean(ConfigConstants.HDFS_DEPENDENCY_ENABLED, ConfigConstants.HDFS_DEPENDENCY_ENABLED_DEFAULT)) {
			fs = FileSystem.get(yarnConfiguration);
		}

		String jobWorkDir = flinkConfiguration.getString(ConfigConstants.JOB_WORK_DIR_KEY,
				ConfigConstants.PATH_JOB_WORK_FILE);
		final Path homeDir = new Path(jobWorkDir);

		// hard coded check for the GoogleHDFS client because its not overriding the getScheme() method.
		if (fs != null && !fs.getClass().getSimpleName().equals("GoogleHadoopFileSystem") &&
				fs.getScheme().startsWith("file")) {
			LOG.warn("The file system scheme is '" + fs.getScheme() + "'. This indicates that the "
					+ "specified Hadoop configuration path is wrong and the system is using the default Hadoop configuration values."
					+ "The Flink YARN client needs to store its files in a distributed file system");
		}

		ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
		Set<File> systemShipFiles = new HashSet<>(shipFiles.size());
		for (File file : shipFiles) {
			systemShipFiles.add(file.getAbsoluteFile());
		}

		//check if there is a logback or log4j file
		File logbackFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOGBACK_NAME);
		final boolean hasLogback = logbackFile.exists();
		if (hasLogback) {
			systemShipFiles.add(logbackFile);
		}

		File log4jFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
		final boolean hasLog4j = log4jFile.exists();
		if (hasLog4j) {
			systemShipFiles.add(log4jFile);
			if (hasLogback) {
				// this means there is already a logback configuration file --> fail
				LOG.warn("The configuration directory ('" + configurationDirectory + "') contains both LOG4J and " +
					"Logback configuration files. Please delete or rename one of them.");
			}
		}

		// Add user files to work dir, and also to CLASSPATH.
		String userFiles = configuration.getString(ConfigConstants.FILES, null);
		LOG.info("userFiles = {}", userFiles);
		if (userFiles != null) {
			String [] userFileArray = userFiles.split(";");
			for (String userFile: userFileArray) {
				systemShipFiles.add(new File(userFile));
			}
		}

		addEnvironmentFoldersToShipFiles(systemShipFiles);

		// Set-up ApplicationSubmissionContext for the application

		final ApplicationId appId = appContext.getApplicationId();

		// ------------------ Add Zookeeper namespace to local flinkConfiguraton ------
		String zkNamespace = getZookeeperNamespace();
		// no user specified cli argument for namespace?
		if (zkNamespace == null || zkNamespace.isEmpty()) {
			// namespace defined in config? else use applicationId as default.
			zkNamespace = configuration.getString(HighAvailabilityOptions.HA_CLUSTER_ID, String.valueOf(appId));
			setZookeeperNamespace(zkNamespace);
		}

		configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);

		if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
			// activate re-execution of failed applications
			appContext.setMaxAppAttempts(
				configuration.getInteger(
					YarnConfigOptions.APPLICATION_ATTEMPTS.key(),
					YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS));

			activateHighAvailabilitySupport(appContext);
		} else {
			// set number of application retries to 1 in the default case
			appContext.setMaxAppAttempts(
				configuration.getInteger(
					YarnConfigOptions.APPLICATION_ATTEMPTS.key(),
					1));
		}

		// local resource map for Yarn
		final Map<String, LocalResource> localResources = new HashMap<>(2 + systemShipFiles.size() + userJarFiles.size());
		// list of remote paths (after upload)
		final List<Path> paths = new ArrayList<>(2 + systemShipFiles.size() + userJarFiles.size());
		// ship list that enables reuse of resources for task manager containers
		StringBuilder envShipFileList = new StringBuilder();

		// upload and register ship files
		List<String> systemClassPaths = new ArrayList<>();

		if (isInDockerMode && isDockerImageIncludeLib) {
			LOG.info("Reset system class path in docker mode.");
			systemClassPaths = new ArrayList<>();
			String runtimeLibDir =
				flinkConfiguration.getString(ConfigConstants.FLINK_RUNTIME_LIB_DIR_KEY,
					ConfigConstants.FLINK_RUNTIME_LIB_DIR_DEFAULT);
			String runtimeConfDir =
				flinkConfiguration.getString(ConfigConstants.FLINK_RUNTIME_CONF_DIR_KEY,
					ConfigConstants.FLINK_RUNTIME_CONF_DIR_DEFAULT);
			String runtimeBasejarDir =
				flinkConfiguration.getString(ConfigConstants.FLINK_RUNTIME_BASEJAR_KEY,
					ConfigConstants.FLINK_RUNTIME_BASEJAR_DEFAULT);
			String runtimeClasspath =
				flinkConfiguration.getString(ConfigConstants.FLINK_RUNTIME_CLASSPATH_KEY, "");
			LOG.info("runtimeLibDir = {}", runtimeLibDir);
			LOG.info("runtimeConfDir = {}", runtimeConfDir);
			LOG.info("runtimeBasejarDir = {}", runtimeBasejarDir);
			systemClassPaths.add(Paths.get(runtimeLibDir, "*").toAbsolutePath().toString());
			systemClassPaths.add(Paths.get(runtimeConfDir, "*").toAbsolutePath().toString());
			systemClassPaths.add(Paths.get(runtimeBasejarDir, "*").toAbsolutePath().toString());
			if (runtimeClasspath != null && !runtimeClasspath.isEmpty()) {
				systemClassPaths.addAll(Arrays.asList(runtimeClasspath.split(":")));
			}
			LOG.info("reset systemClassPaths to {}", systemClassPaths);
		} else {
			// upload and register ship files
			systemClassPaths = uploadAndRegisterFiles(
				systemShipFiles,
				fs,
				homeDir,
				appId,
				paths,
				localResources,
				envShipFileList);
		}

		List<String> userClassPaths = new ArrayList<>();
		if (isInDockerMode && isDockerImageIncludeUserLib) {
			LOG.info("Do not need to upload user jar in docker mode if included.");
			String userlibPath = flinkConfiguration.getString(ConfigConstants.FLINK_RUNTIME_USERLIB_PATH_KEY,
				ConfigConstants.FLINK_RUNTIME_USERLIB_PATH_DEFAULT);

			if (userlibPath == null) {
				throw new IllegalArgumentException(String.format("Please set %s to load user lib.",
					ConfigConstants.FLINK_RUNTIME_USERLIB_PATH_KEY));
			}

			LOG.info("runtimeUserlibPath = {}", userlibPath);
			// To get resources/xx.py in #EnvironmentInitUtils#prepareLocalDir
			userClassPaths.add(Paths.get(userlibPath).toAbsolutePath().toString());
		} else {
			userClassPaths = uploadAndRegisterFiles(
				userJarFiles,
				fs,
				homeDir,
				appId,
				paths,
				localResources,
				envShipFileList);
		}

		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.ORDER) {
			systemClassPaths.addAll(userClassPaths);
		}

		// normalize classpath by sorting

		// Remove sorting for class path. Ensure that user jar appear before lib jar.
//		Collections.sort(systemClassPaths);

		Collections.sort(userClassPaths);

		// classpath assembler
		StringBuilder classPathBuilder = new StringBuilder();
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST) {
			for (String userClassPath : userClassPaths) {
				classPathBuilder.append(userClassPath).append(File.pathSeparator);
			}
		}
		for (String classPath : systemClassPaths) {
			classPathBuilder.append(classPath).append(File.pathSeparator);
		}

		// set the right configuration values for the TaskManager
		int slotsPreTaskManager = clusterSpecification.getSlotsPerTaskManager();
		configuration.setInteger(
			TaskManagerOptions.NUM_TASK_SLOTS,
			slotsPreTaskManager);

		configuration.setString(
			TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY,
			clusterSpecification.getTaskManagerMemoryMB() + "m");

		int numTaskManagers;
		if (jobGraph == null) {
			numTaskManagers = clusterSpecification.getNumberTaskManagers();
		} else {
			numTaskManagers = (jobGraph.calcMinRequiredSlotsNum() + slotsPreTaskManager - 1) / slotsPreTaskManager;
		}

		final double percentage = configuration.getDouble(TaskManagerOptions.NUM_INITIAL_TASK_MANAGERS_PERCENTAGE);
		configuration.setInteger(TaskManagerOptions.NUM_INITIAL_TASK_MANAGERS, (int) (numTaskManagers * percentage));

		Path remotePathJar = null;
		if (isInDockerMode && isDockerImageIncludeLib) {
			LOG.info("Do not need to upload flink.jar in docker mode");
		} else {
			// Setup jar for ApplicationMaster
			remotePathJar = setupSingleLocalResource(
				"flink.jar",
				fs,
				appId,
				flinkJarPath,
				localResources,
				homeDir,
				"");
			paths.add(remotePathJar);
			classPathBuilder.append("flink.jar").append(File.pathSeparator);
		}

		// Upload the flink configuration
		// write out configuration file
		if (isInDockerMode && isDockerImageIncludeUserLib) {
			LOG.info("Do not need to upload flink-conf.yaml");
		} else {
			File tmpConfigurationFile = File.createTempFile(appId + "-flink-conf.yaml", null);
			tmpConfigurationFile.deleteOnExit();
			BootstrapTools.writeConfiguration(configuration, tmpConfigurationFile);

			Path remotePathConf = setupSingleLocalResource(
				"flink-conf.yaml",
				fs,
				appId,
				new Path(tmpConfigurationFile.getAbsolutePath()),
				localResources,
				homeDir,
				"");

			paths.add(remotePathConf);
			classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
		}

		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) {
			for (String userClassPath : userClassPaths) {
				classPathBuilder.append(userClassPath).append(File.pathSeparator);
			}
		}

		// write job graph to tmp file and add it to local resource
		// TODO: server use user main method to generate job graph
		if (jobGraph != null) {
			try {
				File fp = File.createTempFile(appId.toString(), null);
				fp.deleteOnExit();
				try (FileOutputStream output = new FileOutputStream(fp);
					ObjectOutputStream obOutput = new ObjectOutputStream(output);){
					obOutput.writeObject(jobGraph);
				}

				final String jobGraphFilename = "job.graph";
				flinkConfiguration.setString(JOB_GRAPH_FILE_PATH, jobGraphFilename);

				Path pathFromYarnURL = setupSingleLocalResource(
					jobGraphFilename,
					fs,
					appId,
					new Path(fp.toURI()),
					localResources,
					homeDir,
					"");
				paths.add(pathFromYarnURL);
				classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
			} catch (Exception e) {
				LOG.warn("Add job graph to local resource fail");
				throw e;
			}
		}

		final Path yarnFilesDir = getYarnFilesDir(appId);
		if (fs != null && fs.exists(yarnFilesDir)) {
			FsPermission permission = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
			fs.setPermission(yarnFilesDir, permission); // set permission for path.
		}

		//To support Yarn Secure Integration Test Scenario
		//In Integration test setup, the Yarn containers created by YarnMiniCluster does not have the Yarn site XML
		//and KRB5 configuration files. We are adding these files as container local resources for the container
		//applications (JM/TMs) to have proper secure cluster setup
		Path remoteKrb5Path = null;
		Path remoteYarnSiteXmlPath = null;
		boolean hasKrb5 = false;
		if (System.getenv("IN_TESTS") != null) {
			File f = new File(System.getenv("YARN_CONF_DIR"), Utils.YARN_SITE_FILE_NAME);
			LOG.info("Adding Yarn configuration {} to the AM container local resource bucket", f.getAbsolutePath());
			Path yarnSitePath = new Path(f.getAbsolutePath());
			remoteYarnSiteXmlPath = setupSingleLocalResource(
				Utils.YARN_SITE_FILE_NAME,
				fs,
				appId,
				yarnSitePath,
				localResources,
				homeDir,
				"");

			String krb5Config = System.getProperty("java.security.krb5.conf");
			if (krb5Config != null && krb5Config.length() != 0) {
				File krb5 = new File(krb5Config);
				LOG.info("Adding KRB5 configuration {} to the AM container local resource bucket", krb5.getAbsolutePath());
				Path krb5ConfPath = new Path(krb5.getAbsolutePath());
				remoteKrb5Path = setupSingleLocalResource(
					Utils.KRB5_FILE_NAME,
					fs,
					appId,
					krb5ConfPath,
					localResources,
					homeDir,
					"");
				hasKrb5 = true;
			}
		}

		// setup security tokens
		Path remotePathKeytab = null;
		String keytab = configuration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
		if (keytab != null) {
			LOG.info("Adding keytab {} to the AM container local resource bucket", keytab);
			remotePathKeytab = setupSingleLocalResource(
				Utils.KEYTAB_FILE_NAME,
				fs,
				appId,
				new Path(keytab),
				localResources,
				homeDir,
				"");
		}

		final ContainerLaunchContext amContainer = setupApplicationMasterContainer(
			yarnClusterEntrypoint,
			hasLogback,
			hasLog4j,
			hasKrb5,
			clusterSpecification.getMasterMemoryMB());

		if (UserGroupInformation.isSecurityEnabled()) {
			// set HDFS delegation tokens when security is enabled
			LOG.info("Adding delegation token to the AM container..");
			Utils.setTokensFor(amContainer, paths, yarnConfiguration);
		}

		amContainer.setLocalResources(localResources);
		if (fs != null) {
			fs.close();
		}

		// Setup CLASSPATH and environment variables for ApplicationMaster
		final Map<String, String> appMasterEnv = new HashMap<>();

		// set env from configuration
		if (isInDockerMode && isDockerImageIncludeUserLib) {
			ConfigUtils.writeFlinkConfigIntoEnv(appMasterEnv, configuration);
		}
		// set user specified app master environment variables
		appMasterEnv.putAll(Utils.getEnvironmentVariables(ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX, configuration));
		// set Flink app class path
		LOG.info("Classpath: {}", classPathBuilder.toString());
		appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());

		// set Flink on YARN internal configuration values
		appMasterEnv.put(YarnConfigKeys.ENV_TM_COUNT, String.valueOf(clusterSpecification.getNumberTaskManagers()));
		appMasterEnv.put(YarnConfigKeys.ENV_TM_MEMORY, String.valueOf(clusterSpecification.getTaskManagerMemoryMB()));
		if (remotePathJar == null) {
			appMasterEnv.put(YarnConfigKeys.FLINK_JAR_PATH, "");
		} else {
			appMasterEnv.put(YarnConfigKeys.FLINK_JAR_PATH, remotePathJar.toString());
		}
		appMasterEnv.put(YarnConfigKeys.ENV_APP_ID, appId.toString());
		appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, homeDir.toString());
		appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_SHIP_FILES, envShipFileList.toString());
		appMasterEnv.put(YarnConfigKeys.ENV_SLOTS, String.valueOf(clusterSpecification.getSlotsPerTaskManager()));
		appMasterEnv.put(YarnConfigKeys.ENV_DETACHED, String.valueOf(detached));
		appMasterEnv.put(YarnConfigKeys.ENV_ZOOKEEPER_NAMESPACE, getZookeeperNamespace());
		appMasterEnv.put(YarnConfigKeys.FLINK_YARN_FILES, yarnFilesDir.toUri().toString());
		Utils.addDefaultEnv(appMasterEnv);

		// https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md#identity-on-an-insecure-cluster-hadoop_user_name
		appMasterEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, UserGroupInformation.getCurrentUser().getUserName());
		String partitionList = configuration.getString(ConfigConstants.PARTITION_LIST_OLD_KEY, null);
		partitionList = configuration.getString(ConfigConstants.PARTITION_LIST_KEY, partitionList);

		if (partitionList != null && !partitionList.isEmpty()) {
			appMasterEnv.put(ConfigConstants.PARTITION_LIST_KEY, partitionList);
			LOG.info("{} = {}", ConfigConstants.PARTITION_LIST_KEY, partitionList);
		}

		appMasterEnv.put(YarnConfigKeys.ENV_SPT_NOENV, SPT_NOENV);

		String dc = configuration.getString(ConfigConstants.DC_KEY, null);
		if (dc != null) {
			appMasterEnv.put(YarnConfigKeys.ENV_FLINK_YARN_DC, dc);
		} else {
			LOG.warn("Unable to parse dc.");
		}

		boolean enableCoreDump = flinkConfiguration.getBoolean(ConfigConstants.ENABLE_CORE_DUMP_KEY,
			ConfigConstants.DEFAULT_ENABLE_CORE_DUMP);
		String jobName = getJobName();
		if (enableCoreDump && jobName != null && !jobName.isEmpty()) {
			appMasterEnv.put(YarnConfigKeys.ENV_CORE_DUMP_PROC_NAME, jobName);
		}
		if (jobName != null && !jobName.isEmpty()) {
			appMasterEnv.put(YarnConfigKeys.ENV_LOAD_SERVICE_PSM, YarnConfigKeys.ENV_PSM_PREFIX + "." + jobName);
		}

		// Add environment params to AM appMasterEnv for docker mode.
		Utils.setDockerEnv(flinkConfiguration, appMasterEnv);
		String realDockerImage =
			flinkConfiguration.getString(YarnConfigKeys.DOCKER_IMAGE_KEY, null);
		if (realDockerImage != null && !realDockerImage.trim().isEmpty()) {
			LOG.info("Add {}={} to dynamicPropertiesEncoded.", YarnConfigKeys.DOCKER_IMAGE_KEY,
				realDockerImage);
			if (dynamicPropertiesEncoded != null && !dynamicPropertiesEncoded.trim().isEmpty()) {
				dynamicPropertiesEncoded = String.format("%s@@%s=%s", dynamicPropertiesEncoded,
					YarnConfigKeys.DOCKER_IMAGE_KEY, realDockerImage);
			} else {
				dynamicPropertiesEncoded =
					String.format("%s=%s", YarnConfigKeys.DOCKER_IMAGE_KEY, realDockerImage);
			}
		}

		String name;
		if (customName == null) {
			int taskManagerCount = clusterSpecification.getNumberTaskManagers();
			name = "Flink session with " + taskManagerCount + " TaskManagers";
			if (detached) {
				name += " (detached)";
			}
		} else {
			name = customName;
		}

		if (name != null) {
			//Replace spaces by hyphen as space is difficult to handle in the subsequent process.
			appMasterEnv.put(YarnConfigKeys.ENV_FLINK_YARN_JOB, name.replace(" ", "-"));
		}
		if (this.yarnQueue != null) {
			appMasterEnv.put(YarnConfigKeys.ENV_FLINK_YARN_QUEUE, this.yarnQueue.replace(" ", "-"));
		}

		if (remotePathKeytab != null) {
			appMasterEnv.put(YarnConfigKeys.KEYTAB_PATH, remotePathKeytab.toString());
			String principal = configuration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL);
			appMasterEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, principal);
		}

		//To support Yarn Secure Integration Test Scenario
		if (remoteYarnSiteXmlPath != null) {
			appMasterEnv.put(YarnConfigKeys.ENV_YARN_SITE_XML_PATH, remoteYarnSiteXmlPath.toString());
		}
		if (remoteKrb5Path != null) {
			appMasterEnv.put(YarnConfigKeys.ENV_KRB5_PATH, remoteKrb5Path.toString());
		}

		if (dynamicPropertiesEncoded != null) {
			LOG.info("ADD {}={} to AM environment.", YarnConfigKeys.ENV_DYNAMIC_PROPERTIES,
				dynamicPropertiesEncoded);
			appMasterEnv.put(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES, dynamicPropertiesEncoded);
		}

		BtraceUtil.attachToEnv(appMasterEnv, null);

		// set classpath from YARN configuration
		Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);

		amContainer.setEnvironment(appMasterEnv);

		// Set up resource type requirements for ApplicationMaster
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(clusterSpecification.getMasterMemoryMB());
		capability.setVirtualCoresMilli(Utils.vCoresToMilliVcores(clusterSpecification.getMasterVcores()));
		LOG.info("jm cores = {}", clusterSpecification.getMasterVcores());

		final String appType = applicationType != null ?
			applicationType : YARN_STREAMING_APPLICATION_TYPE_DEFAULT;
		appContext.setApplicationName(name);
		appContext.setApplicationType(appType);
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);

		flinkConfiguration.setString(YARN_APPLICATION_TYPE, appType);
		if (!appType.equals(YARN_STREAMING_APPLICATION_TYPE_DEFAULT)) {
			flinkConfiguration.setBoolean(CHECK_JOB_UNIQUE, false);
		}

		// Set priority for application
		int priorityNum = flinkConfiguration.getInteger(YarnConfigOptions.APPLICATION_PRIORITY);
		if (priorityNum >= 0) {
			Priority priority = Priority.newInstance(priorityNum);
			appContext.setPriority(priority);
		}

		if (yarnQueue != null) {
			appContext.setQueue(yarnQueue);
		}

		setApplicationNodeLabel(appContext);

		setApplicationTags(appContext);

		// add a hook to clean up in case deployment fails
		Thread deploymentFailureHook = new DeploymentFailureHook(yarnClient, yarnApplication, yarnFilesDir);
		Runtime.getRuntime().addShutdownHook(deploymentFailureHook);
		LOG.info("Submitting application master " + appId);
		yarnClient.submitApplication(appContext);

		LOG.info("Waiting for the cluster to be allocated");
		final long startTime = System.currentTimeMillis();
		long preTime = startTime;
		ApplicationReport report;
		YarnApplicationState lastAppState = YarnApplicationState.NEW;
		loop: while (true) {
			try {
				report = yarnClient.getApplicationReport(appId);
			} catch (IOException e) {
				throw new YarnDeploymentException("Failed to deploy the cluster.", e);
			}
			YarnApplicationState appState = report.getYarnApplicationState();
			LOG.debug("Application State: {}", appState);
			switch(appState) {
				case FAILED:
				case FINISHED:
				case KILLED:
					throw new YarnDeploymentException("The YARN application unexpectedly switched to state "
						+ appState + " during deployment. \n" +
						"Diagnostics from YARN: " + report.getDiagnostics() + "\n" +
						"If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n" +
						"yarn logs -applicationId " + appId);
					//break ..
				case RUNNING:
					LOG.info("YARN application has been deployed successfully.");
					break loop;
				default:
					if (appState != lastAppState) {
						LOG.info("Deploying cluster, current state " + appState);
					}
					if (System.currentTimeMillis() - preTime > 60000) {
						preTime = System.currentTimeMillis();
						LOG.info("Deployment took more than {} seconds. Please check if the requested resources are available in the YARN cluster",
								(preTime - startTime) / 1000);
					}

			}
			lastAppState = appState;
			Thread.sleep(250);
		}
		// print the application id for user to cancel themselves.
		if (isDetachedMode()) {
			LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
					"Flink on YARN, use the following command or a YARN web interface to stop " +
					"it:\nyarn application -kill " + appId + "\nPlease also note that the " +
					"temporary files of the YARN session in the home directory will not be removed.");
		}
		// since deployment was successful, remove the hook
		ShutdownHookUtil.removeShutdownHook(deploymentFailureHook, getClass().getSimpleName(), LOG);
		return report;
	}

	private String getJobName () {
		try {
			int index = customName.lastIndexOf("_");
			String jobName = this.customName.substring(0, index);
			return jobName;
		} catch (Exception e) {
			LOG.warn("Failed to get job name from jar.");
		}
		return null;
	}


	/**
	 * Returns the Path where the YARN application files should be uploaded to.
	 *
	 * @param appId YARN application id
	 */
	private Path getYarnFilesDir(final ApplicationId appId) throws IOException {
//		final FileSystem fileSystem = FileSystem.get(yarnConfiguration);
//		final Path homeDir = fileSystem.getHomeDirectory();
//		return new Path(homeDir, ".flink/" + appId + '/');

		String jobWorkDir = flinkConfiguration.getString(ConfigConstants.JOB_WORK_DIR_KEY, ConfigConstants.PATH_JOB_WORK_FILE);
		Path yarnFilesDir = new Path(jobWorkDir, ".flink/" + appId + '/');
		return yarnFilesDir;
	}

	/**
	 * Uploads and registers a single resource and adds it to <tt>localResources</tt>.
	 *
	 * @param key
	 * 		the key to add the resource under
	 * @param fs
	 * 		the remote file system to upload to
	 * @param appId
	 * 		application ID
	 * @param localSrcPath
	 * 		local path to the file
	 * @param localResources
	 * 		map of resources
	 *
	 * @return the remote path to the uploaded resource
	 */
	private static Path setupSingleLocalResource(
			String key,
			FileSystem fs,
			ApplicationId appId,
			Path localSrcPath,
			Map<String, LocalResource> localResources,
			Path targetHomeDir,
			String relativeTargetPath) throws IOException, URISyntaxException {

		Tuple2<Path, LocalResource> resource = Utils.setupLocalResource(
			fs,
			appId.toString(),
			localSrcPath,
			targetHomeDir,
			relativeTargetPath);

		localResources.put(key, resource.f1);

		return resource.f0;
	}

	/**
	 * Recursively uploads (and registers) any (user and system) files in <tt>shipFiles</tt> except
	 * for files matching "<tt>flink-dist*.jar</tt>" which should be uploaded separately.
	 *
	 * @param shipFiles
	 * 		files to upload
	 * @param fs
	 * 		file system to upload to
	 * @param targetHomeDir
	 * 		remote home directory to upload to
	 * @param appId
	 * 		application ID
	 * @param remotePaths
	 * 		paths of the remote resources (uploaded resources will be added)
	 * @param localResources
	 * 		map of resources (uploaded resources will be added)
	 * @param envShipFileList
	 * 		list of shipped files in a format understood by {@link Utils#createTaskExecutorContext}
	 *
	 * @return list of class paths with the the proper resource keys from the registration
	 */
	static List<String> uploadAndRegisterFiles(
			Collection<File> shipFiles,
			FileSystem fs,
			Path targetHomeDir,
			ApplicationId appId,
			List<Path> remotePaths,
			Map<String, LocalResource> localResources,
			StringBuilder envShipFileList) throws IOException, URISyntaxException {

		final List<String> classPaths = new ArrayList<>(2 + shipFiles.size());
		for (File shipFile : shipFiles) {
			long beginUploadTime = System.currentTimeMillis();
			LOG.info("Begin to upload and register files = {}", shipFile.getAbsolutePath());
			if (shipFile.isDirectory()) {
				// add directories to the classpath
				java.nio.file.Path shipPath = shipFile.toPath();
				final java.nio.file.Path parentPath = shipPath.getParent();

				Files.walkFileTree(shipPath, new SimpleFileVisitor<java.nio.file.Path>() {
					@Override
					public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
						throws IOException {
						String fileName = file.getFileName().toString();
						if (!(fileName.startsWith("flink-dist") &&
								fileName.endsWith("jar"))) {

							java.nio.file.Path relativePath = parentPath.relativize(file);

							String key = relativePath.toString();
							try {
								Path remotePath = setupSingleLocalResource(
									key,
									fs,
									appId,
									new Path(file.toUri()),
									localResources,
									targetHomeDir,
									relativePath.getParent().toString());
								remotePaths.add(remotePath);
								envShipFileList.append(key).append("=")
									.append(remotePath).append(",");

								// add files to the classpath
								classPaths.add(key);
							} catch (URISyntaxException e) {
								throw new IOException(e);
							}
						}

						return FileVisitResult.CONTINUE;
					}
				});
			} else {
				if (!(shipFile.getName().startsWith("flink-dist") && shipFile.getName().endsWith("jar"))) {
					Path shipLocalPath = new Path(shipFile.toURI());
					String key = shipFile.getName();
					Path remotePath = setupSingleLocalResource(
						key, fs, appId, shipLocalPath, localResources, targetHomeDir, "");
					remotePaths.add(remotePath);
					envShipFileList.append(key).append("=").append(remotePath).append(",");

					// add files to the classpath
					classPaths.add(key);
				}
			}
			LOG.info("Success to upload and register files = {}, spent time = {} ms",
				shipFile.getAbsolutePath(), System.currentTimeMillis() - beginUploadTime);
		}
		return classPaths;
	}

	/**
	 * Kills YARN application and stops YARN client.
	 *
	 * <p>Use this method to kill the App before it has been properly deployed
	 */
	private void failSessionDuringDeployment(YarnClient yarnClient, YarnClientApplication yarnApplication) {
		LOG.info("Killing YARN application");

		try {
			yarnClient.killApplication(yarnApplication.getNewApplicationResponse().getApplicationId());
		} catch (Exception e) {
			// we only log a debug message here because the "killApplication" call is a best-effort
			// call (we don't know if the application has been deployed when the error occured).
			LOG.debug("Error while killing YARN application", e);
		}
		yarnClient.stop();
	}

	private static class ClusterResourceDescription {
		public final int totalFreeMemory;
		public final int containerLimit;
		public final int[] nodeManagersFree;

		public ClusterResourceDescription(int totalFreeMemory, int containerLimit, int[] nodeManagersFree) {
			this.totalFreeMemory = totalFreeMemory;
			this.containerLimit = containerLimit;
			this.nodeManagersFree = nodeManagersFree;
		}
	}

	@Override
	public String getClusterDescription() {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);

			YarnClusterMetrics metrics = yarnClient.getYarnClusterMetrics();

			ps.append("NodeManagers in the ClusterClient " + metrics.getNumNodeManagers());
			List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);
			final String format = "|%-16s |%-16s %n";
			ps.printf("|Property         |Value          %n");
			ps.println("+---------------------------------------+");
			int totalMemory = 0;
			int totalCores = 0;
			for (NodeReport rep : nodes) {
				final Resource res = rep.getCapability();
				totalMemory += res.getMemory();
				totalCores += res.getVirtualCores();
				ps.format(format, "NodeID", rep.getNodeId());
				ps.format(format, "Memory", res.getMemory() + " MB");
				ps.format(format, "vCores", res.getVirtualCores());
				ps.format(format, "HealthReport", rep.getHealthReport());
				ps.format(format, "Containers", rep.getNumContainers());
				ps.println("+---------------------------------------+");
			}
			ps.println("Summary: totalMemory " + totalMemory + " totalCores " + totalCores);
			List<QueueInfo> qInfo = yarnClient.getAllQueues();
			for (QueueInfo q : qInfo) {
				ps.println("Queue: " + q.getQueueName() + ", Current Capacity: " + q.getCurrentCapacity() + " Max Capacity: " +
					q.getMaximumCapacity() + " Applications: " + q.getApplications().size());
			}
			return baos.toString();
		} catch (Exception e) {
			throw new RuntimeException("Couldn't get cluster description", e);
		}
	}

	public void setName(String name) {
		this.customName = Preconditions.checkNotNull(name, "The customized name must not be null");
	}

	public void setApplicationType(String type) {
		this.applicationType = Preconditions.checkNotNull(type, "The customized application type must not be null");
	}

	private void activateHighAvailabilitySupport(ApplicationSubmissionContext appContext) throws
		InvocationTargetException, IllegalAccessException {

		ApplicationSubmissionContextReflector reflector = ApplicationSubmissionContextReflector.getInstance();

		reflector.setKeepContainersAcrossApplicationAttempts(appContext, true);

		reflector.setAttemptFailuresValidityInterval(
			appContext,
			flinkConfiguration.getLong(YarnConfigOptions.APPLICATION_ATTEMPT_FAILURE_VALIDITY_INTERVAL));
	}

	private void setApplicationTags(final ApplicationSubmissionContext appContext) throws InvocationTargetException,
		IllegalAccessException {

		final ApplicationSubmissionContextReflector reflector = ApplicationSubmissionContextReflector.getInstance();
		final String tagsString = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_TAGS);

		final Set<String> applicationTags = new HashSet<>();

		// Trim whitespace and cull empty tags
		for (final String tag : tagsString.split(",")) {
			final String trimmedTag = tag.trim();
			if (!trimmedTag.isEmpty()) {
				applicationTags.add(trimmedTag);
			}
		}

		reflector.setApplicationTags(appContext, applicationTags);
	}

	private void setApplicationNodeLabel(final ApplicationSubmissionContext appContext) throws InvocationTargetException,
		IllegalAccessException {

		if (nodeLabel != null) {
			final ApplicationSubmissionContextReflector reflector = ApplicationSubmissionContextReflector.getInstance();
			reflector.setApplicationNodeLabel(appContext, nodeLabel);
		}
	}

	/**
	 * Singleton object which uses reflection to determine whether the {@link ApplicationSubmissionContext}
	 * supports various methods which, depending on the Hadoop version, may or may not be supported.
	 *
	 * <p>If an unsupported method is invoked, nothing happens.
	 *
	 * <p>Currently three methods are proxied:
	 * - setApplicationTags (>= 2.4.0)
	 * - setAttemptFailuresValidityInterval (>= 2.6.0)
	 * - setKeepContainersAcrossApplicationAttempts (>= 2.4.0)
	 * - setNodeLabelExpression (>= 2.6.0)
	 */
	private static class ApplicationSubmissionContextReflector {
		private static final Logger LOG = LoggerFactory.getLogger(ApplicationSubmissionContextReflector.class);

		private static final ApplicationSubmissionContextReflector instance =
			new ApplicationSubmissionContextReflector(ApplicationSubmissionContext.class);

		public static ApplicationSubmissionContextReflector getInstance() {
			return instance;
		}

		private static final String APPLICATION_TAGS_METHOD_NAME = "setApplicationTags";
		private static final String ATTEMPT_FAILURES_METHOD_NAME = "setAttemptFailuresValidityInterval";
		private static final String KEEP_CONTAINERS_METHOD_NAME = "setKeepContainersAcrossApplicationAttempts";
		private static final String NODE_LABEL_EXPRESSION_NAME = "setNodeLabelExpression";

		private final Method applicationTagsMethod;
		private final Method attemptFailuresValidityIntervalMethod;
		private final Method keepContainersMethod;
		@Nullable
		private final Method nodeLabelExpressionMethod;

		private ApplicationSubmissionContextReflector(Class<ApplicationSubmissionContext> clazz) {
			Method applicationTagsMethod;
			Method attemptFailuresValidityIntervalMethod;
			Method keepContainersMethod;
			Method nodeLabelExpressionMethod;

			try {
				// this method is only supported by Hadoop 2.4.0 onwards
				applicationTagsMethod = clazz.getMethod(APPLICATION_TAGS_METHOD_NAME, Set.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), APPLICATION_TAGS_METHOD_NAME);
			} catch (NoSuchMethodException e) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), APPLICATION_TAGS_METHOD_NAME);
				// assign null because the Hadoop version apparently does not support this call.
				applicationTagsMethod = null;
			}

			this.applicationTagsMethod = applicationTagsMethod;

			try {
				// this method is only supported by Hadoop 2.6.0 onwards
				attemptFailuresValidityIntervalMethod = clazz.getMethod(ATTEMPT_FAILURES_METHOD_NAME, long.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), ATTEMPT_FAILURES_METHOD_NAME);
			} catch (NoSuchMethodException e) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), ATTEMPT_FAILURES_METHOD_NAME);
				// assign null because the Hadoop version apparently does not support this call.
				attemptFailuresValidityIntervalMethod = null;
			}

			this.attemptFailuresValidityIntervalMethod = attemptFailuresValidityIntervalMethod;

			try {
				// this method is only supported by Hadoop 2.4.0 onwards
				keepContainersMethod = clazz.getMethod(KEEP_CONTAINERS_METHOD_NAME, boolean.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), KEEP_CONTAINERS_METHOD_NAME);
			} catch (NoSuchMethodException e) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), KEEP_CONTAINERS_METHOD_NAME);
				// assign null because the Hadoop version apparently does not support this call.
				keepContainersMethod = null;
			}

			this.keepContainersMethod = keepContainersMethod;

			try {
				nodeLabelExpressionMethod = clazz.getMethod(NODE_LABEL_EXPRESSION_NAME, String.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), NODE_LABEL_EXPRESSION_NAME);
			} catch (NoSuchMethodException e) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), NODE_LABEL_EXPRESSION_NAME);
				nodeLabelExpressionMethod = null;
			}

			this.nodeLabelExpressionMethod = nodeLabelExpressionMethod;
		}

		public void setApplicationTags(
			ApplicationSubmissionContext appContext,
			Set<String> applicationTags) throws InvocationTargetException, IllegalAccessException {
			if (applicationTagsMethod != null) {
				LOG.debug("Calling method {} of {}.",
					applicationTagsMethod.getName(),
					appContext.getClass().getCanonicalName());
				applicationTagsMethod.invoke(appContext, applicationTags);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.",
					appContext.getClass().getCanonicalName(),
					APPLICATION_TAGS_METHOD_NAME);
			}
		}

		public void setApplicationNodeLabel(
			ApplicationSubmissionContext appContext,
			String nodeLabel) throws InvocationTargetException, IllegalAccessException {
			if (nodeLabelExpressionMethod != null) {
				LOG.debug("Calling method {} of {}.",
					nodeLabelExpressionMethod.getName(),
					appContext.getClass().getCanonicalName());
				nodeLabelExpressionMethod.invoke(appContext, nodeLabel);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.",
					appContext.getClass().getCanonicalName(),
					NODE_LABEL_EXPRESSION_NAME);
			}
		}

		public void setAttemptFailuresValidityInterval(
				ApplicationSubmissionContext appContext,
				long validityInterval) throws InvocationTargetException, IllegalAccessException {
			if (attemptFailuresValidityIntervalMethod != null) {
				LOG.debug("Calling method {} of {}.",
					attemptFailuresValidityIntervalMethod.getName(),
					appContext.getClass().getCanonicalName());
				attemptFailuresValidityIntervalMethod.invoke(appContext, validityInterval);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.",
					appContext.getClass().getCanonicalName(),
					ATTEMPT_FAILURES_METHOD_NAME);
			}
		}

		public void setKeepContainersAcrossApplicationAttempts(
			ApplicationSubmissionContext appContext,
			boolean keepContainers) throws InvocationTargetException, IllegalAccessException {

			if (keepContainersMethod != null) {
				LOG.debug("Calling method {} of {}.", keepContainersMethod.getName(),
					appContext.getClass().getCanonicalName());
				keepContainersMethod.invoke(appContext, keepContainers);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.",
					appContext.getClass().getCanonicalName(), KEEP_CONTAINERS_METHOD_NAME);
			}
		}
	}

	private static class YarnDeploymentException extends RuntimeException {
		private static final long serialVersionUID = -812040641215388943L;

		public YarnDeploymentException(String message) {
			super(message);
		}

		public YarnDeploymentException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	private class DeploymentFailureHook extends Thread {

		private final YarnClient yarnClient;
		private final YarnClientApplication yarnApplication;
		private final Path yarnFilesDir;

		DeploymentFailureHook(YarnClient yarnClient, YarnClientApplication yarnApplication, Path yarnFilesDir) {
			this.yarnClient = Preconditions.checkNotNull(yarnClient);
			this.yarnApplication = Preconditions.checkNotNull(yarnApplication);
			this.yarnFilesDir = Preconditions.checkNotNull(yarnFilesDir);
		}

		@Override
		public void run() {
			LOG.info("Cancelling deployment from Deployment Failure Hook");
			failSessionDuringDeployment(yarnClient, yarnApplication);
			LOG.info("Deleting files in {}.", yarnFilesDir);
			try {
				FileSystem fs = FileSystem.get(yarnConfiguration);

				if (fs.exists(yarnFilesDir) && !fs.delete(yarnFilesDir, true)) {
					throw new IOException("Deleting files in " + yarnFilesDir + " was unsuccessful");
				}

				fs.close();
			} catch (IOException e) {
				LOG.error("Failed to delete Flink Jar and configuration files in HDFS", e);
			}
		}
	}

	protected void addEnvironmentFoldersToShipFiles(Collection<File> effectiveShipFiles) {
		addLibFoldersToShipFiles(effectiveShipFiles);
		addPluginsFoldersToShipFiles(effectiveShipFiles);
	}

	private void addLibFoldersToShipFiles(Collection<File> effectiveShipFiles) {
		// Add lib folder to the ship files if the environment variable is set.
		// This is for convenience when running from the command-line.
		// (for other files users explicitly set the ship files)
		String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);
		if (libDir != null) {
			File directoryFile = new File(libDir);
			if (directoryFile.isDirectory()) {
				effectiveShipFiles.add(directoryFile);
			} else {
				throw new YarnDeploymentException("The environment variable '" + ENV_FLINK_LIB_DIR +
					"' is set to '" + libDir + "' but the directory doesn't exist.");
			}
		} else if (this.shipFiles.isEmpty()) {
			LOG.warn("Environment variable '{}' not set and ship files have not been provided manually. " +
				"Not shipping any library files.", ENV_FLINK_LIB_DIR);
		}
	}

	private void addPluginsFoldersToShipFiles(Collection<File> effectiveShipFiles) {
		String pluginsDir = System.getenv().get(ENV_FLINK_PLUGINS_DIR);
		if (pluginsDir != null) {
			File directoryFile = new File(pluginsDir);
			if (directoryFile.isDirectory()) {
				effectiveShipFiles.add(directoryFile);
			} else {
				LOG.warn("The environment variable '" + ENV_FLINK_PLUGINS_DIR +
					"' is set to '" + pluginsDir + "' but the directory doesn't exist.");
			}
		}
	}

	protected ContainerLaunchContext setupApplicationMasterContainer(
			String yarnClusterEntrypoint,
			boolean hasLogback,
			boolean hasLog4j,
			boolean hasKrb5,
			int jobManagerMemoryMb) {
		// ------------------ Prepare Application Master Container  ------------------------------

		// respect custom JVM options in the YAML file
		String javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS).length() > 0) {
			javaOpts += " " + flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
		}

		String logLevel = flinkConfiguration.getString(ConfigConstants.FLINK_LOG_LEVEL_KEY,
			ConfigConstants.FLINK_LOG_LEVEL_DEFAULT);
		javaOpts += " -Dlog.level=" + logLevel;

		String logLayout = flinkConfiguration.getString(ConfigConstants.FLINK_LOG_LAYOUT_KEY,
			ConfigConstants.FLINK_LOG_LAYOUT_DEFAULT);
		if (logLayout.length() > 1 && logLayout.charAt(0) == '\'' && logLayout.charAt(logLayout.length() - 1) == '\'') {
			logLayout = logLayout.substring(1, logLayout.length() - 1);
		}
		javaOpts += " -Dlog.layout=\\\"" + logLayout + "\\\"";

		// set databus channel
		String databusChannel = flinkConfiguration.getString(ConfigConstants.FLINK_LOG_DATABUS_CHANNEL_KEY,
			ConfigConstants.FLINK_LOG_DATABUS_CHANNEL_DEFAULT);
		javaOpts += " -Dlog.databus.channel=" + databusChannel;

		//applicable only for YarnMiniCluster secure test run
		//krb5.conf file will be available as local resource in JM/TM container
		if (hasKrb5) {
			javaOpts += " -Djava.security.krb5.conf=krb5.conf";
		}

		// Use G1GC
		if (flinkConfiguration.getBoolean(ConfigConstants.FLINK_GC_G1_KEY, ConfigConstants.FLINK_GC_G1_DEFAULT)) {
			javaOpts += " -XX:+UseG1GC";
			javaOpts += " -XX:MaxGCPauseMillis=" + flinkConfiguration.getInteger(
				ConfigConstants.FLINK_MAX_GC_PAUSE_MILLIS_KEY, ConfigConstants.FLINK_MAX_GC_PAUSE_MILLIS_DEFAULT);
		}

		// JVM GC log opts
		javaOpts += " " + flinkConfiguration.getString(ConfigConstants.FLINK_GC_LOG_OPTS_KEY,
			ConfigConstants.FLINK_GC_LOG_OPTS_DEFAULT);
		javaOpts += " " + "-Xloggc:" + flinkConfiguration.getString(ConfigConstants.FLINK_GC_LOG_FILE_KEY,
			ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/gc.log");
		javaOpts += " " + "-XX:ErrorFile=" + flinkConfiguration.getString(ConfigConstants.FLINK_JVM_ERROR_FILE_KEY,
				ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/hs_err_pid%p.log");

		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		final  Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");

		int heapSize = Utils.calculateHeapSize(jobManagerMemoryMb, flinkConfiguration);
		String jvmHeapMem = String.format("-Xms%sm -Xmx%sm", heapSize, heapSize);
		startCommandValues.put("jvmmem", jvmHeapMem);

		startCommandValues.put("jvmopts", javaOpts);
		String logging = "";

		boolean isInDockerMode =
			flinkConfiguration.getBoolean(YarnConfigKeys.IS_IN_DOCKER_MODE_KEY, false);
		boolean isDockerImageIncludeLib =
			flinkConfiguration.getBoolean(YarnConfigOptions.IS_DOCKER_INCLUDE_LIB);

		String log4jConfFile = CONFIG_FILE_LOG4J_NAME;
		String logbackConfFile = CONFIG_FILE_LOGBACK_NAME;
		if (isInDockerMode && isDockerImageIncludeLib) {
			String runtimeConfDir =
				flinkConfiguration.getString(ConfigConstants.FLINK_RUNTIME_CONF_DIR_KEY,
					ConfigConstants.FLINK_RUNTIME_CONF_DIR_DEFAULT);
			log4jConfFile = Paths.get(runtimeConfDir, CONFIG_FILE_LOG4J_NAME).
				toAbsolutePath().toString();
			logbackConfFile = Paths.get(runtimeConfDir, CONFIG_FILE_LOGBACK_NAME).
				toAbsolutePath().toString();
		}

		if (hasLogback || hasLog4j) {
			logging = "-Dlog.file=\"" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.log\"";

			if (hasLogback) {
				logging += " -Dlogback.configurationFile=file:" + logbackConfFile;
			}

			if (hasLog4j) {
				logging += " -Dlog4j.configuration=file:" + log4jConfFile;
			}
		}

		startCommandValues.put("logging", logging);
		startCommandValues.put("class", yarnClusterEntrypoint);
		startCommandValues.put("redirects",
			"1>> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.out " +
			"2>> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.err");
		startCommandValues.put("args", "");

		final String commandTemplate = flinkConfiguration
			.getString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
				ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE);
		final String amCommand =
			BootstrapTools.getStartCommand(commandTemplate, startCommandValues);

		amContainer.setCommands(Collections.singletonList(amCommand));

		LOG.debug("Application Master start command: " + amCommand);

		return amContainer;
	}

	private static YarnConfigOptions.UserJarInclusion getUserJarInclusionMode(org.apache.flink.configuration.Configuration config) {
		throwIfUserTriesToDisableUserJarInclusionInSystemClassPath(config);

		return config.getEnum(YarnConfigOptions.UserJarInclusion.class, YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR);
	}

	private static void throwIfUserTriesToDisableUserJarInclusionInSystemClassPath(final Configuration config) {
		final String userJarInclusion = config.getString(YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR);
		if ("DISABLED".equalsIgnoreCase(userJarInclusion)) {
			throw new IllegalArgumentException(String.format("Config option %s cannot be set to DISABLED anymore (see FLINK-11781)",
				YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR.key()));
		}
	}

	/**
	 * Creates a YarnClusterClient; may be overridden in tests.
	 */
	protected abstract ClusterClient<ApplicationId> createYarnClusterClient(
			AbstractYarnClusterDescriptor descriptor,
			int numberTaskManagers,
			int slotsPerTaskManager,
			ApplicationReport report,
			org.apache.flink.configuration.Configuration flinkConfiguration,
			boolean perJobCluster) throws Exception;
}

