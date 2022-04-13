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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.monitor.utils.HttpUtil;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.runtime.util.IPv6Util;
import org.apache.flink.util.StringUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.btdsec.SecTokenIdentifier;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.BtraceUtil;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.NettyShuffleEnvironmentOptions.NETWORK_BLOCKING_SHUFFLE_TYPE;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR;

/**
 * Utility class that provides helper methods to work with Apache Hadoop YARN.
 */
public final class Utils {

	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	/** Keytab file name populated in YARN container. */
	public static final String DEFAULT_KEYTAB_FILE = "krb5.keytab";

	/** KRB5 file name populated in YARN container for secure IT run. */
	public static final String KRB5_FILE_NAME = "krb5.conf";

	/** Yarn site xml file name populated in YARN container for secure IT run. */
	public static final String YARN_SITE_FILE_NAME = "yarn-site.xml";

	/** Number of total retries to fetch the remote resources after uploaded in case of FileNotFoundException. */
	public static final int REMOTE_RESOURCES_FETCH_NUM_RETRY = 3;

	/** Time to wait in milliseconds between each remote resources fetch in case of FileNotFoundException. */
	public static final int REMOTE_RESOURCES_FETCH_WAIT_IN_MILLI = 100;

	/**
	 * Define the name of flink shuffle service, it has the following usages:
	 * (1) Configure shuffle service in NodeManger in yarn-site.xml
	 * (2) Suggest the auxiliary service name of shuffle service in NodeManger
	 * (3) Yarn(Session)ResourceManager need to configure its serviceData in
	 * 		ContainerLaunchContext so that flink shuffle service will get method
	 * 		initializeApplication() being invoked. Furthermore we can add more
	 * 		information through service data if we want to add authentication
	 * 		mechanism.
	 */
	public static final String YARN_SHUFFLE_SERVICE_NAME = "yarn_shuffle_service_for_flink";

	/** Initialize this variable at the first time containFlinkShuffleService() method been called. */
	private static Boolean containFlinkShuffleService = null;

	/** Default docker image used for docker on yarn. */
	private static final String DEFAULT_IMAGE = "default";

	/**
	 * Url path separator.
	 */
	private static final String URL_SEP = "/";

	public static void setupYarnClassPath(Configuration conf, Map<String, String> appMasterEnv) {
		addToEnvironment(
			appMasterEnv,
			Environment.CLASSPATH.name(),
			appMasterEnv.get(ENV_FLINK_CLASSPATH));
		String[] applicationClassPathEntries = conf.getStrings(
			YarnConfiguration.YARN_APPLICATION_CLASSPATH,
			YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
		for (String c : applicationClassPathEntries) {
			addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
		}
	}

	/**
	 * Deletes the YARN application files, e.g., Flink binaries, libraries, etc., from the remote
	 * filesystem.
	 *
	 * @param env The environment variables.
	 */
	public static void deleteApplicationFiles(final Map<String, String> env) {
		final String applicationFilesDir = env.get(YarnConfigKeys.FLINK_YARN_FILES);
		if (!StringUtils.isNullOrWhitespaceOnly(applicationFilesDir)) {
			final org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(applicationFilesDir);
			try {
				final org.apache.flink.core.fs.FileSystem fileSystem = path.getFileSystem();
				if (!fileSystem.delete(path, true)) {
					LOG.error("Deleting yarn application files under {} was unsuccessful.", applicationFilesDir);
				}
			} catch (final IOException e) {
				LOG.error("Could not properly delete yarn application files directory {}.", applicationFilesDir, e);
			}
		} else {
			LOG.debug("No yarn application files directory set. Therefore, cannot clean up the data.");
		}
	}

	/**
	 * Creates a YARN resource for the remote object at the given location.
	 *
	 * @param remoteRsrcPath	remote location of the resource
	 * @param resourceSize		size of the resource
	 * @param resourceModificationTime last modification time of the resource
	 *
	 * @return YARN resource
	 */
	static LocalResource registerLocalResource(
			Path remoteRsrcPath,
			long resourceSize,
			long resourceModificationTime,
			LocalResourceVisibility resourceVisibility) {
		LocalResource localResource = Records.newRecord(LocalResource.class);
		localResource.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
		localResource.setSize(resourceSize);
		localResource.setTimestamp(resourceModificationTime);
		localResource.setType(LocalResourceType.FILE);
		localResource.setVisibility(resourceVisibility);
		return localResource;
	}

	/**
	 * Creates a YARN resource for the remote object at the given location.
	 * @param fs remote filesystem
	 * @param remoteRsrcPath resource path to be registered
	 * @return YARN resource
	 */
	private static LocalResource registerLocalResource(FileSystem fs, Path remoteRsrcPath) throws IOException {
		FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
		return registerLocalResource(
			remoteRsrcPath,
			jarStat.getLen(),
			jarStat.getModificationTime(),
			LocalResourceVisibility.APPLICATION);
	}

	public static void setTokensFor(ContainerLaunchContext amContainer, List<Path> paths, Configuration conf) throws IOException {
		Credentials credentials = new Credentials();
		// for HDFS
		TokenCache.obtainTokensForNamenodes(credentials, paths.toArray(new Path[0]), conf);
		// for HBase
		obtainTokenForHBase(credentials, conf);
		// for user
		UserGroupInformation currUsr = UserGroupInformation.getCurrentUser();

		Collection<Token<? extends TokenIdentifier>> usrTok = currUsr.getTokens();
		for (Token<? extends TokenIdentifier> token : usrTok) {
			final Text id = new Text(token.getIdentifier());
			LOG.info("Adding user token " + id + " with " + token);
			credentials.addToken(id, token);
		}
		try (DataOutputBuffer dob = new DataOutputBuffer()) {
			credentials.writeTokenStorageToStream(dob);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Wrote tokens. Credentials buffer length: " + dob.getLength());
			}

			ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
			amContainer.setTokens(securityTokens);
		}
	}

	/**
	 * Obtain Kerberos security token for HBase.
	 */
	private static void obtainTokenForHBase(Credentials credentials, Configuration conf) throws IOException {
		if (UserGroupInformation.isSecurityEnabled()) {
			LOG.info("Attempting to obtain Kerberos security token for HBase");
			try {
				// ----
				// Intended call: HBaseConfiguration.addHbaseResources(conf);
				Class
						.forName("org.apache.hadoop.hbase.HBaseConfiguration")
						.getMethod("addHbaseResources", Configuration.class)
						.invoke(null, conf);
				// ----

				LOG.info("HBase security setting: {}", conf.get("hbase.security.authentication"));

				if (!"kerberos".equals(conf.get("hbase.security.authentication"))) {
					LOG.info("HBase has not been configured to use Kerberos.");
					return;
				}

				LOG.info("Obtaining Kerberos security token for HBase");
				// ----
				// Intended call: Token<AuthenticationTokenIdentifier> token = TokenUtil.obtainToken(conf);
				Token<?> token = (Token<?>) Class
						.forName("org.apache.hadoop.hbase.security.token.TokenUtil")
						.getMethod("obtainToken", Configuration.class)
						.invoke(null, conf);
				// ----

				if (token == null) {
					LOG.error("No Kerberos security token for HBase available");
					return;
				}

				credentials.addToken(token.getService(), token);
				LOG.info("Added HBase Kerberos security token to credentials.");
			} catch (ClassNotFoundException
					| NoSuchMethodException
					| IllegalAccessException
					| InvocationTargetException e) {
				LOG.info("HBase is not available (not packaged with this application): {} : \"{}\".",
						e.getClass().getSimpleName(), e.getMessage());
			}
		}
	}

	/**
	 * Copied method from org.apache.hadoop.yarn.util.Apps.
	 * It was broken by YARN-1824 (2.4.0) and fixed for 2.4.1
	 * by https://issues.apache.org/jira/browse/YARN-1931
	 */
	public static void addToEnvironment(Map<String, String> environment,
			String variable, String value) {
		String val = environment.get(variable);
		if (val == null) {
			val = value;
		} else {
			val = val + File.pathSeparator + value;
		}
		environment.put(StringInterner.weakIntern(variable),
				StringInterner.weakIntern(val));
	}

	/**
	 * Resolve keytab path either as absolute path or relative to working directory.
	 *
	 * @param workingDir current working directory
	 * @param keytabPath configured keytab path.
	 * @return resolved keytab path, or null if not found.
	 */
	public static String resolveKeytabPath(String workingDir, String keytabPath) {
		String keytab = null;
		if (keytabPath != null) {
			File f;
			f = new File(keytabPath);
			if (f.exists()) {
				keytab = f.getAbsolutePath();
				LOG.info("Resolved keytab path: {}", keytab);
			} else {
				// try using relative paths, this is the case when the keytab was shipped
				// as a local resource
				f = new File(workingDir, keytabPath);
				if (f.exists()) {
					keytab = f.getAbsolutePath();
					LOG.info("Resolved keytab path: {}", keytab);
				} else {
					LOG.warn("Could not resolve keytab path with: {}", keytabPath);
					keytab = null;
				}
			}
		}
		return keytab;
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private Utils() {
		throw new RuntimeException();
	}

	/**
	 * Creates the launch context, which describes how to bring up a TaskExecutor / TaskManager process in
	 * an allocated YARN container.
	 *
	 * <p>This code is extremely YARN specific and registers all the resources that the TaskExecutor
	 * needs (such as JAR file, config file, ...) and all environment variables in a YARN
	 * container launch context. The launch context then ensures that those resources will be
	 * copied into the containers transient working directory.
	 *
	 * @param flinkConfig
	 *		 The Flink configuration object.
	 * @param yarnConfig
	 *		 The YARN configuration object.
	 * @param env
	 *		 The environment variables.
	 * @param tmParams
	 *		 The TaskExecutor container memory parameters.
	 * @param taskManagerDynamicProperties
	 *		 The dynamic configurations to be updated for the TaskExecutors based on client uploaded Flink config.
	 * @param workingDirectory
	 *		 The current application master container's working directory.
	 * @param taskManagerMainClass
	 *		 The class with the main method.
	 * @param log
	 *		 The logger.
	 *
	 * @return The launch context for the TaskManager processes.
	 *
	 * @throws Exception Thrown if the launch context could not be created, for example if
	 *				   the resources could not be copied.
	 */
	static ContainerLaunchContext createTaskExecutorContext(
		org.apache.flink.configuration.Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		Map<String, String> env,
		ContaineredTaskManagerParameters tmParams,
		String taskManagerDynamicProperties,
		String workingDirectory,
		Class<?> taskManagerMainClass,
		Logger log) throws Exception {

		// get and validate all relevant variables

		String remoteFlinkJarPath = env.get(YarnConfigKeys.FLINK_DIST_JAR);
		require(remoteFlinkJarPath != null, "Environment variable %s not set", YarnConfigKeys.FLINK_DIST_JAR);

		String appId = env.get(YarnConfigKeys.ENV_APP_ID);
		require(appId != null, "Environment variable %s not set", YarnConfigKeys.ENV_APP_ID);

		String clientHomeDir = env.get(YarnConfigKeys.ENV_CLIENT_HOME_DIR);
		require(clientHomeDir != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_HOME_DIR);

		String shipListString = env.get(YarnConfigKeys.ENV_CLIENT_SHIP_FILES);
		require(shipListString != null, "Environment variable %s not set", YarnConfigKeys.ENV_CLIENT_SHIP_FILES);

		String yarnClientUsername = env.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		require(yarnClientUsername != null, "Environment variable %s not set", YarnConfigKeys.ENV_HADOOP_USER_NAME);

		final String remoteKeytabPath = env.get(YarnConfigKeys.REMOTE_KEYTAB_PATH);
		final String localKeytabPath = env.get(YarnConfigKeys.LOCAL_KEYTAB_PATH);
		final String keytabPrincipal = env.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		final String remoteYarnConfPath = env.get(YarnConfigKeys.ENV_YARN_SITE_XML_PATH);
		final String remoteKrb5Path = env.get(YarnConfigKeys.ENV_KRB5_PATH);

		if (log.isDebugEnabled()) {
			log.debug("TM:remote keytab path obtained {}", remoteKeytabPath);
			log.debug("TM:local keytab path obtained {}", localKeytabPath);
			log.debug("TM:keytab principal obtained {}", keytabPrincipal);
			log.debug("TM:remote yarn conf path obtained {}", remoteYarnConfPath);
			log.debug("TM:remote krb5 path obtained {}", remoteKrb5Path);
		}

		String classPathString = env.get(ENV_FLINK_CLASSPATH);
		require(classPathString != null, "Environment variable %s not set", YarnConfigKeys.ENV_FLINK_CLASSPATH);

		//register keytab
		LocalResource keytabResource = null;
		if (remoteKeytabPath != null) {
			log.info("Adding keytab {} to the AM container local resource bucket", remoteKeytabPath);
			Path keytabPath = new Path(remoteKeytabPath);
			FileSystem fs = keytabPath.getFileSystem(yarnConfig);
			keytabResource = registerLocalResource(fs, keytabPath);
		}

		//To support Yarn Secure Integration Test Scenario
		LocalResource yarnConfResource = null;
		LocalResource krb5ConfResource = null;
		boolean hasKrb5 = false;
		if (remoteYarnConfPath != null) {
			log.info("TM:Adding remoteYarnConfPath {} to the container local resource bucket", remoteYarnConfPath);
			Path yarnConfPath = new Path(remoteYarnConfPath);
			FileSystem fs = yarnConfPath.getFileSystem(yarnConfig);
			yarnConfResource = registerLocalResource(fs, yarnConfPath);
		}

		if (remoteKrb5Path != null) {
			log.info("TM:Adding remoteKrb5Path {} to the container local resource bucket", remoteKrb5Path);
			Path krb5ConfPath = new Path(remoteKrb5Path);
			FileSystem fs = krb5ConfPath.getFileSystem(yarnConfig);
			krb5ConfResource = registerLocalResource(fs, krb5ConfPath);
			hasKrb5 = true;
		}

		Map<String, LocalResource> taskManagerLocalResources = new HashMap<>();

		// register Flink Jar with remote HDFS
		final YarnLocalResourceDescriptor flinkDistLocalResourceDesc =
				YarnLocalResourceDescriptor.fromString(remoteFlinkJarPath);
		taskManagerLocalResources.put(
				flinkDistLocalResourceDesc.getResourceKey(),
				flinkDistLocalResourceDesc.toLocalResource());

		//To support Yarn Secure Integration Test Scenario
		if (yarnConfResource != null) {
			taskManagerLocalResources.put(YARN_SITE_FILE_NAME, yarnConfResource);
		}
		if (krb5ConfResource != null) {
			taskManagerLocalResources.put(KRB5_FILE_NAME, krb5ConfResource);
		}
		if (keytabResource != null) {
			taskManagerLocalResources.put(localKeytabPath, keytabResource);
		}

		// prepare additional files to be shipped
		decodeYarnLocalResourceDescriptorListFromString(shipListString).forEach(
			resourceDesc -> taskManagerLocalResources.put(resourceDesc.getResourceKey(), resourceDesc.toLocalResource()));

		// now that all resources are prepared, we can create the launch context

		log.info("Creating container launch context for TaskManagers");

		boolean hasLogback = new File(workingDirectory, "logback.xml").exists();
		boolean hasLog4j = new File(workingDirectory, "log4j.properties").exists();

		String launchCommand = BootstrapTools.getTaskManagerShellCommand(
				flinkConfig, tmParams, ".", ApplicationConstants.LOG_DIR_EXPANSION_VAR,
				hasLogback, hasLog4j, hasKrb5, taskManagerMainClass, taskManagerDynamicProperties);

		if (log.isDebugEnabled()) {
			log.debug("Starting TaskManagers with command: " + launchCommand);
		} else {
			log.info("Starting TaskManagers");
		}

		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		ctx.setCommands(Collections.singletonList(launchCommand));
		ctx.setLocalResources(taskManagerLocalResources);

		Map<String, String> containerEnv = new HashMap<>();

		// put applicationId
		containerEnv.put(YarnConfigKeys.ENV_APP_ID, appId);

		final String subpartitionType = flinkConfig.getString(NETWORK_BLOCKING_SHUFFLE_TYPE);
		if (subpartitionType.equals("yarn")) {
			ctx.setServiceData(Collections.singletonMap(YARN_SHUFFLE_SERVICE_NAME, ByteBuffer.allocate(0)));
		}
		containerEnv.put(YarnConfigKeys.ENV_SEC_KV_AUTH, "1");
		containerEnv.putAll(tmParams.taskManagerEnv());

		// add YARN classpath, etc to the container environment
		containerEnv.put(ENV_FLINK_CLASSPATH, classPathString);
		setupYarnClassPath(yarnConfig, containerEnv);

		containerEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

		containerEnv.put(YarnConfigKeys.ENV_FLINK_YARN_QUEUE, env.get(YarnConfigKeys.ENV_FLINK_YARN_QUEUE));
		containerEnv.put(YarnConfigKeys.ENV_FLINK_YARN_CLUSTER, env.get(YarnConfigKeys.ENV_FLINK_YARN_CLUSTER));
		containerEnv.put(YarnConfigKeys.ENV_FLINK_YARN_JOB, env.get(YarnConfigKeys.ENV_FLINK_YARN_JOB));
		if (env.containsKey(YarnConfigKeys.ENV_FLINK_YARN_DC)) {
			containerEnv.put(YarnConfigKeys.ENV_FLINK_YARN_DC, env.get(YarnConfigKeys.ENV_FLINK_YARN_DC));
		}

		if (remoteKeytabPath != null && localKeytabPath != null && keytabPrincipal != null) {
			containerEnv.put(YarnConfigKeys.REMOTE_KEYTAB_PATH, remoteKeytabPath);
			containerEnv.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, localKeytabPath);
			containerEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, keytabPrincipal);
		} else if (localKeytabPath != null && keytabPrincipal != null) {
			containerEnv.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, localKeytabPath);
			containerEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, keytabPrincipal);
		}

		setIpv6Env(flinkConfig, containerEnv);
		Utils.setHdfsBtrace(flinkConfig, containerEnv);
		BtraceUtil.attachToEnv(containerEnv, null);

		if (env.containsKey(YarnConfigKeys.ENV_LOAD_SERVICE_PSM)) {
			containerEnv.put(YarnConfigKeys.ENV_LOAD_SERVICE_PSM, env.get(YarnConfigKeys.ENV_LOAD_SERVICE_PSM));
		}

		String partitionList = env.get(ConfigConstants.PARTITION_LIST_KEY);
		if (partitionList != null && !partitionList.isEmpty()) {
			containerEnv.put(ConfigConstants.PARTITION_LIST_KEY, partitionList);
			LOG.info("Set {} in container environment = {}",
				ConfigConstants.PARTITION_LIST_KEY, partitionList);
		}

		String brokerQueueList = env.get(ConfigConstants.ROCKETMQ_BROKER_QUEUE_LIST_KEY);
		if (brokerQueueList != null && !brokerQueueList.isEmpty()) {
			containerEnv.put(ConfigConstants.ROCKETMQ_BROKER_QUEUE_LIST_KEY, brokerQueueList);
			LOG.info("Set {} in container environment = {}",
				ConfigConstants.ROCKETMQ_BROKER_QUEUE_LIST_KEY, brokerQueueList);
		}

		// Add environment params to TM appMasterEnv for docker mode.
		setDockerEnv(flinkConfig, containerEnv);

		ctx.setEnvironment(containerEnv);

		// For TaskManager YARN container context, read the tokens from the jobmanager yarn container local file.
		// NOTE: must read the tokens from the local file, not from the UGI context, because if UGI is login
		// using Kerberos keytabs, there is no HDFS delegation token in the UGI context.
		final String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);

		boolean setTaskManagerToken = flinkConfig.getBoolean(YarnConfigOptions.SET_TASK_MANAGER_TOKEN);

		if (setTaskManagerToken && fileLocation != null) {
			log.debug("Adding security tokens to TaskExecutor's container launch context.");

			try (DataOutputBuffer dob = new DataOutputBuffer()) {
				Method readTokenStorageFileMethod = Credentials.class.getMethod(
					"readTokenStorageFile", File.class, org.apache.hadoop.conf.Configuration.class);

				Credentials cred =
					(Credentials) readTokenStorageFileMethod.invoke(
						null,
						new File(fileLocation),
						HadoopUtils.getHadoopConfiguration(flinkConfig));

				// Filter out AMRMToken before setting the tokens to the TaskManager container context.
				Credentials taskManagerCred = new Credentials();
				Collection<Token<? extends TokenIdentifier>> userTokens = cred.getAllTokens();
				for (Token<? extends TokenIdentifier> token : userTokens) {
					if (!token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
						if (token.getKind().equals(SecTokenIdentifier.KIND_NAME)) {
							// SecToken should use service as key, otherwise it can't be updated.
							taskManagerCred.addToken(token.getService(), token);
						} else {
							final Text id = new Text(token.getIdentifier());
							taskManagerCred.addToken(id, token);
						}
					}
				}

				taskManagerCred.writeTokenStorageToStream(dob);
				ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
				ctx.setTokens(securityTokens);
			} catch (Throwable t) {
				log.error("Failed to add Hadoop's security tokens.", t);
			}
		} else if (!setTaskManagerToken){
			log.info("No need to set token for TaskManager.");
		} else {
			log.info("Could not set security tokens because Hadoop's token file location is unknown.");
		}

		return ctx;
	}

	static boolean isRemotePath(String path) throws IOException {
		org.apache.flink.core.fs.Path flinkPath = new org.apache.flink.core.fs.Path(path);
		return flinkPath.getFileSystem().isDistributedFS();
	}

	public static void setHdfsBtrace(
			org.apache.flink.configuration.Configuration flinkConfiguration,
			Map<String, String> env) {
		final String btracePlatform = flinkConfiguration.getString(ConfigConstants.HDFS_BTRACE_PLATFORM, ConfigConstants.HDFS_BTRACE_PLATFORM_DEFAULT);
		env.put(ConfigConstants.HDFS_BTRACE_TAGS_KEY, String.format(ConfigConstants.HDFS_BTRACE_TAGS_VALUE, btracePlatform));
	}

	public static void setIpv6Env(
			org.apache.flink.configuration.Configuration flinkConfiguration,
			Map<String, String> env) {
		if (!IPv6Util.ipv6Enabled(flinkConfiguration)) {
			// unset the ipv6 environments when disable ipv6.
			String oldUnsetEnvs = env.get(ConfigConstants.ENV_RUNTIME_UNSET_YARN);
			String newUnsetEnvs;
			if (!StringUtils.isNullOrWhitespaceOnly(oldUnsetEnvs)) {
				newUnsetEnvs = oldUnsetEnvs + ";" + ConfigConstants.ENV_BYTED_IPV6_SUPPORT + ";" + ConfigConstants.ENV_MY_IPV6_SUPPORT;
			} else {
				newUnsetEnvs = ConfigConstants.ENV_BYTED_IPV6_SUPPORT + ";" + ConfigConstants.ENV_MY_IPV6_SUPPORT;
			}
			env.put(ConfigConstants.ENV_RUNTIME_UNSET_YARN, newUnsetEnvs);
		}
	}

	private static List<YarnLocalResourceDescriptor> decodeYarnLocalResourceDescriptorListFromString(String resources) throws Exception {
		final List<YarnLocalResourceDescriptor> resourceDescriptors = new ArrayList<>();
		for (String shipResourceDescStr : resources.split(LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR)) {
			if (!shipResourceDescStr.isEmpty()) {
				resourceDescriptors.add(YarnLocalResourceDescriptor.fromString(shipResourceDescStr));
			}
		}
		return resourceDescriptors;
	}

	/**
	 * Validates a condition, throwing a RuntimeException if the condition is violated.
	 *
	 * @param condition The condition.
	 * @param message The message for the runtime exception, with format variables as defined by
	 *                {@link String#format(String, Object...)}.
	 * @param values The format arguments.
	 */
	static void require(boolean condition, String message, Object... values) {
		if (!condition) {
			throw new RuntimeException(String.format(message, values));
		}
	}

	public static long vCoresToMilliVcores(double vCores) {
		return (long) (vCores * 1000);
	}

	/**
	 * check yarn startup flink yarn shuffle service or not.
	 */
	private static Boolean containFlinkShuffleServiceInternal() {
		String services = new YarnConfiguration(new org.apache.hadoop.conf.Configuration()).get(
				"yarn.nodemanager.aux-services", "");
		Boolean ret = false;
		if (services.contains(YARN_SHUFFLE_SERVICE_NAME)) {
			ret = true;
		}
		return ret;
	}

	public static boolean containFlinkShuffleService() {
		if (containFlinkShuffleService != null) {
			return containFlinkShuffleService;
		}
		synchronized (Utils.class) {
			if (containFlinkShuffleService != null) {
				return containFlinkShuffleService;
			}
			// initialize for only once
			containFlinkShuffleService = containFlinkShuffleServiceInternal();
			return containFlinkShuffleService;
		}
	}

	public static String getEnvOrUnknown(String key) {
		String value = System.getenv(key);
		if (value == null) {
			LOG.warn("get {} from env failed, fallback to unknown.", key);
			value = "unknown";
		}
		return value;
	}

	public static String getYarnHostname() {
		String hostname =  System.getenv(ApplicationConstants.Environment.NM_HOST.key());
		if (hostname == null) {
			try {
				LOG.info("Get hostname from yarn env failed try to get local.");
				hostname = InetAddress.getLocalHost().getCanonicalHostName();
			} catch (UnknownHostException e) {
				hostname = "noHostName";
			}
		}
		return hostname;
	}

	public static String getCurrentContainerID() {
		String containerID = System.getenv(Environment.CONTAINER_ID.key());
		if (containerID == null) {
			LOG.info("Get container id from yarn env failed.");
			containerID = "noContainerID";
		}
		return containerID;
	}

	public static void updateYarnConfigForClient(
			final YarnConfiguration yarnConfiguration,
			final org.apache.flink.configuration.Configuration flinkConfig) {
		updateYarnConfig(yarnConfiguration, flinkConfig, YarnConfigOptions.YARN_CONFIG_KEY_PREFIX);
		updateYarnConfig(yarnConfiguration, flinkConfig, YarnConfigOptions.CLIENT_YARN_CONFIG_KEY_PREFIX);
	}

	public static void updateYarnConfigForJobManager(
			final YarnConfiguration yarnConfiguration,
			final org.apache.flink.configuration.Configuration flinkConfig) {
		updateYarnConfig(yarnConfiguration, flinkConfig, YarnConfigOptions.YARN_CONFIG_KEY_PREFIX);
		updateYarnConfig(yarnConfiguration, flinkConfig, YarnConfigOptions.JOBMANAGER_YARN_CONFIG_KEY_PREFIX);
	}

	public static void updateYarnConfig(
			final YarnConfiguration yarnConfiguration,
			final org.apache.flink.configuration.Configuration flinkConfig,
			final String configPrefix) {

		for (Map.Entry<String, String> entry: flinkConfig.toMap().entrySet()) {
			if (entry.getKey().startsWith(configPrefix) && entry.getKey().length() > configPrefix.length()) {
				// remove prefix
				String key = entry.getKey().substring(configPrefix.length());
				yarnConfiguration.set(key, entry.getValue());
				LOG.info("update yarn config set {} to {}.", key, entry.getValue());
			}
		}
	}

	/*
	 * container_e496_1572589496647_25097_01_000006 = container_{epoch}_{applicationId}_{applicationAttempId}_{containerId}
	 */
	public static String pruneContainerId(String containerId) {
		if (!StringUtils.isNullOrWhitespaceOnly(containerId)) {
			String[] containerIdParts = containerId.split("_");
			if (containerIdParts.length == 6) {
				return containerIdParts[5];
			}
			return containerId;
		}
		return containerId;
	}

	/**
	 * If docker image id is configured in flinkConfiguration, add docker environment parameters
	 * to environment map.
	 *
	 * @param flinkConfiguration flink configuration.
	 * @param envMap             environment map.
	 */
	public static void setDockerEnv(org.apache.flink.configuration.Configuration flinkConfiguration,
									Map<String, String> envMap) {
		try {
			String dockerImage = flinkConfiguration.getString(YarnConfigOptions.DOCKER_IMAGE);
			boolean dockerEnabled = flinkConfiguration.getBoolean(YarnConfigOptions.DOCKER_ENABLED);
			if (!dockerEnabled) {
				LOG.info("Deactivate docker image, run on physical machines.");
				return;
			} else if (StringUtils.isNullOrWhitespaceOnly(dockerImage)) {
				LOG.info("No docker image configured, run on physical machines.");
				return;
			}

			LOG.info("Docker image: {} has been configured, set docker environment params.",
				dockerImage);
			dockerImage = getDockerImage(dockerImage, flinkConfiguration);
			if (StringUtils.isNullOrWhitespaceOnly(dockerImage)) {
				throw new RuntimeException("The real docker image is empty, please check config " + YarnConfigOptions.DOCKER_IMAGE.key());
			}
			flinkConfiguration.setString(YarnConfigOptions.DOCKER_IMAGE, dockerImage);
			LOG.info("The real docker image id is: {}", dockerImage);
			envMap.put(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_KEY, dockerImage);
			envMap.put(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_TYPE_KEY,
				YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_TYPE_DEFAULT);
			boolean enableDefaultMount = flinkConfiguration.getBoolean(YarnConfigOptions.DOCKER_MOUNTS_DEFAULT_ENABLE);
			List<String> dockerMountList = new ArrayList<>();
			String otherDockerMounts = flinkConfiguration.getString(YarnConfigOptions.DOCKER_MOUNTS);

			if (!StringUtils.isNullOrWhitespaceOnly(otherDockerMounts)) {
				dockerMountList.add(otherDockerMounts);
			}
			if (enableDefaultMount) {
				dockerMountList.add(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS_DEFAULT);
			}
			String dockerMounts = String.join(";", dockerMountList);
			LOG.info("Docker mounts: {}, default enable: {}", dockerMounts, enableDefaultMount);
			if (!StringUtils.isNullOrWhitespaceOnly(dockerMounts)) {
				envMap.put(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS_KEY, dockerMounts);
			}
			envMap.put(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_CAP_ADD_KEY,
				YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_CAP_ADD_DEFAULT);
			String dockerLogMounts = YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_LOG_MOUNTS_DEFAULT;
			String otherDockerLogMounts = flinkConfiguration.getString(YarnConfigOptions.DOCKER_LOG_MOUNTS);
			if (otherDockerLogMounts != null && !otherDockerLogMounts.isEmpty()) {
				dockerLogMounts = otherDockerLogMounts + ";" + dockerLogMounts;
			}
			LOG.info("Docker log mounts: {}", dockerLogMounts);
			envMap.put(YarnConfigKeys.ENV_YARN_CONTAINER_RUNTIME_DOCKER_LOG_MOUNTS_KEY, dockerLogMounts);
		} catch (IOException e) {
			LOG.error("ERROR occurred while get real docker image", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Add docker hub before image and replace 'latest' docker version with the real version.
	 *
	 * @param flinkConfiguration flink configuration.
	 * @return the real docker version.
	 */
	public static String getDockerImage(String dockerImage,
										org.apache.flink.configuration.Configuration flinkConfiguration) throws IOException {
		if (StringUtils.isNullOrWhitespaceOnly(dockerImage)) {
			return null;
		}

		if (DEFAULT_IMAGE.equalsIgnoreCase(dockerImage)) {
			dockerImage = flinkConfiguration.getString(YarnConfigOptions.DOCKER_DEFAULT_IMAGE);
		}

		String dockerHub = flinkConfiguration.getString(YarnConfigOptions.DOCKER_HUB);

		String[] items = dockerImage.trim().split(":");
		if (items.length != 2) {
			throw new RuntimeException("docker.image must be {psm}:{version}.");
		}
		String dockerPsm = items[0].trim();
		String dockerVersion = items[1].trim();

		if (!YarnConfigKeys.DOCKER_VERSION_LATEST.equalsIgnoreCase(dockerVersion)) {
			if (flinkConfiguration.containsKey(YarnConfigKeys.DOCKER_NAMESPACE_KEY)) {
				String dockerNamespace = flinkConfiguration.getString(YarnConfigOptions.DOCKER_NAMESPACE);
				if (!dockerImage.startsWith(dockerHub + URL_SEP + dockerNamespace)) {
					dockerImage = dockerHub + URL_SEP + dockerNamespace + URL_SEP + dockerImage;
				}
			} else {
				if (!dockerImage.startsWith(dockerHub)) {
					dockerImage = dockerHub + URL_SEP + dockerImage;
				}
			}

			return dockerImage;
		}
		LOG.info("Replace 'latest' version with real latest version id.");

		String dockerServer = flinkConfiguration.getString(YarnConfigOptions.DOCKER_SERVER);
		String dockerRegion = flinkConfiguration.getString(YarnConfigOptions.DOCKER_REGION);
		String dockerAuthorization = flinkConfiguration.getString(YarnConfigOptions.DOCKER_AUTHORIZATION);
		String dockerUrlTemplate = flinkConfiguration.getString(YarnConfigOptions.DOCKER_VERSION_URL_TEMPLATE);
		String dockerUrl = String.format(dockerUrlTemplate, dockerServer, dockerPsm, dockerRegion);
		Map<String, String> headers = new HashMap<>();
		headers.put(YarnConfigKeys.DOCKER_HTTP_HEADER_AUTHORIZATION_KEY, dockerAuthorization);
		HttpUtil.HttpResponsePojo response = HttpUtil.sendGet(dockerUrl, headers);
		String content = response.getContent();
		JsonNode respJson = new ObjectMapper().readTree(content);
		String image = respJson.hasNonNull(dockerRegion) ? respJson.get(dockerRegion).asText() : null;
		LOG.info("Get image from {}, image is: {}", dockerUrl, image);
		return image;
	}
}
