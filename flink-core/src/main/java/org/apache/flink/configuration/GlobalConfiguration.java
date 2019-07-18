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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

/**
 * Global configuration object for Flink. Similar to Java properties configuration
 * objects it includes key-value pairs which represent the framework's configuration.
 */
@Internal
public final class GlobalConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

	public static final String FLINK_CONF_FILENAME = "flink-conf.yaml";

	// the keys whose values should be hidden
	private static final String[] SENSITIVE_KEYS = new String[] {"password", "secret"};

	// the hidden content to be displayed
	public static final String HIDDEN_CONTENT = "******";

	// the key of common configuration in yaml file.
	public static final String COMMON = "common";

	// --------------------------------------------------------------------------------------------

	private GlobalConfiguration() {}

	// --------------------------------------------------------------------------------------------

	/**
	 * Loads the global configuration from the environment. Fails if an error occurs during loading. Returns an
	 * empty configuration object if the environment variable is not set. In production this variable is set but
	 * tests and local execution/debugging don't have this environment variable set. That's why we should fail
	 * if it is not set.
	 * @return Returns the Configuration
	 */
	public static Configuration loadConfiguration() {
		return loadConfiguration(new Configuration());
	}

	/**
	 * Loads the global configuration and adds the given dynamic properties
	 * configuration.
	 *
	 * @param dynamicProperties The given dynamic properties
	 * @return Returns the loaded global configuration with dynamic properties
	 */
	public static Configuration loadConfiguration(Configuration dynamicProperties) {
		final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		if (configDir == null) {
			return new Configuration(dynamicProperties);
		}

		return loadConfiguration(configDir, dynamicProperties);
	}

	/**
	 * Loads the configuration files from the specified directory.
	 *
	 * <p>YAML files are supported as configuration files.
	 *
	 * @param configDir
	 *        the directory which contains the configuration files
	 */
	public static Configuration loadConfiguration(final String configDir) {
		return loadConfiguration(configDir, null);
	}

	/**
	 * Loads the configuration files from the specified directory. If the dynamic properties
	 * configuration is not null, then it is added to the loaded configuration.
	 *
	 * @param configDir directory to load the configuration from
	 * @param dynamicProperties configuration file containing the dynamic properties. Null if none.
	 * @return The configuration loaded from the given configuration directory
	 */
	public static Configuration loadConfiguration(final String configDir,
		@Nullable final Configuration dynamicProperties) {

		if (configDir == null) {
			throw new IllegalArgumentException("Given configuration directory is null, cannot load configuration");
		}

		final File confDirFile = new File(configDir);
		if (!(confDirFile.exists())) {
			throw new IllegalConfigurationException(
				"The given configuration directory name '" + configDir +
					"' (" + confDirFile.getAbsolutePath() + ") does not describe an existing directory.");
		}

		// get Flink yaml configuration file
		final File yamlConfigFile = new File(confDirFile, FLINK_CONF_FILENAME);

		if (!yamlConfigFile.exists()) {
			throw new IllegalConfigurationException(
				"The Flink config file '" + yamlConfigFile +
					"' (" + confDirFile.getAbsolutePath() + ") does not exist.");
		}

		Configuration configuration = loadYAMLResource(yamlConfigFile);

		if (dynamicProperties != null) {
			configuration.addAll(dynamicProperties);
		}

		return enrichWithEnvironmentVariables(configuration);
	}

	private static Configuration enrichWithEnvironmentVariables(Configuration configuration) {
		enrichWithEnvironmentVariable(ConfigConstants.ENV_FLINK_PLUGINS_DIR, configuration);
		return configuration;
	}

	private static void enrichWithEnvironmentVariable(String environmentVariable, Configuration configuration) {
		String pluginsDirFromEnv = System.getenv(environmentVariable);

		if (pluginsDirFromEnv == null) {
			return;
		}

		String pluginsDirFromConfig = configuration.getString(environmentVariable, pluginsDirFromEnv);

		if (!pluginsDirFromEnv.equals(pluginsDirFromConfig)) {
			throw new IllegalConfigurationException(
				"The given configuration file already contains a value (" + pluginsDirFromEnv +
					") for the key (" + environmentVariable +
					") that would have been overwritten with (" + pluginsDirFromConfig +
					") by an environment with the same name.");
		}

		configuration.setString(environmentVariable, pluginsDirFromEnv);
	}

	/**
	 * Loads a YAML-file of key-value pairs.
	 *
	 * <p>Colon and whitespace ": " separate key and value (one per line). The hash tag "#" starts a single-line comment.
	 *
	 * <p>Example:
	 *
	 * <pre>
	 * jobmanager.rpc.address: localhost # network address for communication with the job manager
	 * jobmanager.rpc.port   : 6123      # network port to connect to for communication with the job manager
	 * taskmanager.rpc.port  : 6122      # network port the task manager expects incoming IPC connections
	 * </pre>
	 *
	 * <p>This does not span the whole YAML specification, but only the *syntax* of simple YAML key-value pairs (see issue
	 * #113 on GitHub). If at any point in time, there is a need to go beyond simple key-value pairs syntax
	 * compatibility will allow to introduce a YAML parser library.
	 *
	 * @param file the YAML file to read from
	 * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
	 */
	private static Configuration loadYAMLResource(File file) {
		Configuration config = new Configuration();
		String clusterName = System.getProperty(ConfigConstants.CLUSTER_NAME_KEY,
			ConfigConstants.CLUSTER_NAME_DEFAULT);
		try {
			InputStream input = new FileInputStream(file);
			Yaml yaml = new Yaml();
			Map<String, Object> allYamlConf = (Map<String, Object>) yaml.load(input);
			Map<String, Object> commonConf = (Map<String, Object>) allYamlConf.get(COMMON);
			Map<String, Object> clusterConf = (Map<String, Object>) allYamlConf.get(clusterName);
			if (commonConf != null && clusterConf != null) {
				commonConf.putAll(clusterConf);
				allYamlConf = commonConf;
			}
			String[] dynamicParam = {ConfigConstants.CLUSTER_NAME_KEY,
				ConfigConstants.HDFS_PREFIX_KEY};
			for (Map.Entry<String, Object> entry : allYamlConf.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue().toString();
				for (String param : dynamicParam) {
					value = value.replace(String.format("${%s}", param),
						(String) allYamlConf.get(param));
				}
				config.setString(key, value);
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error parsing YAML configuration.", e);
		}

		return config;
	}

	/**
	 * Check whether the key is a hidden key.
	 *
	 * @param key the config key
	 */
	public static boolean isSensitive(String key) {
		Preconditions.checkNotNull(key, "key is null");
		final String keyInLower = key.toLowerCase();
		for (String hideKey : SENSITIVE_KEYS) {
			if (keyInLower.length() >= hideKey.length()
				&& keyInLower.contains(hideKey)) {
				return true;
			}
		}
		return false;
	}
}
