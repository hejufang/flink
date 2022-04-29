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
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Global configuration object for Flink. Similar to Java properties configuration
 * objects it includes key-value pairs which represent the framework's configuration.
 */
@Internal
public final class GlobalConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

	public static final String FLINK_CONF_FILENAME = "flink-conf.yaml";

	// the keys whose values should be hidden
	private static final String[] SENSITIVE_KEYS = new String[] {"password", "secret", "fs.azure.account.key"};

	// the hidden content to be displayed
	public static final String HIDDEN_CONTENT = "******";

	// the key of common configuration in yaml file.
	public static final String COMMON = "common";

	// the key of flexible origin value in yaml file.
	public static final String ORIGIN_KEY_POSTFIX = "#ORIGIN#";

	private static final String[] DYNAMIC_PARAM_KEYS = {
			ConfigConstants.CLUSTER_NAME_KEY,
			ConfigConstants.HDFS_PREFIX_KEY,
			ConfigConstants.DC_KEY,
			ConfigConstants.CHECKPOINT_HDFS_PREFIX_KEY};

	private static final String[] ENV_PARAM_KEYS = {
			ConfigConstants.ENV_FLINK_HOME_DIR};

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
	public static Configuration loadConfiguration(final String configDir, @Nullable final Configuration dynamicProperties) {

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

		return configuration;
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
			if (allYamlConf == null) {
				LOG.warn("Configuration in file {} is Null.", file.getAbsolutePath());
				return config;
			}
			Map<String, Object> commonConf = (Map<String, Object>) allYamlConf.get(COMMON);
			Map<String, Object> clusterConf = (Map<String, Object>) allYamlConf.get(clusterName);
			if (commonConf != null) {
				allYamlConf = commonConf;
			}
			if (clusterConf != null) {
				allYamlConf.putAll(clusterConf);
			}
			for (Map.Entry<String, Object> entry : allYamlConf.entrySet()) {
				String key = entry.getKey();
				if (isWithOriginPostfix(key)) {
					// OriginKey can not be replaced.
					continue;
				}
				String value = entry.getValue().toString();
				String valueOld = value;
				for (String param : DYNAMIC_PARAM_KEYS) {
					String paramKey = String.format("${%s}", param);
					if (value.contains(paramKey)) {
						value = value.replace(paramKey, (String) allYamlConf.get(param));
					}
				}
				for (String param : ENV_PARAM_KEYS) {
					String paramKey = String.format("${%s}", param);
					String paramValue = System.getenv(param);
					if (value.contains(paramKey) && !StringUtils.isNullOrWhitespaceOnly(paramValue)) {
						value = value.replace(paramKey, paramValue);
					}
				}
				config.setString(key, value);

				// save OriginKey with value.
				if (!value.equals(valueOld)) {
					config.setString(appendOriginPostfixTo(key), valueOld);
				}
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error parsing YAML configuration.", e);
		}

		return config;
	}

	private static String appendOriginPostfixTo(String key) {
		return key + ORIGIN_KEY_POSTFIX;
	}

	private static String trimOriginPostfixFrom(String key) {
		return key.substring(0, key.length() - ORIGIN_KEY_POSTFIX.length());
	}

	private static boolean isWithOriginPostfix(String key) {
		return key.endsWith(ORIGIN_KEY_POSTFIX);
	}

	/**
	 * Reload config with dynamic properties.
	 * @param config old Configuration.
	 * @param properties dynamic properties.
	 */
	public static void reloadConfigWithDynamicProperties(Configuration config, Properties properties) {
		List<String> dynamicParamKeys = new ArrayList<>(Arrays.asList(DYNAMIC_PARAM_KEYS));
		for (String key : config.keySet()) {
			if (isWithOriginPostfix(key)) {
				String value = config.getString(key, null);
				String valueOld = value;
				for (Map.Entry<Object, Object> entry : properties.entrySet()) {
					String propertyKey = entry.getKey().toString();
					String propertyValue = entry.getValue().toString();
					dynamicParamKeys.remove(propertyKey);
					value = value.replace(String.format("${%s}", propertyKey), propertyValue);
				}
				for (String param : dynamicParamKeys) {
					String paramKey = String.format("${%s}", param);
					if (value.contains(paramKey)) {
						value = value.replace(paramKey, config.getString(param, null));
					}
				}
				if (!value.equals(valueOld)) {
					config.setString(trimOriginPostfixFrom(key), value);
				}
			}
		}
	}

	public static String reloadPropertyValue(Configuration config, String value) {
		if (value != null) {
			List<String> dynamicParamKeys = new ArrayList<>(Arrays.asList(DYNAMIC_PARAM_KEYS));
			for (String param : dynamicParamKeys) {
				String paramKey = String.format("${%s}", param);
				if (value.contains(paramKey)) {
					value = value.replace(paramKey, config.getString(param, ""));
				}
			}
		}
		return value;
	}

	/**
	 * Reload config with specific encoded properties.
	 * @param config old Configuration.
	 * @param configKeyPrefix key of the specific properties.
	 */
	public static void reloadConfigWithSpecificProperties(Configuration config, String configKeyPrefix) {
		for (String key : config.keySet()) {
			if (key.startsWith(configKeyPrefix) && key.length() > configKeyPrefix.length()) {
				String value = config.getString(key, null);
				if (value != null) {
					// remove prefix
					String realKey = key.substring(configKeyPrefix.length());
					config.setString(realKey, value);
				}
			}
		}
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
