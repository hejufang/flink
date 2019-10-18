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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Global configuration object for Flink. Similar to Java properties configuration
 * objects it includes key-value pairs which represent the framework's configuration.
 */
@Internal
public final class GlobalConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

	public static final String FLINK_CONF_FILENAME = "flink-conf.yaml";

	// avoid flink-yarn dependency
	public static final String ENV_FLINK_CONFIG_PREFIX = "_FLINK_CONFIG_";

	// the keys whose values should be hidden
	private static final String[] SENSITIVE_KEYS = new String[] {"password", "secret"};

	// the hidden content to be displayed
	public static final String HIDDEN_CONTENT = "******";

	// the key of common configuration in yaml file.
	public static final String COMMON = "common";

	// the key of flexible origin value in yaml file.
	private static final String ORIGIN_KEY_POSTFIX = "#ORIGIN#";

	private static final String[] DYNAMIC_PARAM_KEYS = {
		ConfigConstants.CLUSTER_NAME_KEY, ConfigConstants.HDFS_PREFIX_KEY, ConfigConstants.DC_KEY};

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

		Map<String, String> env = System.getenv();
		LOG.info("Environment variable size: {}", env.size());
		List<String> configEnvKeys = env.keySet().stream().filter(key ->
			key.startsWith(ENV_FLINK_CONFIG_PREFIX)).collect(Collectors.toList());
		LOG.info("Flink configuration environment variable size: {}", configEnvKeys.size());
		LOG.info("Keys: {}", configEnvKeys.stream().map(
			GlobalConfiguration::reformatEnvironmentVariable).collect(Collectors.joining(",")));

		Configuration configuration;
		if (yamlConfigFile.exists()) {
			configuration = loadYAMLResource(yamlConfigFile);
		} else if (configEnvKeys.size() > 0) {
			configuration = loadConfigFromEnvironmentVariable(env, configEnvKeys);
		} else {
			throw new IllegalConfigurationException(
				"The Flink config file '" + yamlConfigFile +
					"' (" + confDirFile.getAbsolutePath() + ") and environment variable " +
					ENV_FLINK_CONFIG_PREFIX + " both do not exist.");
		}

		if (dynamicProperties != null) {
			configuration.addAll(dynamicProperties);
		}

		return enrichWithEnvironmentVariables(configuration);
	}

	private static Configuration loadConfigFromEnvironmentVariable(Map<String, String> env, List<String> keys) {
		Configuration configuration = new Configuration();
		keys.forEach(key -> {
			String reformatKey = reformatEnvironmentVariable(key);
			String value = env.get(key);
			String valueOld = value;
			for (String param : DYNAMIC_PARAM_KEYS) {
				String paramKey = String.format("${%s}", param);
				if (value.contains(paramKey)) {
					value = value.replace(paramKey, env.get(formatEnvironmentVariable(param)));
				}
			}
			configuration.setString(reformatKey, value);

			// save OriginKey with value.
			if (!value.equals(valueOld)) {
				configuration.setString(appendOriginPostfixTo(reformatKey), valueOld);
			}
			configuration.setString(reformatKey.replaceAll(
				ENV_FLINK_CONFIG_PREFIX, ""), value);
		});
		return configuration;
	}

	// avoid dependency same as #org.apache.flink.yarn.ConfigUtils#reformatEnvironmentVariable
	private static String reformatEnvironmentVariable(String s) {
		return s.replaceAll("_dot_", ".")
				.replaceAll("_well_", "#")
				.replaceAll("_line_", "-");
	}

	// avoid dependency same as #org.apache.flink.yarn.ConfigUtils#formatEnvironmentVariable
	private static String formatEnvironmentVariable(String s) {
		return s.replaceAll("\\.", "_dot_")
			.replaceAll("#", "_well_")
			.replaceAll("-", "_line_");
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
