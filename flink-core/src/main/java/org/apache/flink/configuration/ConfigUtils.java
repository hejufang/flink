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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Utils for configuration.
 */
public class ConfigUtils {
	private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);

	private static final Pattern pattern = Pattern.compile("^[a-zA-Z0-9_]*$");

	public static void writeFlinkConfigIntoEnv(Map<String, String> env, Configuration configuration) {
		int size = 0;
		for (String key : configuration.keySet()) {
			String value = configuration.getString(key, null);
			if (value != null) {
				String formatKey = formatEnvironmentVariable(ConfigConstants.ENV_FLINK_CONFIG_PREFIX + key);
				if (!pattern.matcher(formatKey).find()) {
					throw new IllegalArgumentException("Illegal configuration key : " + formatKey + ", only #.- and letter and number.");
				}
				env.put(formatKey, value);
				LOG.debug("Add configuration into env, key: {}, value: {}.", formatKey, value);
				size += 1;
			}
		}
		LOG.info("Write configuration into environment variables(size={}).", size);
	}

	public static String reformatEnvironmentVariable(String s) {
		return s.replaceAll("_dot_", ".")
			.replaceAll("_well_", "#")
			.replaceAll("_line_", "-");
	}

	public static String formatEnvironmentVariable(String s) {
		return s.replaceAll("\\.", "_dot_")
			.replaceAll("#", "_well_")
			.replaceAll("-", "_line_");
	}
}
