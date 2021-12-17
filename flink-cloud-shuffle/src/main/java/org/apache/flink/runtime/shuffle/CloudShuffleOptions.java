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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;

import com.bytedance.css.common.CssConf;

import java.util.HashMap;
import java.util.Map;

/**
 * Options.
 */
public class CloudShuffleOptions {

	public static final String PREFIX = "flink.cloud-shuffle-service.";

	public static final ConfigOption<Boolean> CLOUD_SHUFFLE_SERVICE_SUPPORT = ConfigOptions
		.key("flink.cloud-shuffle-service.support")
		.booleanType()
		.defaultValue(false)
		.withDescription("Whether the cluster supports CSS or not.");

	public static final ConfigOption<String> CLOUD_SHUFFLE_SERVICE_COORDINATOR_URL = ConfigOptions
		.key("flink.cloud-shuffle-service.coordinator.url")
		.stringType()
		.noDefaultValue()
		.withDescription("CSS Coordinator URL");

	public static final ConfigOption<String> CLOUD_SHUFFLE_SERVICE_ADDRESS = ConfigOptions
			.key("flink.cloud-shuffle-service.address")
			.stringType()
			.noDefaultValue()
			.withDescription("CSS address");

	public static final ConfigOption<String> CLOUD_SHUFFLE_SERVICE_PORT = ConfigOptions
			.key("flink.cloud-shuffle-service.port")
			.stringType()
			.noDefaultValue()
			.withDescription("CSS port");

	public static final ConfigOption<String> CLOUD_SHUFFLE_CLUSTER = ConfigOptions
		.key("flink.cloud-shuffle-service.css.cluster.name")
		.stringType()
		.noDefaultValue()
		.withDescription("css cluster name");

	public static final ConfigOption<String> CLOUD_SHUFFLE_ZK_ADDRESS = ConfigOptions
		.key("flink.cloud-shuffle-service.css.zookeeper.address")
		.stringType()
		.noDefaultValue()
		.withDescription("css zookeeper address");

	public static final ConfigOption<String> CLOUD_SHUFFLE_REGISTRY_TYPE = ConfigOptions
			.key("flink.cloud-shuffle-service.css.worker.registry.type")
			.stringType()
			.defaultValue("zookeeper")
			.withDescription("css worker registry type");

	public static final ConfigOption<Integer> CLOUD_SHUFFLE_SERVICE_NUMBER_OF_WORKERS = ConfigOptions
			.key("flink.cloud-shuffle-service.number-of-workers")
			.intType()
			.noDefaultValue()
			.withDescription("CSS number of workers");

	public static final ConfigOption<MemorySize> CLOUD_SHUFFLE_SERVICE_BUFFER_SIZE = ConfigOptions
			.key("flink.cloud-shuffle-service.buffer-size")
			.memoryType()
			.defaultValue(MemorySize.parse("4mb"))
			.withDescription("Max size for a single buffer.");

	public static final ConfigOption<MemorySize> CLOUD_SHUFFLE_SERVICE_MAX_BATCH_SIZE = ConfigOptions
		.key("flink.cloud-shuffle-service.max-batch-size")
		.memoryType()
		.defaultValue(MemorySize.parse("128mb"))
		.withDescription("Max batch size for a pushAll.");

	public static final ConfigOption<MemorySize> CLOUD_SHUFFLE_SERVICE_MAX_BATCH_SIZE_PER_GROUP = ConfigOptions
		.key("flink.cloud-shuffle-service.max-batch-size-per-group")
		.memoryType()
		.defaultValue(MemorySize.parse("64mb"))
		.withDescription("Max group size for a group batch push.");

	public static final ConfigOption<MemorySize> CLOUD_SHUFFLE_SERVICE_INITIAL_SIZE_PER_REDUCER = ConfigOptions
		.key("flink.cloud-shuffle-service.initial-size-per-reducer")
		.memoryType()
		.defaultValue(MemorySize.parse("32kb"))
		.withDescription("Initial size for each reducer.");

	// used for TM
	public static CssConf fromConfiguration(Configuration configuration) {
		CssConf cssConf = new CssConf();
		final String address = configuration.get(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_ADDRESS);
		final String port = configuration.get(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_PORT);
		cssConf.set("css.master.address", "css://" + address + ":" + port);

		Map<String, String> cssProperties = propertiesFromConfiguration(configuration);

		for (Map.Entry<String, String> entry : cssProperties.entrySet()) {
			cssConf.set(entry.getKey(), entry.getValue());
		}
		return cssConf;
	}

	// used for JM
	public static Map<String, String> propertiesFromConfiguration(Configuration configuration) {
		Map<String, String> m = new HashMap<>();
		for (Map.Entry<String, String> entry : configuration.toMap().entrySet()) {
			if (entry.getKey().startsWith(CloudShuffleOptions.PREFIX)) {
				m.put(entry.getKey().substring(CloudShuffleOptions.PREFIX.length()), entry.getValue());
			}
		}
		return m;
	}
}
