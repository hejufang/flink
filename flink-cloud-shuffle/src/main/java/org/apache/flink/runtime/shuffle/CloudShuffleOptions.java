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

	// css coordinator request
	public static final String CSS_ASK_PATH = "/css/v1/api/permission/ask";
	public static final String CSS_APPLICATION_TYPE_KEY = "applicationType";
	public static final String CSS_APPLICATION_TYPE_VALUE = "FLINK";
	public static final String CSS_APPLICATION_NAME_KEY = "appName";
	public static final String CSS_DC_KEY = "dc";
	public static final String CSS_PLATFORM_KEY = "platform";
	public static final String CSS_PLATFORM_DEFAULT_VALUE = "unknown";

	// css master info
	public static final String CSS_PREFIX = "flink.cloud-shuffle-service.";
	public static final String CSS_MASTER_ADDRESS = "css.master.address";
	public static final int CLOUD_HISTOGRAM_SIZE = 10;

	/**  Whether the css is enabled. */
	public static final ConfigOption<Boolean> CLOUD_SHUFFLE_SERVICE_SUPPORT = ConfigOptions
		.key("flink.cloud-shuffle-service.support")
		.booleanType()
		.defaultValue(false)
		.withDescription("Whether the cluster supports CSS or not.");

	/**  Whether the css is enabled. */
	public static final ConfigOption<Boolean> CLOUD_SHUFFLE_SERVICE_NEED_FRESHEN_REAL_DC = ConfigOptions
		.key("flink.cloud-shuffle-service.need-freshen-real-dc")
		.booleanType()
		.defaultValue(false)
		.withDescription("Whether the css client need to get the real dc before submitting, it need get from res-lake "
			+ "for internal yarn. This only supports in yarn per-job mode.");

	/** Flink client use css coordinator get the css configurations, but it has a lower priority than the configuration alone. */
	public static final ConfigOption<String> CLOUD_SHUFFLE_SERVICE_COORDINATOR_URL = ConfigOptions
		.key("flink.cloud-shuffle-service.coordinator.url")
		.stringType()
		.noDefaultValue()
		.withDescription("CSS Coordinator URL");

	/** Whether flink use css jar which get the remote path from css coordinator. */
	public static final ConfigOption<Boolean> CLOUD_SHUFFLE_SERVICE_GET_CSS_JAR_FROM_COORDINATOR = ConfigOptions
		.key("flink.cloud-shuffle-service.get-css-jar-from-coordinator")
		.booleanType()
		.defaultValue(true)
		.withDescription("CSS dependency gets from coordinator if true, if false css jar need to set to the classpath.");

	/** The css master information(address and port), it will be set in JobMaster. */
	public static final ConfigOption<String> CLOUD_SHUFFLE_SERVICE_ADDRESS = ConfigOptions
		.key("flink.cloud-shuffle-service.address")
		.stringType()
		.noDefaultValue()
		.withDescription("CSS master address, it's the address of jobMaster.");

	public static final ConfigOption<String> CLOUD_SHUFFLE_SERVICE_PORT = ConfigOptions
		.key("flink.cloud-shuffle-service.port")
		.stringType()
		.noDefaultValue()
		.withDescription("CSS master port, css master is running in jobMaster.");

	/** Which the job come form. */
	public static final ConfigOption<String> CLOUD_SHUFFLE_JOB_PLATFORM = ConfigOptions
		.key("flink.cloud-shuffle-service.platform")
		.stringType()
		.defaultValue(CSS_PLATFORM_DEFAULT_VALUE)
		.withDescription("which platform this job from.");

	/** The css configurations. */
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

	/** The write configuration for batch putting data to css. */
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
		cssConf.set(CSS_MASTER_ADDRESS, "css://" + address + ":" + port);

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
			if (entry.getKey().startsWith(CloudShuffleOptions.CSS_PREFIX)) {
				m.put(entry.getKey().substring(CloudShuffleOptions.CSS_PREFIX.length()), entry.getValue());
			}
		}
		return m;
	}
}
