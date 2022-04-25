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

/**
 *  Options to configure shuffle service.
 */
@SuppressWarnings("WeakerAccess")
public class ShuffleServiceOptions {

	public static final String CLOUD_SHUFFLE = "cloudShuffle";
	public static final String NETTY_SHUFFLE = "nettyShuffle";

	private ShuffleServiceOptions() {
	}

	/**
	 * The full class name of the shuffle service factory implementation to be used by the cluster.
	 */
	public static final ConfigOption<String> SHUFFLE_SERVICE_FACTORY_CLASS = ConfigOptions
		.key("shuffle-service-factory.class")
		.defaultValue("org.apache.flink.runtime.io.network.NettyShuffleServiceFactory")
		.withDescription("The full class name of the shuffle service factory implementation to be used by the cluster. " +
			"The default implementation uses Netty for network communication and local memory as well disk space " +
			"to store results on a TaskExecutor.");

	/**
	 * Whether css is enabled.
	 */
	public static final ConfigOption<Boolean> SHUFFLE_CLOUD_SHUFFLE_MODE = ConfigOptions
		.key("shuffle.cloud-shuffle-mode")
		.defaultValue(false)
		.withDescription("True if CSS is enabled, it will affect partition availability and shuffle data format.");

	/**
	 * Where to report the job status to css.
	 */
	public static final ConfigOption<Boolean> CLOUD_SHUFFLE_SERVICE_REPORT_JOB_STATUS_TO_COORDINATOR = ConfigOptions
		.key("flink.cloud-shuffle-service.report-job-status-to-coordinator")
		.booleanType()
		.defaultValue(false)
		.withDescription("Report job status to css coordinator, it will be effective only using css.");
}
