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

package org.apache.flink.runtime.io.network;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.shuffle.CloudShuffleDescriptor;
import org.apache.flink.runtime.shuffle.CloudShuffleMaster;
import org.apache.flink.runtime.shuffle.CloudShuffleOptions;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleServiceFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CloudShuffleServiceFactory.
 */
public class CloudShuffleServiceFactory
		implements ShuffleServiceFactory<CloudShuffleDescriptor, ResultPartition, SingleInputGate> {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleServiceFactory.class);

	@Override
	public ShuffleMaster<CloudShuffleDescriptor> createShuffleMaster(Configuration configuration) {
		if (!configuration.getBoolean(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_ENABLED)) {
			throw new UnsupportedOperationException("shuffle-service-factory.class should be set to enable cloud shuffle service.");
		}

		return new CloudShuffleMaster(configuration);
	}

	@Override
	public ShuffleEnvironment<ResultPartition, SingleInputGate> createShuffleEnvironment(ShuffleEnvironmentContext shuffleEnvironmentContext) {
		throw new UnsupportedOperationException("not supported in this MR");
	}
}
