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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.css.api.CssShuffleContext;
import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.client.impl.ShuffleClientImpl;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.PartitionGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * ShuffleMaster for Cloud Shuffle Service.
 */
public class CloudShuffleMaster implements ShuffleMaster<CloudShuffleDescriptor> {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleMaster.class);

	private final int baseShuffleId;

	private final Map<Integer, CloudShuffleDescriptor.CloudShuffleInfo> shuffleIds;

	private final String applicationId;
	private final int applicationAttemptNumber;

	// TODO. add a new method to close client
	private final ShuffleClient cssClient;

	private String masterHost;
	private String masterPort;

	private final CssConf cssConf;

	@VisibleForTesting
	public CloudShuffleMaster(
			String applicationId,
			ShuffleClient cssClient,
			int applicationAttemptNumber) {
		this.shuffleIds = new HashMap<>();
		this.applicationId = applicationId;
		this.applicationAttemptNumber = applicationAttemptNumber;
		this.cssClient = cssClient;
		this.cssConf = new CssConf();

		// use high 16 bit as Application Attempt
		// use low 16 bit as Shuffle Id
		this.baseShuffleId = applicationAttemptNumber << 16;
	}

	public CloudShuffleMaster(Configuration configuration) {
		// start CSS Master
		try {
			if (configuration.getBoolean(ConfigConstants.IS_KUBERNETES_KEY, false)) {
				String jmAddress = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, ConfigConstants.JOB_MANAGER_IPC_ADDRESS_VALUE);
				if (jmAddress.equals(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_VALUE)) {
					String msg = String.format("The value of {} is needed, but it's not set.", ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY);
					LOG.warn(msg);
					throw new FlinkRuntimeException(msg);
				} else {
					this.masterHost = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "none");
				}
			} else {
				this.masterHost = getLocalFQDNHostName();
			}
			CssShuffleContext.get().startMaster(this.masterHost, 0, CloudShuffleOptions.propertiesFromConfiguration(configuration));

			this.masterPort = String.valueOf(CssShuffleContext.get().getMasterPort());
			configuration.set(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_ADDRESS, this.masterHost);
			configuration.set(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_PORT, this.masterPort);

			LOG.info("CLOUD_SHUFFLE_SERVICE_ADDRESS: {}, CLOUD_SHUFFLE_SERVICE_PORT: {}.", this.masterHost, this.masterPort);

			final int numberOfWorkers = configuration.getInteger(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_NUMBER_OF_WORKERS);
			CssShuffleContext.get().allocateWorkerIfNeeded(numberOfWorkers);

			this.masterHost = CssShuffleContext.get().getMasterHost();
			this.masterPort = String.valueOf(CssShuffleContext.get().getMasterPort());
		} catch (Throwable e) {
			LOG.error("Fail to start CSS Master.", e);
			System.exit(-100);
		}

		this.cssConf = CloudShuffleOptions.fromConfiguration(configuration);
		this.cssClient = (ShuffleClientImpl) ShuffleClient.get(cssConf);
		this.shuffleIds = new HashMap<>();
		if (configuration.getBoolean(ConfigConstants.IS_KUBERNETES_KEY, false)) {
			this.applicationId = configuration.getString(ConfigConstants.KUBERNETES_CLUSTER_ID, ConfigConstants.KUBERNETES_CLUSTER_ID_DEFAULT);
			this.applicationAttemptNumber = (new Random()).nextInt();
		} else {
			this.applicationId = System.getenv(ConfigConstants.ENV_APP_ID);
			this.applicationAttemptNumber = getApplicationAttemptNumber(System.getenv("CONTAINER_ID"));
		}
		this.baseShuffleId = applicationAttemptNumber << 16;
	}

	@Override
	public CompletableFuture<CloudShuffleDescriptor> registerPartitionWithProducer(
			PartitionDescriptor partitionDescriptor,
			ProducerDescriptor producerDescriptor) {

		final int mapperIndex = partitionDescriptor.getPartitionId().getPartitionNum();

		final ShuffleInfo shuffleInfo = partitionDescriptor.getShuffleInfo();
		final int shuffleId = baseShuffleId + shuffleInfo.getShuffleId();

		if (!shuffleIds.containsKey(shuffleId)) {
			// send RPC request to register the shuffleId
			List<PartitionGroup> partitionGroups = null;
			try {
				partitionGroups = cssClient.registerPartitionGroup(
						this.applicationId,
						shuffleId,
						shuffleInfo.getNumberOfMappers(), // number of mappers
						shuffleInfo.getNumberOfReducers(),
						CssConf.maxPartitionsPerGroup(cssConf)); // number of reducers
				for (PartitionGroup group : partitionGroups) {
					LOG.info("partitionGroupId: {}, startPartition: {}, endPartition: {}.", group.partitionGroupId, group.startPartition, group.endPartition);
				}
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}

			if (partitionGroups.isEmpty()) {
				throw new IllegalStateException();
			}

			// update the shuffleId
			shuffleIds.put(shuffleId, new CloudShuffleDescriptor.CloudShuffleInfo(
				shuffleId,
				shuffleInfo.getMapperBeginIndex(),
				shuffleInfo.getMapperEndIndex(),
				shuffleInfo.getReducerBeginIndex(),
				shuffleInfo.getReducerEndIndex(),
				partitionGroups));

			LOG.info("Producer {} registers partitionGroup (id={}) with {} mappers and {} reducers",
				producerDescriptor.getProducerExecutionId(),
				shuffleId,
				shuffleInfo.getNumberOfMappers(),
				shuffleInfo.getNumberOfReducers());
		}

		CloudShuffleDescriptor cloudShuffleDescriptor = new CloudShuffleDescriptor(
				new ResultPartitionID(partitionDescriptor.getPartitionId(), producerDescriptor.getProducerExecutionId()),
				shuffleIds.get(shuffleId),
				mapperIndex,
				producerDescriptor.getAttemptNumber(),
				masterHost,
				masterPort);

		LOG.info("Register partition(ID={}) with producer(executionId={}, index={}, attemptNumber={}).",
				partitionDescriptor.getPartitionId(),
				producerDescriptor.getProducerExecutionId(),
				mapperIndex,
				producerDescriptor.getAttemptNumber());

		return CompletableFuture.completedFuture(cloudShuffleDescriptor);
	}

	@Override
	public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {}

	private String getLocalFQDNHostName() {
		String fqdnHostName = null;
		try {
			InetAddress inetAddress = InetAddress.getLocalHost();
			fqdnHostName = inetAddress.getCanonicalHostName();
		} catch (Throwable t) {
			LOG.warn("Unable to determine the canonical hostname. Input split assignment (such as " +
					"for HDFS files) may be non-local when the canonical hostname is missing.");
			System.exit(-1000);
		}
		return fqdnHostName;
	}

	@VisibleForTesting
	public int getApplicationAttemptNumber(String containerId) {
		// like container_e522_1627528269117_282114_02_000001
		final int length = containerId.length();
		return Integer.parseInt(containerId.substring(length - 9, length - 7));
	}
}
