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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.Preconditions;

import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.PartitionGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_INPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_OUTPUT;
import static org.apache.flink.runtime.shuffle.metrics.CloudShuffleMetricFactory.createShuffleIOOwnerMetricGroup;
import static org.apache.flink.runtime.shuffle.metrics.CloudShuffleMetricFactory.registerInputMetrics;
import static org.apache.flink.runtime.shuffle.metrics.CloudShuffleMetricFactory.registerOutputMetrics;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * CloudShuffleEnvironment.
 */
public class CloudShuffleEnvironment implements ShuffleEnvironment<CloudShuffleResultPartition, CloudShuffleInputGate> {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleEnvironment.class);

	// the port is used to identify the TaskManager on Flink UI(not actually used)
	private static final int PORT = Math.abs(new java.util.Random().nextInt());

	private final Object lock = new Object();

	private final Configuration configuration;

	private final String applicationId;

	private boolean isClosed;

	private ShuffleClient cssClient;

	private String currentCSSMasterAddress;
	private String currentCSSMasterPort;

	public CloudShuffleEnvironment(Configuration configuration) {
		this.configuration = configuration;
		this.isClosed = false;

		this.applicationId = System.getenv("_APP_ID");

		this.cssClient = null;
		this.currentCSSMasterAddress = "-";
		this.currentCSSMasterPort = "-";
	}

	@Override
	public int start() throws IOException {
		return PORT;
	}

	@Override
	public ShuffleIOOwnerContext createShuffleIOOwnerContext(String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup) {
		MetricGroup cloudGroup = createShuffleIOOwnerMetricGroup(checkNotNull(parentGroup));
		return new ShuffleIOOwnerContext(
				checkNotNull(ownerName),
				checkNotNull(executionAttemptID),
				parentGroup,
				cloudGroup.addGroup(METRIC_GROUP_OUTPUT),
				cloudGroup.addGroup(METRIC_GROUP_INPUT));
	}

	@Override
	public List<CloudShuffleResultPartition> createResultPartitionWriters(ShuffleIOOwnerContext ownerContext, List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {
		LOG.info("{} build {} Cloud Shuffle ResultPartitions.", ownerContext.getOwnerName(), resultPartitionDeploymentDescriptors.size());
		synchronized (lock) {
			Preconditions.checkState(!isClosed, "The CloudShuffleEnvironment has already been shut down.");

			if (resultPartitionDeploymentDescriptors.size() > 0) {
				boolean changed = setUpMasterHostAndPort(configuration, (CloudShuffleDescriptor) resultPartitionDeploymentDescriptors.get(0).getShuffleDescriptor());
				if (changed) {
					if (cssClient != null) {
						cssClient.shutDown();
					}
					final CssConf cssConf = CloudShuffleOptions.fromConfiguration(configuration);
					cssClient = ShuffleClient.get(cssConf);
				}
			}

			CloudShuffleResultPartition[] resultPartitions = new CloudShuffleResultPartition[resultPartitionDeploymentDescriptors.size()];

			final long segmentSize = configuration.get(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_BUFFER_SIZE).getBytes();
			final long maxBatchSize = configuration.get(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_MAX_BATCH_SIZE).getBytes();
			final long maxBatchSizePerGroup = configuration.get(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_MAX_BATCH_SIZE_PER_GROUP).getBytes();
			final long initialSizePerReducer = configuration.get(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_INITIAL_SIZE_PER_REDUCER).getBytes();

			for (int partitionIndex = 0; partitionIndex < resultPartitionDeploymentDescriptors.size(); partitionIndex++) {
				ResultPartitionDeploymentDescriptor deploymentDescriptor = resultPartitionDeploymentDescriptors.get(partitionIndex);
				CloudShuffleDescriptor shuffleDescriptor = (CloudShuffleDescriptor) deploymentDescriptor.getShuffleDescriptor();

				List<PartitionGroup> partitionGroups = shuffleDescriptor.getPartitionGroups();
				cssClient.applyShufflePartitionGroup(shuffleDescriptor.getShuffleId(), partitionGroups);
				final Map<Integer, PartitionGroup> reducerIdToGroups = new HashMap<>(shuffleDescriptor.getNumberOfReducers());
				for (PartitionGroup group : partitionGroups) {
					for (int i = group.startPartition; i < group.endPartition; i++) {
						reducerIdToGroups.put(i, group);
					}
				}

				resultPartitions[partitionIndex] = new CloudShuffleResultPartition(
					ownerContext.getOwnerName(),
					deploymentDescriptor.getMaxParallelism(),
					shuffleDescriptor.getResultPartitionID(),
					cssClient,
					applicationId,
					shuffleDescriptor.getShuffleId(),
					shuffleDescriptor.getMapperId() - shuffleDescriptor.getMapperBeginIndex(), // start from 0
					shuffleDescriptor.getMapperAttemptId(),
					shuffleDescriptor.getNumberOfMappers(),
					shuffleDescriptor.getNumberOfReducers(),
					segmentSize,
					maxBatchSize,
					maxBatchSizePerGroup,
					initialSizePerReducer,
					reducerIdToGroups);
			}
			registerOutputMetrics(ownerContext.getOutputGroup(), resultPartitions);

			return  Arrays.asList(resultPartitions);
		}
	}

	@Override
	public List<CloudShuffleInputGate> createInputGates(ShuffleIOOwnerContext ownerContext, PartitionProducerStateProvider partitionProducerStateProvider, List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {
		LOG.info("{} build {} Cloud Shuffle InputGates.", ownerContext.getOwnerName(), inputGateDeploymentDescriptors.size());
		synchronized (lock) {
			Preconditions.checkState(!isClosed, "The CloudShuffleEnvironment has already been shut down.");

			if (cssClient == null && inputGateDeploymentDescriptors.size() > 0) {
				boolean changed = setUpMasterHostAndPort(configuration, (CloudShuffleDescriptor) inputGateDeploymentDescriptors.get(0).getShuffleDescriptors()[0]);
				if (changed) {
					if (cssClient != null) {
						cssClient.shutDown();
					}
					final CssConf cssConf = CloudShuffleOptions.fromConfiguration(configuration);
					cssClient = ShuffleClient.get(cssConf);
				}
			}

			final int segmentSize = (int) configuration.get(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_BUFFER_SIZE).getBytes();

			boolean isBlocking = true;
			CloudShuffleInputGate[] inputGates = new CloudShuffleInputGate[inputGateDeploymentDescriptors.size()];
			for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
				final InputGateDeploymentDescriptor igdd = inputGateDeploymentDescriptors.get(gateIndex);
				final CloudShuffleDescriptor cloudShuffleDescriptor = (CloudShuffleDescriptor) igdd.getShuffleDescriptors()[0];
				inputGates[gateIndex] = new CloudShuffleInputGate(
					ownerContext.getOwnerName(),
					gateIndex,
					applicationId,
					cssClient,
					cloudShuffleDescriptor.getShuffleId(),
					igdd.getConsumedSubpartitionIndex(),
					cloudShuffleDescriptor.getNumberOfMappers(),
					segmentSize);

				if (!igdd.getConsumedPartitionType().isBlocking()) {
					isBlocking = false;
				}
			}
			if (isBlocking) {
				registerInputMetrics(ownerContext.getInputGroup(), inputGates);
			}

			return Arrays.asList(inputGates);
		}
	}

	@Override
	public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {
		// there's no local resources
	}

	@Override
	public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
		return Collections.emptyList();
	}

	@Override
	public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo) throws IOException, InterruptedException {
		throw new UnsupportedEncodingException();
	}

	@Override
	public void close() throws Exception {
		synchronized (lock) {
			isClosed = true;
		}
	}

	@VisibleForTesting
	public boolean setUpMasterHostAndPort(Configuration configuration, CloudShuffleDescriptor descriptor) {
		configuration.set(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_ADDRESS, descriptor.getMasterHost());
		configuration.set(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_PORT, descriptor.getMasterPort());

		final String host = descriptor.getMasterHost();
		final String port = descriptor.getMasterPort();

		boolean changed = !host.equals(currentCSSMasterAddress) || !port.equals(currentCSSMasterPort);
		if (changed) {
			LOG.info("Setup CSS Master address(from host={},port={}, to host={},port={}).", currentCSSMasterAddress, currentCSSMasterPort, host, port);
			currentCSSMasterAddress = host;
			currentCSSMasterPort = port;
		}
		return changed;
	}
}
