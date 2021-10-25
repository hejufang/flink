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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.util.Optional;

/**
 * ShuffleDescriptor for Cloud Shuffle Service.
 */
public class CloudShuffleDescriptor implements ShuffleDescriptor {

	private final ResultPartitionID resultPartitionID;
	private final int shuffleId;
	private final int mapperId;
	private final int mapperAttemptId;
	private final int numberOfMappers;
	private final int numberOfReducers;

	private final String masterHost;
	private final String masterPort;

	public CloudShuffleDescriptor(
			ResultPartitionID resultPartitionID,
			int shuffleId,
			int mapperId,
			int mapperAttemptId,
			int numberOfMappers,
			int numberOfReducers,
			String masterHost,
			String masterPort) {
		this.resultPartitionID = resultPartitionID;
		this.shuffleId = shuffleId;
		this.mapperId = mapperId;
		this.mapperAttemptId = mapperAttemptId;
		this.numberOfMappers = numberOfMappers;
		this.numberOfReducers = numberOfReducers;
		this.masterHost = masterHost;
		this.masterPort = masterPort;
	}

	public int getShuffleId() {
		return shuffleId;
	}

	public int getMapperId() {
		return mapperId;
	}

	public int getMapperAttemptId() {
		return mapperAttemptId;
	}

	public int getNumberOfMappers() {
		return numberOfMappers;
	}

	public int getNumberOfReducers() {
		return numberOfReducers;
	}

	@Override
	public ResultPartitionID getResultPartitionID() {
		return resultPartitionID;
	}

	@Override
	public Optional<ResourceID> storesLocalResourcesOn() {
		return Optional.empty();
	}

	public String getMasterHost() {
		return masterHost;
	}

	public String getMasterPort() {
		return masterPort;
	}
}
