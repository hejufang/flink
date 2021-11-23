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

import java.io.Serializable;
import java.util.Optional;

/**
 * ShuffleDescriptor for Cloud Shuffle Service.
 */
public class CloudShuffleDescriptor implements ShuffleDescriptor {

	private static final long serialVersionUID = 951657789L;

	private final ResultPartitionID resultPartitionID;
	private final int mapperId;
	private final int mapperAttemptId;

	private final CloudShuffleInfo shuffleInfo;

	private final String masterHost;
	private final String masterPort;

	public CloudShuffleDescriptor(
			ResultPartitionID resultPartitionID,
			CloudShuffleInfo shuffleInfo,
			int mapperId,
			int mapperAttemptId,
			String masterHost,
			String masterPort) {
		this.resultPartitionID = resultPartitionID;
		this.shuffleInfo = shuffleInfo;
		this.mapperId = mapperId;
		this.mapperAttemptId = mapperAttemptId;
		this.masterHost = masterHost;
		this.masterPort = masterPort;
	}

	public int getShuffleId() {
		return shuffleInfo.getShuffleId();
	}

	public int getMapperId() {
		return mapperId;
	}

	public int getMapperAttemptId() {
		return mapperAttemptId;
	}

	public int getNumberOfMappers() {
		return shuffleInfo.getNumberOfMappers();
	}

	public int getNumberOfReducers() {
		return shuffleInfo.getNumberOfReducers();
	}

	public int getMapperBeginIndex() {
		return shuffleInfo.getMapperBeginIndex();
	}

	public int getMapperEndIndex() {
		return shuffleInfo.getMapperEndIndex();
	}

	public int getReducerBeginIndex() {
		return shuffleInfo.getReducerBeginIndex();
	}

	public int getReducerEndIndex() {
		return shuffleInfo.getReducerEndIndex();
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

	/**
	 * CloudShuffleInfo.
	 */
	public static class CloudShuffleInfo implements Serializable {
		private static final long serialVersionUID = 926787515L;

		// high 16bit is application attempt
		// low 16bit is shuffleId from ShuffleInfo.java
		private final int shuffleId;
		private final int mapperBeginIndex;
		private final int mapperEndIndex;
		private final int reducerBeginIndex;
		private final int reducerEndIndex;

		public CloudShuffleInfo(int shuffleId, int mapperBeginIndex, int mapperEndIndex, int reducerBeginIndex, int reducerEndIndex) {
			this.shuffleId = shuffleId;
			this.mapperBeginIndex = mapperBeginIndex;
			this.mapperEndIndex = mapperEndIndex;
			this.reducerBeginIndex = reducerBeginIndex;
			this.reducerEndIndex = reducerEndIndex;
		}

		public int getShuffleId() {
			return shuffleId;
		}

		public int getMapperBeginIndex() {
			return mapperBeginIndex;
		}

		public int getMapperEndIndex() {
			return mapperEndIndex;
		}

		public int getReducerBeginIndex() {
			return reducerBeginIndex;
		}

		public int getReducerEndIndex() {
			return reducerEndIndex;
		}

		public int getNumberOfMappers() {
			return mapperEndIndex - mapperBeginIndex + 1;
		}

		public int getNumberOfReducers() {
			return reducerEndIndex - reducerBeginIndex + 1;
		}

		@Override
		public String toString() {
			return "CloudShuffleInfo{" +
				"shuffleId=" + shuffleId +
				", mapperBeginIndex=" + mapperBeginIndex +
				", mapperEndIndex=" + mapperEndIndex +
				", reducerBeginIndex=" + reducerBeginIndex +
				", reducerEndIndex=" + reducerEndIndex +
				'}';
		}
	}
}
