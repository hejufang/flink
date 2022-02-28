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

package org.apache.flink.connector.abase.options;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.abase.utils.AbaseValueType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * normal options for abase: source/sink.
 */
public class AbaseNormalOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	private final String cluster;
	private final String table;
	private final String storage;
	private final String psm;
	private final int timeout;
	private final int maxTotalConnections;
	private final int maxIdleConnections;
	private final int minIdleConnections;
	private final int maxRetries;
	private final String keyFormatter;   // key formatter, $num stands for column value, such "eco:$0:$1:$2"
	private final int[] keyIndices;
	private final AbaseValueType abaseValueType;
	private final boolean isHashMap;
	private final FlinkConnectorRateLimiter rateLimiter;

	public String getCluster() {
		return cluster;
	}

	public String getTable() {
		return table;
	}

	public String getStorage() {
		return storage;
	}

	public String getPsm() {
		return psm;
	}

	public int getTimeout() {
		return timeout;
	}

	public int getMaxTotalConnections() {
		return maxTotalConnections;
	}

	public int getMaxIdleConnections() {
		return maxIdleConnections;
	}

	public int getMinIdleConnections() {
		return minIdleConnections;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public String getKeyFormatter() {
		return keyFormatter;
	}

	public int[] getKeyIndices() {
		return keyIndices;
	}

	public AbaseValueType getAbaseValueType() {
		return abaseValueType;
	}

	public boolean isHashMap() {
		return isHashMap;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	private AbaseNormalOptions(
			String cluster,
			String table,
			String storage,
			String psm,
			int timeout,
			int maxTotalConnections,
			int maxIdleConnections,
			int minIdleConnections,
			int maxRetries,
			String keyFormatter,
			int[] keyIndices,
			AbaseValueType abaseValueType,
			boolean isHashMap,
			FlinkConnectorRateLimiter rateLimiter) {
		this.cluster = cluster;
		this.table = table;
		this.storage = storage;
		this.psm = psm;
		this.timeout = timeout;
		this.maxTotalConnections = maxTotalConnections;
		this.maxIdleConnections = maxIdleConnections;
		this.minIdleConnections = minIdleConnections;
		this.maxRetries = maxRetries;
		this.keyFormatter = keyFormatter;
		this.keyIndices = keyIndices;
		this.abaseValueType = abaseValueType;
		this.isHashMap = isHashMap;
		this.rateLimiter = rateLimiter;
	}

	public static AbaseOptionsBuilder builder() {
		return new AbaseOptionsBuilder();
	}

	/**
	 * Abase options builder.
	 */
	public static class AbaseOptionsBuilder {

		private String cluster;
		private String table;
		private String storage;
		private String psm;
		private int timeout;
		private int maxTotalConnections;
		private int maxIdleConnections;
		private int minIdleConnections;
		private int getResourceMaxRetries;
		private String keyFormatter;
		private int[] keyIndices;
		private AbaseValueType abaseValueType = AbaseValueType.GENERAL;
		private boolean isHashMap = false;
		private FlinkConnectorRateLimiter rateLimiter;

		private AbaseOptionsBuilder() {
		}

		public AbaseOptionsBuilder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public AbaseOptionsBuilder setTable(String table) {
			this.table = table;
			return this;
		}

		public AbaseOptionsBuilder setStorage(String storage) {
			this.storage = storage;
			return this;
		}

		public AbaseOptionsBuilder setPsm(String psm) {
			this.psm = psm;
			return this;
		}

		public AbaseOptionsBuilder setTimeout(int timeout) {
			this.timeout = timeout;
			return this;
		}

		public AbaseOptionsBuilder setMaxTotalConnections(int maxTotalConnections) {
			this.maxTotalConnections = maxTotalConnections;
			return this;
		}

		public AbaseOptionsBuilder setMaxIdleConnections(int maxIdleConnections) {
			this.maxIdleConnections = maxIdleConnections;
			return this;
		}

		public AbaseOptionsBuilder setMinIdleConnections(int minIdleConnections) {
			this.minIdleConnections = minIdleConnections;
			return this;
		}

		public AbaseOptionsBuilder setGetResourceMaxRetries(int getResourceMaxRetries) {
			this.getResourceMaxRetries = getResourceMaxRetries;
			return this;
		}

		public AbaseOptionsBuilder setKeyFormatter(String keyFormatter) {
			this.keyFormatter = keyFormatter;
			return this;
		}

		public AbaseOptionsBuilder setKeyIndices(int[] keyIndices) {
			this.keyIndices = keyIndices;
			return this;
		}

		public AbaseOptionsBuilder setAbaseValueType(AbaseValueType abaseValueType) {
			this.abaseValueType = abaseValueType;
			return this;
		}

		public AbaseOptionsBuilder setHashMap(boolean hashMap) {
			isHashMap = hashMap;
			return this;
		}

		public AbaseOptionsBuilder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public AbaseNormalOptions build() {
			Preconditions.checkNotNull(cluster, "cluster is not supplied.");
			Preconditions.checkNotNull(storage, "storage type is not supplied.");
			Preconditions.checkNotNull(psm, "psm is not supplied.");
			Preconditions.checkArgument(timeout > 0, "timeout must be greater than 0");
			Preconditions.checkArgument(maxTotalConnections > 0,
				"maxTotalConnections must be greater than 0");
			Preconditions.checkArgument(maxIdleConnections >= 0,
				"maxIdleConnections can't be less than 0");
			Preconditions.checkArgument(minIdleConnections >= 0,
				"minIdleConnections can't be less than 0");
			Preconditions.checkArgument(getResourceMaxRetries > 0,
				"getResourceMaxRetries must be greater than 0");
			Preconditions.checkArgument(keyIndices != null && keyIndices.length > 0,
				"keyIndices can't be empty");
			Preconditions.checkNotNull(abaseValueType, "AbaseValueType can not be null.");

			return new AbaseNormalOptions(
				cluster,
				table,
				storage,
				psm,
				timeout,
				maxTotalConnections,
				maxIdleConnections,
				minIdleConnections,
				getResourceMaxRetries,
				keyFormatter,
				keyIndices,
				abaseValueType,
				isHashMap,
				rateLimiter);
		}

		@Override
		public String toString() {
			return "AbaseOptionsBuilder{" +
				"cluster='" + cluster + '\'' +
				", table='" + table + '\'' +
				", storage='" + storage + '\'' +
				", psm='" + psm + '\'' +
				", timeout=" + timeout +
				", maxTotalConnections=" + maxTotalConnections +
				", maxIdleConnections=" + maxIdleConnections +
				", minIdleConnections=" + minIdleConnections +
				", getResourceMaxRetries=" + getResourceMaxRetries +
				", keyFormatter" + keyFormatter +
				", keySeparator=" + Arrays.toString(keyIndices) +
				", abaseValueType=" + abaseValueType +
				", isHashMap=" + isHashMap +
				", rateLimited=" + (rateLimiter == null ? 0 : rateLimiter.getRate()) +
				'}';
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof AbaseNormalOptions)) {
			return false;
		}
		AbaseNormalOptions that = (AbaseNormalOptions) o;
		return timeout == that.timeout &&
			maxTotalConnections == that.maxTotalConnections &&
			maxIdleConnections == that.maxIdleConnections &&
			minIdleConnections == that.minIdleConnections &&
			maxRetries == that.maxRetries &&
			Objects.equals(keyFormatter, that.keyFormatter) &&
			Arrays.equals(keyIndices, that.keyIndices) &&
			Objects.equals(cluster, that.cluster) &&
			Objects.equals(table, that.table) &&
			Objects.equals(storage, that.storage) &&
			Objects.equals(psm, that.psm) &&
			abaseValueType == that.abaseValueType &&
			isHashMap == that.isHashMap &&
			Objects.equals(rateLimiter, that.rateLimiter);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			cluster,
			table,
			storage,
			psm,
			timeout,
			maxTotalConnections,
			maxIdleConnections,
			minIdleConnections,
			maxRetries,
			keyFormatter,
			Arrays.hashCode(keyIndices),
			abaseValueType,
			isHashMap,
			rateLimiter);
	}
}
