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

package org.apache.flink.connector.bytetable.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.bytetable.util.ByteTableMutateType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common Options for ByteTable.
 */
@Internal
public class ByteTableOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String database;
	private final String tableName;
	private final String psm;
	private final String cluster;
	private final String service;
	private final int connTimeoutMs;
	private final int chanTimeoutMs;
	private final int reqTimeoutMs;
	private final ByteTableMutateType mutateType;
	@Nullable private final FlinkConnectorRateLimiter rateLimiter;
	private final int parallelism;

	private ByteTableOptions(
			String database,
			String tableName,
			String psm,
			String cluster,
			String service,
			int connTimeoutMs,
			int chanTimeoutMs,
			int reqTimeoutMs,
			ByteTableMutateType mutateType,
			@Nullable FlinkConnectorRateLimiter rateLimiter,
			int parallelism) {
		this.database = database;
		this.tableName = tableName;
		this.psm = psm;
		this.cluster = cluster;
		this.service = service;
		this.connTimeoutMs = connTimeoutMs;
		this.chanTimeoutMs = chanTimeoutMs;
		this.reqTimeoutMs = reqTimeoutMs;
		this.mutateType = mutateType;
		this.rateLimiter = rateLimiter;
		this.parallelism = parallelism;
	}

	public String getDatabase() {
		return database;
	}

	public String getTableName() {
		return tableName;
	}

	public String getPsm() {
		return psm;
	}

	public String getCluster() {
		return cluster;
	}

	public String getService() {
		return service;
	}

	public int getConnTimeoutMs() {
		return connTimeoutMs;
	}

	public int getChanTimeoutMs() {
		return chanTimeoutMs;
	}

	public int getReqTimeoutMs() {
		return reqTimeoutMs;
	}

	public ByteTableMutateType getMutateType() {
		return mutateType;
	}

	@Nullable
	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	public int getParallelism() {
		return parallelism;
	}

	@Override
	public String toString() {
		return "ByteTableOptions{" +
			"database='" + database + '\'' +
			", tableName='" + tableName + '\'' +
			", psm='" + psm + '\'' +
			", cluster='" + cluster + '\'' +
			", service='" + service + '\'' +
			", connTimeoutMs='" + connTimeoutMs + '\'' +
			", chanTimeoutMs='" + chanTimeoutMs + '\'' +
			", reqTimeoutMs='" + reqTimeoutMs + '\'' +
			", mutateType='" + mutateType + '\'' +
			", rateLimiter='" + (rateLimiter == null ? 0 : rateLimiter.getRate()) + '\'' +
			", parallelism='" + parallelism + '\'' +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ByteTableOptions that = (ByteTableOptions) o;
		return connTimeoutMs == that.connTimeoutMs
			&& chanTimeoutMs == that.chanTimeoutMs
			&& reqTimeoutMs == that.reqTimeoutMs
			&& parallelism == that.parallelism
			&& Objects.equals(database, that.database)
			&& Objects.equals(tableName, that.tableName)
			&& Objects.equals(psm, that.psm)
			&& Objects.equals(cluster, that.cluster)
			&& Objects.equals(service, that.service)
			&& Objects.equals(mutateType, that.mutateType)
			&& Objects.equals(rateLimiter, that.rateLimiter);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			database,
			tableName,
			psm,
			cluster,
			service,
			connTimeoutMs,
			chanTimeoutMs,
			reqTimeoutMs,
			mutateType,
			rateLimiter,
			parallelism);
	}

	/**
	 * Creates a builder of {@link ByteTableOptions}.
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link ByteTableOptions}.
	 */
	public static class Builder {

		private String database;
		private String tableName;
		private String psm;
		private String cluster;
		private String service;
		private int connTimeoutMs = 1000;
		private int chanTimeoutMs = 1000;
		private int reqTimeoutMs = 10000;
		// default single row mutate.
		private ByteTableMutateType mutateType = ByteTableMutateType.MUTATE_SINGLE;
		private FlinkConnectorRateLimiter rateLimiter;
		private int parallelism;

		/**
		 * Required. Sets the ByteTable database name.
		 */
		public Builder setDatabase(String database) {
			checkNotNull(database);
			this.database = database;
			return this;
		}

		/**
		 * Required. Sets the ByteTable table name.
		 */
		public Builder setTableName(String tableName) {
			checkNotNull(tableName);
			this.tableName = tableName;
			return this;
		}

		/**
		 * Required. Sets the ByteTable psm.
		 */
		public Builder setPsm(String psm) {
			checkNotNull(psm);
			this.psm = psm;
			return this;
		}

		/**
		 * Required. Sets the ByteTable cluster.
		 */
		public Builder setCluster(String cluster) {
			checkNotNull(cluster);
			this.cluster = cluster;
			return this;
		}

		/**
		 * Required. Sets the ByteTable service.
		 */
		public Builder setService(String service) {
			checkNotNull(service);
			this.service = service;
			return this;
		}

		/**
		 * Optional, Sets the rate limiter.
		 */
		public Builder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public Builder setConnTimeoutMs(int connTimeoutMs) {
			this.connTimeoutMs = connTimeoutMs;
			return this;
		}

		public Builder setChanTimeoutMs(int chanTimeoutMs) {
			this.chanTimeoutMs = chanTimeoutMs;
			return this;
		}

		public Builder setReqTimeoutMs(int reqTimeoutMs) {
			this.reqTimeoutMs = reqTimeoutMs;
			return this;
		}

		public Builder setMutateType(ByteTableMutateType mutateType) {
			this.mutateType = mutateType;
			return this;
		}

		public Builder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		/**
		 * Creates an instance of {@link ByteTableOptions}.
		 */
		public ByteTableOptions build() {
			checkNotNull(database, "database is not set");
			checkNotNull(tableName, "TableName is not set.");
			checkNotNull(psm, "PSM is not set.");
			checkNotNull(cluster, "Cluster is not set.");
			checkNotNull(service, "Service is not set.");
			return new ByteTableOptions(
				database,
				tableName,
				psm,
				cluster,
				service,
				connTimeoutMs,
				chanTimeoutMs,
				reqTimeoutMs,
				mutateType,
				rateLimiter,
				parallelism);
		}
	}
}

