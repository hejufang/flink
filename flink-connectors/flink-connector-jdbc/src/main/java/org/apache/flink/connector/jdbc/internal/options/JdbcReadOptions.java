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

package org.apache.flink.connector.jdbc.internal.options;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the JDBC scan.
 */
public class JdbcReadOptions implements Serializable {

	private final String query;
	private final String partitionColumnName;
	private final Long partitionLowerBound;
	private final Long partitionUpperBound;
	private final Integer numPartitions;
	private final Long scanIntervalMs;
	private final Integer countOfScanTimes;
	private final Integer parallelism;

	private final int fetchSize;

	private JdbcReadOptions(
			String query,
			String partitionColumnName,
			Long partitionLowerBound,
			Long partitionUpperBound,
			Integer numPartitions,
			int fetchSize,
			Long scanIntervalMs,
			Integer countOfScanTimes,
			Integer parallelism) {
		this.query = query;
		this.partitionColumnName = partitionColumnName;
		this.partitionLowerBound = partitionLowerBound;
		this.partitionUpperBound = partitionUpperBound;
		this.numPartitions = numPartitions;
		this.scanIntervalMs = scanIntervalMs;
		this.countOfScanTimes = countOfScanTimes;
		this.parallelism = parallelism;

		this.fetchSize = fetchSize;
	}

	public Optional<String> getQuery() {
		return Optional.ofNullable(query);
	}

	public Optional<String> getPartitionColumnName() {
		return Optional.ofNullable(partitionColumnName);
	}

	public Optional<Long> getPartitionLowerBound() {
		return Optional.ofNullable(partitionLowerBound);
	}

	public Optional<Long> getPartitionUpperBound() {
		return Optional.ofNullable(partitionUpperBound);
	}

	public Optional<Integer> getNumPartitions() {
		return Optional.ofNullable(numPartitions);
	}

	public Optional<Integer> getParallelism() {
		return Optional.ofNullable(parallelism);
	}

	public Optional<Long> getScanIntervalMs() {
		return Optional.ofNullable(scanIntervalMs);
	}

	public Optional<Integer> getCountOfScanTimes() {
		return Optional.ofNullable(countOfScanTimes);
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JdbcReadOptions) {
			JdbcReadOptions options = (JdbcReadOptions) o;
			return Objects.equals(query, options.query) &&
					Objects.equals(partitionColumnName, options.partitionColumnName) &&
					Objects.equals(partitionLowerBound, options.partitionLowerBound) &&
					Objects.equals(partitionUpperBound, options.partitionUpperBound) &&
					Objects.equals(numPartitions, options.numPartitions) &&
					Objects.equals(scanIntervalMs, options.scanIntervalMs) &&
					Objects.equals(countOfScanTimes, options.countOfScanTimes) &&
					Objects.equals(parallelism, options.parallelism) &&
					Objects.equals(fetchSize, options.fetchSize);
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link JdbcReadOptions}.
	 */
	public static class Builder {
		protected String query;
		protected String partitionColumnName;
		protected Long partitionLowerBound;
		protected Long partitionUpperBound;
		protected Integer numPartitions;
		protected Long scanIntervalMs;
		private Integer countOfScanTimes;
		private Integer parallelism;

		protected int fetchSize = 0;

		/**
		 * optional, SQL query statement for this JDBC source.
		 */
		public Builder setQuery(String query) {
			this.query = query;
			return this;
		}

		/**
		 * optional, name of the column used for partitioning the input.
		 */
		public Builder setPartitionColumnName(String partitionColumnName) {
			this.partitionColumnName = partitionColumnName;
			return this;
		}

		/**
		 * optional, the smallest value of the first partition.
		 */
		public Builder setPartitionLowerBound(long partitionLowerBound) {
			this.partitionLowerBound = partitionLowerBound;
			return this;
		}

		/**
		 * optional, the largest value of the last partition.
		 */
		public Builder setPartitionUpperBound(long partitionUpperBound) {
			this.partitionUpperBound = partitionUpperBound;
			return this;
		}

		/**
		 * optional, the maximum number of partitions that can be used for parallelism in table reading.
		 */
		public Builder setNumPartitions(int numPartitions) {
			this.numPartitions = numPartitions;
			return this;
		}

		/**
		 * optional, the number of rows to fetch per round trip.
		 * default value is 0, according to the jdbc api, 0 means that fetchSize hint will be ignored.
		 */
		public Builder setFetchSize(int fetchSize) {
			this.fetchSize = fetchSize;
			return this;
		}

		public Builder setScanIntervalMs(Long scanIntervalMs) {
			this.scanIntervalMs = scanIntervalMs;
			return this;
		}

		public Builder setCountOfScanTimes(Integer countOfScanTimes) {
			this.countOfScanTimes = countOfScanTimes;
			return this;
		}

		public Builder setParallelism(Integer parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public JdbcReadOptions build() {
			return new JdbcReadOptions(query, partitionColumnName, partitionLowerBound,
				partitionUpperBound, numPartitions, fetchSize, scanIntervalMs,
				countOfScanTimes, parallelism);
		}
	}
}
