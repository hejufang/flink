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

package org.apache.flink.connector.hsap;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_MAX_RETRIES;

/**
 * DorisOptions.
 */
public class HsapOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final String HSAP_IDENTIFY = "hsap";

	public static final ConfigOption<String> ADDR_LIST = ConfigOptions
		.key("hsap-addrs")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines the address of hsap.");

	public static final ConfigOption<String> DB_NAME = ConfigOptions
		.key("db-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines DB name which we want to access.");

	public static final ConfigOption<String> TABLE_NAME = ConfigOptions
		.key("table-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines table name which we want to access.");

	public static final ConfigOption<String> PSM = ConfigOptions
		.key("hsap-psm")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. It defines hsap server psm, " +
			"if you set a conflict hsap-addr at same time, it will prefer to use hsap-psm.");

	public static final ConfigOption<String> DATA_CENTER = ConfigOptions
		.key("data-center")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. It defines hsap server data-center.");

	public static final ConfigOption<Boolean> STREAMING_INGESTION = ConfigOptions
		.key("streaming-ingestion")
		.booleanType()
		.defaultValue(true)
		.withDeprecatedKeys("Optional. it defines use streaming or batch ingestion mode");

	public static final ConfigOption<Boolean> AUTO_FLUSH = ConfigOptions
		.key("auto-flush")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional. will flush for every record if enabled");

	public static final ConfigOption<String> HLL_COLUMNS = ConfigOptions
		.key("hll-columns")
		.stringType()
		.defaultValue("")
		.withDescription("Optional. specify hll columns which be be convert to hyper loglog types," +
			"ex: \"col_a,col_b\"");

	public static final ConfigOption<String> RAW_HLL_COLUMNS = ConfigOptions
		.key("raw-hll-columns")
		.stringType()
		.defaultValue("")
		.withDescription("Optional. specified columns are in form of HyperLogLog bytes, will be sent " +
			"to ingestion server as HLL Type" +
			"ex: \"col_a,col_b\"");

	public static final ConfigOption<Integer> CONNECTION_PER_SERVER = ConfigOptions
		.key("sink.connection-per-server")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. It defines max connection between client with per server.");

	private final String addr;

	private final MemorySize bufferSize;

	private final int maxRetryTimes;

	private final String database;

	private final String table;

	private final FlinkConnectorRateLimiter rateLimiter;

	private final int parallelism;

	private final long flushIntervalMs;

	private final String hsapPsm;

	private final String dataCenter;

	private boolean streamingIngestion;

	private boolean autoFlush;

	private Set<String> hllColumns;

	private Set<String> rawHllColumns;

	public HsapOptions(
			String addr,
			MemorySize bufferSize,
			int maxRetryTimes,
			String database,
			String table,
			int parallelism,
			FlinkConnectorRateLimiter rateLimiter,
			long flushIntervalMs,
			String hsapPsm,
			String dataCenter,
			boolean streamingIngestion,
			boolean autoFlush,
			Set<String> hllColumns,
			Set<String> rawHllColumns) {
		this.addr = addr;
		this.bufferSize = bufferSize;
		this.maxRetryTimes = maxRetryTimes;
		this.database = database;
		this.table = table;
		this.parallelism = parallelism;
		this.rateLimiter = rateLimiter;
		this.flushIntervalMs = flushIntervalMs;
		this.hsapPsm = hsapPsm;
		this.dataCenter = dataCenter;
		this.streamingIngestion = streamingIngestion;
		this.autoFlush = autoFlush;
		this.hllColumns = hllColumns;
		this.rawHllColumns = rawHllColumns;
	}

	public String getAddr() {
		return addr;
	}

	public MemorySize getBufferSize() {
		return bufferSize;
	}

	public int getMaxRetryTimes() {
		return maxRetryTimes;
	}

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public int getParallelism() {
		return parallelism;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	public long getFlushIntervalMs() {
		return flushIntervalMs;
	}

	public String getHsapPsm() {
		return hsapPsm;
	}

	public String getDataCenter() {
		return dataCenter;
	}

	public boolean isStreamingIngestion() {
		return streamingIngestion;
	}

	public boolean isAutoFlush() {
		return autoFlush;
	}

	public boolean isHllColumn(String name) {
		return hllColumns.contains(name.toLowerCase());
	}

	public boolean isRawHllColumn(String name) {
		return rawHllColumns.contains(name.toLowerCase());
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder.
	 */
	public static class Builder {
		private String addr;

		private MemorySize bufferSize = MemorySize.ofMebiBytes(4);

		private int maxRetryTimes = SINK_MAX_RETRIES.defaultValue();

		private String database;

		private String table;

		private int parallelism = PARALLELISM.defaultValue();

		private FlinkConnectorRateLimiter rateLimiter;

		private long flushIntervalMs = 0L;

		private String hsapPsm;

		private String dataCenter;

		private Boolean streamingIngestion = STREAMING_INGESTION.defaultValue();

		private Boolean autoFlush = AUTO_FLUSH.defaultValue();

		private Set<String> hllColumns = new HashSet<>();

		private Set<String> rawHllColumns = new HashSet<>();

		public Builder setAddr(String addr) {
			this.addr = addr;
			return this;
		}

		public Builder setBufferSize(MemorySize bufferSize) {
			this.bufferSize = bufferSize;
			return this;
		}

		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public Builder setDatabase(String database) {
			this.database = database;
			return this;
		}

		public Builder setTable(String table) {
			this.table = table;
			return this;
		}

		public Builder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public Builder setFlushIntervalMs(long flushIntervalMs) {
			this.flushIntervalMs = flushIntervalMs;
			return this;
		}

		public Builder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public Builder setHsapPsm(String hsapPsm) {
			this.hsapPsm = hsapPsm;
			return this;
		}

		public Builder setDataCenter(String dataCenter) {
			this.dataCenter = dataCenter;
			return this;
		}

		public Builder setStreamingIngestion(Boolean streamingIngestion) {
			this.streamingIngestion = streamingIngestion;
			return this;
		}

		public Builder setAutoFlush(Boolean autoFlush) {
			this.autoFlush = autoFlush;
			return this;
		}

		public Builder setHllColumns(String hllColumns) {
			String[] cols = hllColumns.split(",");
			Set<String> columnList = new HashSet<>();
			if (cols != null) {
				for (String col : cols) {
					col = col.trim();
					if (!col.isEmpty()) {
						columnList.add(col.toLowerCase());
					}
				}
			}
			this.hllColumns = columnList;
			return this;
		}

		public Builder setRawHllColumns(String rawHllColumns) {
			String[] cols = rawHllColumns.split(",");
			Set<String> columnList = new HashSet<>();
			if (cols != null) {
				for (String col : cols) {
					col = col.trim();
					if (!col.isEmpty()) {
						columnList.add(col.toLowerCase());
					}
				}
			}
			this.rawHllColumns = columnList;
			return this;
		}

		public HsapOptions build() {
			Preconditions.checkState(addr != null || (hsapPsm != null && dataCenter != null),
				String.format("Address and psm can not be null at same time " +
					"and if you set %s you have to set %s", PSM.key(), DATA_CENTER.key()));

			return new HsapOptions(addr, bufferSize, maxRetryTimes, database, table, parallelism,
				rateLimiter, flushIntervalMs, hsapPsm, dataCenter, streamingIngestion, autoFlush, hllColumns,
				rawHllColumns);
		}
	}
}
