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
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_MAX_ROWS;

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

	public static final ConfigOption<Integer> CONNECTION_PER_SERVER = ConfigOptions
		.key("sink.connection-per-server")
		.intType()
		.defaultValue(5)
		.withDescription("Optional. It defines max connection between client with per server.");

	private final String addr;

	private final int batchRowNum;

	private final String database;

	private final String table;

	private final int connectionPerServer;

	private final FlinkConnectorRateLimiter rateLimiter;

	private final int parallelism;

	private final long flushIntervalMs;

	public HsapOptions(
			String addr,
			int batchRowNum,
			String database,
			String table,
			int connectionPerServer,
			int parallelism,
			FlinkConnectorRateLimiter rateLimiter,
			long flushIntervalMs) {
		this.addr = addr;
		this.batchRowNum = batchRowNum;
		this.database = database;
		this.table = table;
		this.connectionPerServer = connectionPerServer;
		this.parallelism = parallelism;
		this.rateLimiter = rateLimiter;
		this.flushIntervalMs = flushIntervalMs;
	}

	public String getAddr() {
		return addr;
	}

	public int getBatchRowNum() {
		return batchRowNum;
	}

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public int getConnectionPerServer() {
		return connectionPerServer;
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

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder.
	 */
	public static class Builder {
		private String addr;

		private int batchRowNum = SINK_BUFFER_FLUSH_MAX_ROWS.defaultValue();

		private String database;

		private String table;

		private int connectionPerServer = CONNECTION_PER_SERVER.defaultValue();

		private int parallelism = PARALLELISM.defaultValue();

		private FlinkConnectorRateLimiter rateLimiter;

		private long flushIntervalMs = SINK_BUFFER_FLUSH_INTERVAL.defaultValue().toMillis();

		public Builder setAddr(String addr) {
			this.addr = addr;
			return this;
		}

		public Builder setBatchRowNum(int batchRowNum) {
			this.batchRowNum = batchRowNum;
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

		public Builder setConnectionPerServer(int connectionPerServer) {
			this.connectionPerServer = connectionPerServer;
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

		public HsapOptions build() {
			Preconditions.checkNotNull(addr, "Address can not be null");
			return new HsapOptions(addr, batchRowNum, database,
				table, connectionPerServer, parallelism, rateLimiter, flushIntervalMs);
		}
	}
}
