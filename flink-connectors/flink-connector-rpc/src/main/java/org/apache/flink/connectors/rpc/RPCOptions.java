/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.rpc;

import org.apache.flink.connectors.rpc.thrift.TransportType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * All options are put here.
 */
public class RPCOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	// consul
	private final String consul;
	private final String cluster;
	private final int consulIntervalSeconds;
	private final String psm;
	private final String testHostPort;
	// thrift
	private final String thriftServiceClass;
	private final String thriftMethod;
	private final TransportType transportType;
	// connect
	private final int connectTimeoutMs;
	private final int connectionPoolSize;

	private String batchConstantValue;
	private final int batchSize;

	private RPCOptions(
			String consul,
			String cluster,
			int consulIntervalSeconds,
			String psm,
			String testHostPort,
			String thriftServiceClass,
			String thriftMethod,
			TransportType transportType,
			int connectTimeoutMs,
			int connectionPoolSize,
			String batchConstantValue,
			int batchSize) {
		this.consul = consul;
		this.cluster = cluster;
		this.consulIntervalSeconds = consulIntervalSeconds;
		this.psm = psm;
		this.testHostPort = testHostPort;
		this.thriftServiceClass = thriftServiceClass;
		this.thriftMethod = thriftMethod;
		this.transportType = transportType;
		this.connectTimeoutMs = connectTimeoutMs;
		this.connectionPoolSize = connectionPoolSize;
		this.batchConstantValue = batchConstantValue;
		this.batchSize = batchSize;
	}

	public String getConsul() {
		return consul;
	}

	public String getCluster() {
		return cluster;
	}

	public int getConsulIntervalSeconds() {
		return consulIntervalSeconds;
	}

	public String getPsm() {
		return psm;
	}

	public String getThriftServiceClass() {
		return thriftServiceClass;
	}

	public String getThriftMethod() {
		return thriftMethod;
	}

	public TransportType getTransportType() {
		return transportType;
	}

	public int getConnectTimeoutMs() {
		return connectTimeoutMs;
	}

	public int getConnectionPoolSize() {
		return connectionPoolSize;
	}

	public String getTestHostPort() {
		return testHostPort;
	}

	public String getBatchConstantValue() {
		return batchConstantValue;
	}

	public int getBatchSize() {
		return batchSize;
	}

	/**
	 * Get a {@link Builder}.
	 * @return a Builder instance which can build a {@link RPCOptions}.
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link RPCOptions}.
	 */
	public static class Builder {
		// consul
		private String consul;
		private String cluster;
		private int consulIntervalSeconds = 600; // default 10 min
		private String psm;
		private String testHostPort;
		// thrift
		private String thriftServiceClass;
		private String thriftMethod;
		private TransportType transportType = TransportType.Framed;
		// connect
		private int connectTimeoutMs = 10_000; // default 10s
		private int connectionPoolSize = 4; // default 4 connections
		private String batchConstantValue;
		private int batchSize = 1;

		private Builder() {
		}

		public Builder setConsul(String consul) {
			this.consul = consul;
			return this;
		}

		public Builder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public Builder setConsulIntervalSeconds(int consulIntervalSeconds) {
			this.consulIntervalSeconds = consulIntervalSeconds;
			return this;
		}

		public Builder setPsm(String psm) {
			this.psm = psm;
			return this;
		}

		public Builder setThriftServiceClass(String thriftServiceClass) {
			this.thriftServiceClass = thriftServiceClass;
			return this;
		}

		public Builder setThriftMethod(String thriftMethod) {
			this.thriftMethod = thriftMethod;
			return this;
		}

		public Builder setTransportType(TransportType transportType) {
			this.transportType = transportType;
			return this;
		}

		public Builder setConnectTimeoutMs(int connectTimeoutMs) {
			this.connectTimeoutMs = connectTimeoutMs;
			return this;
		}

		public Builder setConnectionPoolSize(int connectionPoolSize) {
			this.connectionPoolSize = connectionPoolSize;
			return this;
		}

		public Builder setTestHostPort(String testHostPort) {
			this.testHostPort = testHostPort;
			return this;
		}

		public Builder setBatchConstantValue(String batchConstantValue) {
			this.batchConstantValue = batchConstantValue;
			return this;
		}

		public Builder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public RPCOptions build() {
			Preconditions.checkNotNull(consul, "consul was not supplied.");
			Preconditions.checkNotNull(thriftServiceClass, "thriftServiceClass was not supplied.");
			Preconditions.checkNotNull(thriftMethod, "thriftMethod was not supplied.");
			return new RPCOptions(
				consul,
				cluster,
				consulIntervalSeconds,
				psm,
				testHostPort,
				thriftServiceClass,
				thriftMethod,
				transportType,
				connectTimeoutMs,
				connectionPoolSize,
				batchConstantValue,
				batchSize
			);
		}
	}
}
