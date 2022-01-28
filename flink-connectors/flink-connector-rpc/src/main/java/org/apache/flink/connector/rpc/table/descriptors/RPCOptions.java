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

package org.apache.flink.connector.rpc.table.descriptors;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.util.Preconditions;

import com.bytedance.arch.transport.TransportType;

import java.io.Serializable;

import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.SERVICE_CLIENT_IMPL_CLASS;

/**
 * All options are put here.
 */
public class RPCOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	// consul
	private final String consul;
	private final String cluster;
	private final long consulUpdateIntervalMs;
	private final String psm;
	// thrift
	private final String thriftServiceClass;
	private final String thriftMethod;
	private final TransportType transportType;
	private final String serviceClientImplClass;
	// connect
	private final int connectTimeoutMs;
	private final int socketTimeoutMs;
	private final int connectionPoolSize;

	private final FlinkConnectorRateLimiter rateLimiter;

	private RPCOptions(
			String consul,
			String cluster,
			long consulUpdateIntervalMs,
			String psm,
			String thriftServiceClass,
			String thriftMethod,
			TransportType transportType,
			String serviceClientImplClass,
			int connectTimeoutMs,
			int socketTimeoutMs,
			int connectionPoolSize,
			FlinkConnectorRateLimiter rateLimiter) {
		this.consul = consul;
		this.cluster = cluster;
		this.consulUpdateIntervalMs = consulUpdateIntervalMs;
		this.psm = psm;
		this.thriftServiceClass = thriftServiceClass;
		this.thriftMethod = thriftMethod;
		this.transportType = transportType;
		this.serviceClientImplClass = serviceClientImplClass;
		this.connectTimeoutMs = connectTimeoutMs;
		this.socketTimeoutMs = socketTimeoutMs;
		this.connectionPoolSize = connectionPoolSize;
		this.rateLimiter = rateLimiter;
	}

	public String getConsul() {
		return consul;
	}

	public String getCluster() {
		return cluster;
	}

	public long getConsulUpdateIntervalMs() {
		return consulUpdateIntervalMs;
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

	public String getServiceClientImplClass() {
		return serviceClientImplClass;
	}

	public int getConnectTimeoutMs() {
		return connectTimeoutMs;
	}

	public int getSocketTimeoutMs() {
		return socketTimeoutMs;
	}

	public int getConnectionPoolSize() {
		return connectionPoolSize;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder of {@link RPCOptions}.
	 */
	public static class Builder {
		// consul
		private String consul;
		private String cluster;
		private long consulUpdateIntervalMs;
		private String psm;
		// thrift
		private String thriftServiceClass;
		private String thriftMethod;
		private TransportType transportType = TransportType.Framed;
		private String serviceClientImplClass = SERVICE_CLIENT_IMPL_CLASS.defaultValue();
		// connect
		private int connectTimeoutMs;
		private int socketTimeoutMs;
		private int connectionPoolSize;

		private FlinkConnectorRateLimiter rateLimiter;

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

		public Builder setConsulUpdateIntervalMs(long consulUpdateIntervalMs) {
			this.consulUpdateIntervalMs = consulUpdateIntervalMs;
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

		public Builder setServiceClientImplClass(String serviceClientImplClass) {
			this.serviceClientImplClass = serviceClientImplClass;
			return this;
		}

		public Builder setConnectTimeoutMs(int connectTimeoutMs) {
			this.connectTimeoutMs = connectTimeoutMs;
			return this;
		}

		public Builder setSocketTimeoutMs(int socketTimeoutMs) {
			this.socketTimeoutMs = socketTimeoutMs;
			return this;
		}

		public Builder setConnectionPoolSize(int connectionPoolSize) {
			this.connectionPoolSize = connectionPoolSize;
			return this;
		}

		public Builder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public RPCOptions build() {
			Preconditions.checkNotNull(consul, "consul was not supplied.");
			Preconditions.checkNotNull(thriftServiceClass, "thriftServiceClass was not supplied.");
			Preconditions.checkNotNull(thriftMethod, "thriftMethod was not supplied.");
			return new RPCOptions(
				consul,
				cluster,
				consulUpdateIntervalMs,
				psm,
				thriftServiceClass,
				thriftMethod,
				transportType,
				serviceClientImplClass,
				connectTimeoutMs,
				socketTimeoutMs,
				connectionPoolSize,
				rateLimiter
			);
		}
	}
}
