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

import org.apache.flink.util.RetryManager;

import java.io.Serializable;

/**
 * All rpc sink options are put here.
 */
public class RPCSinkOptions implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String requestBatchClass;
	private final int flushTimeoutMs;
	private final String responseValue;
	private final RetryManager.Strategy retryStrategy;
	private final int sinkParallelism;

	private RPCSinkOptions(
		String requestBatchClass,
		int flushTimeoutMs,
		String responseValue,
		RetryManager.Strategy retryStrategy,
		int sinkParallelism) {
		this.requestBatchClass = requestBatchClass;
		this.flushTimeoutMs = flushTimeoutMs;
		this.responseValue = responseValue;
		this.retryStrategy = retryStrategy;
		this.sinkParallelism = sinkParallelism;
	}

	public String getRequestBatchClass() {
		return requestBatchClass;
	}

	public int getFlushTimeoutMs() {
		return flushTimeoutMs;
	}

	public String getResponseValue() {
		return responseValue;
	}

	public RetryManager.Strategy getRetryStrategy() {
		return retryStrategy;
	}

	public int getSinkParallelism() {
		return sinkParallelism;
	}

	/**
	 * Get a {@link RPCOptions.Builder}.
	 *
	 * @return a Builder instance which can build a {@link RPCSinkOptions}.
	 */
	public static RPCSinkOptions.Builder builder() {
		return new RPCSinkOptions.Builder();
	}

	/**
	 * Builder for {@link RPCSinkOptions}.
	 */
	public static class Builder {
		private String responseValue;
		private String requestBatchClass;
		private int flushTimeoutMs = -1; // default no flush timeout
		private RetryManager.Strategy retryStrategy;
		private int sinkParallelism = -1; // default -1, not set.

		private Builder() {
		}

		public RPCSinkOptions.Builder setResponseValue(String responseValue) {
			this.responseValue = responseValue;
			return this;
		}

		public RPCSinkOptions.Builder setRequestBatchClass(String batchClass) {
			this.requestBatchClass = batchClass;
			return this;
		}

		public RPCSinkOptions.Builder setFlushTimeoutMs(int flushTimeoutMs) {
			this.flushTimeoutMs = flushTimeoutMs;
			return this;
		}

		public RPCSinkOptions.Builder setRetryStrategy(RetryManager.Strategy retryStrategy) {
			this.retryStrategy = retryStrategy;
			return this;
		}

		public RPCSinkOptions.Builder setSinkParallelism(int sinkParallelism) {
			this.sinkParallelism = sinkParallelism;
			return this;
		}

		public RPCSinkOptions build() {
			return new RPCSinkOptions(
				requestBatchClass,
				flushTimeoutMs,
				responseValue,
				retryStrategy,
				sinkParallelism
			);
		}
	}
}
