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

package org.apache.flink.connector.abase.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.abase.client.AbaseClientWrapper;
import org.apache.flink.connector.abase.client.BaseClient;
import org.apache.flink.connector.abase.client.RedisClientWrapper;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.abase.AbaseClient;
import com.bytedance.abase.AbaseClientBuilder;
import com.bytedance.redis.RedisClient;
import com.bytedance.redis.RedisClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abase Client and Table Utils.
 */
public class AbaseClientTableUtils {
	private static final Logger LOG = LoggerFactory.getLogger(AbaseClientTableUtils.class);

	public static BaseClient getClientWrapper(AbaseNormalOptions normalOptions) {
		if (normalOptions.getStorage().equals(Constants.ABASE_IDENTIFIER)) {
			return new AbaseClientWrapper(normalOptions);
		} else {
			return new RedisClientWrapper(normalOptions);
		}
	}

	public static synchronized AbaseClient getAbaseClient(AbaseNormalOptions normalOptions) {
		AbaseClient client;
		try {
			client = new AbaseClientBuilder(normalOptions.getCluster(), normalOptions.getPsm())
				.setConnConnectTimeout(normalOptions.getTimeout())
				.setPoolMaxIdle(normalOptions.getMaxIdleConnections())
				.setPoolMinIdle(normalOptions.getMinIdleConnections())
				.setPoolMaxTotal(normalOptions.getMaxTotalConnections())
				.setConnConnectMaxRetry(normalOptions.getMaxRetries())
				.build();
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
		LOG.info("AbaseClient Connection established");
		return client;
	}

	public static synchronized RedisClient getRedisClient(AbaseNormalOptions normalOptions) {
		RedisClient client;
		try {
			client = new RedisClientBuilder(normalOptions.getCluster(), normalOptions.getPsm())
				.setConnConnectTimeout(normalOptions.getTimeout())
				.setPoolMaxIdle(normalOptions.getMaxIdleConnections())
				.setPoolMinIdle(normalOptions.getMinIdleConnections())
				.setPoolMaxTotal(normalOptions.getMaxTotalConnections())
				.setConnConnectMaxRetry(normalOptions.getMaxRetries())
				.build();
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
		LOG.info("RedisClient Connection established");
		return client;
	}

	public static ProcessingTimeService getTimeService(RuntimeContext context) {
		if (context instanceof StreamingRuntimeContext) {
			return ((StreamingRuntimeContext) context).getProcessingTimeService();
		}
		throw new IllegalArgumentException("Failed to get processing time service of context.");
	}
}
