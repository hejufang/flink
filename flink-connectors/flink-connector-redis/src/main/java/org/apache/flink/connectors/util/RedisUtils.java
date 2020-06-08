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

package org.apache.flink.connectors.util;

import com.bytedance.kvclient.ClientPool;
import com.bytedance.redis.RedisConfig;
import com.bytedance.redis.RedisPool;
import com.bytedance.springdb.SpringDbConfig;
import com.bytedance.springdb.SpringDbPool;
import redis.clients.jedis.Jedis;

/**
 * RedisUtil Function.
 */
public class RedisUtils {

	public static ClientPool getRedisClientPool(
			String cluster,
			String psm,
			long serverUpdatePeriod,
			int timeout,
			boolean forceConnectionsSetting,
			int maxTotalConnections,
			int maxIdleConnections,
			int minIdleConnections) {
		RedisConfig config;
		config = new RedisConfig(cluster, psm);
		if (serverUpdatePeriod != 0) {
			config.setServerUpdatePeriod(serverUpdatePeriod);
		}
		if (timeout != 0) {
			config.setTimeout(timeout);
		}

		if (!forceConnectionsSetting) {
			// Set connection num to 1 to avoid too many connections.
			config.setMaxTotalConnections(1);
			config.setMaxIdleConnections(1);
			config.setMinIdleConnections(1);
		} else {
			if (maxTotalConnections != 0) {
				config.setMaxTotalConnections(maxTotalConnections);
			}
			if (maxIdleConnections != 0) {
				config.setMaxIdleConnections(maxIdleConnections);
			}
			if (minIdleConnections != 0) {
				config.setMinIdleConnections(minIdleConnections);
			}
		}
		return new RedisPool(config);
	}

	public static SpringDbPool getAbaseClientPool(
			String cluster,
			String psm,
			String table,
			long serverUpdatePeriod,
			int timeout,
			boolean forceConnectionsSetting,
			int maxTotalConnections,
			int maxIdleConnections,
			int minIdleConnections) {
		SpringDbConfig config = new SpringDbConfig(cluster, psm, table);
		if (serverUpdatePeriod != 0) {
			config.setServerUpdatePeriod(serverUpdatePeriod);
		}
		if (timeout != 0) {
			config.setTimeout(timeout);
		}

		if (!forceConnectionsSetting) {
			// Set connection num to 1 to avoid too many connections.
			config.setMaxTotalConnections(1);
			config.setMaxIdleConnections(1);
			config.setMinIdleConnections(1);
		} else {
			if (maxTotalConnections != 0) {
				config.setMaxTotalConnections(maxTotalConnections);
			}
			if (maxIdleConnections != 0) {
				config.setMaxIdleConnections(maxIdleConnections);
			}
			if (minIdleConnections != 0) {
				config.setMinIdleConnections(minIdleConnections);
			}
		}

		return new SpringDbPool(config);
	}

	public static Jedis getJedisFromClientPool(ClientPool clientPool, int getResourceMaxRetries) {
		Jedis jedis = clientPool.getResource();
		int retryCount = 0;

		while (jedis == null && retryCount < getResourceMaxRetries) {
			jedis = clientPool.getResource();
			retryCount++;
		}

		if (jedis == null) {
			throw new RuntimeException("Failed to get resource from clientPool after " +
					retryCount + " retries.");
		}
		return jedis;
	}
}
