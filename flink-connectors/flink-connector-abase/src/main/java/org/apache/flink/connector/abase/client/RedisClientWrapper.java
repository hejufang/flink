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

package org.apache.flink.connector.abase.client;

import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.utils.AbaseClientTableUtils;

import com.bytedance.redis.RedisClient;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * redis client wrapper.
 */
public class RedisClientWrapper implements BaseClient {

	private static final long serialVersionUID = 1L;

	private transient RedisClient redisClient;
	private final AbaseNormalOptions normalOptions;

	public RedisClientWrapper(AbaseNormalOptions normalOptions) {
		this.normalOptions = normalOptions;
	}

	@Override
	public void open() {
		this.redisClient = AbaseClientTableUtils.getRedisClient(normalOptions);
	}

	@Override
	public void close() throws Exception {
		if (this.redisClient != null) {
			this.redisClient.close();
		}
	}

	@Override
	public byte[] get(byte[] key) {
		return this.redisClient.get(key);
	}

	@Override
	public String get(String key) {
		return this.redisClient.get(key);
	}

	@Override
	public List<String> hmget(String key, String... fields) {
		return this.redisClient.hmget(key, fields);
	}

	@Override
	public List<String> lrange(String key, long start, long end) {
		return this.redisClient.lrange(key, start, end);
	}

	@Override
	public Set<String> smembers(String key) {
		return this.redisClient.smembers(key);
	}

	@Override
	public Set<String> zrange(String key, long start, long end) {
		return this.redisClient.zrange(key, start, end);
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		return this.redisClient.hgetAll(key);
	}

	@Override
	public ClientPipeline pipelined() {
		return new RedisPipelineWrapper(this.redisClient.pipelined());
	}
}
