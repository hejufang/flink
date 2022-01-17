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

import com.bytedance.redis.RedisPipeline;
import redis.clients.jedis.Response;

import java.util.List;
import java.util.Map;

/**
 * redis pipeline wrapper.
 */
public class RedisPipelineWrapper implements ClientPipeline {

	private static final long serialVersionUID = 1L;

	private final RedisPipeline redisPipeline;

	public RedisPipelineWrapper(RedisPipeline redisPipeline) {
		this.redisPipeline = redisPipeline;
	}

	@Override
	public Response<String> set(byte[] key, byte[] value) {
		return this.redisPipeline.set(key, value);
	}

	@Override
	public Response<String> setex(byte[] key, int seconds, byte[] value) {
		return this.redisPipeline.setex(key, seconds, value);
	}

	@Override
	public Response<Long> hset(byte[] key, byte[] field, byte[] value) {
		return this.redisPipeline.hset(key, field, value);
	}

	@Override
	public Response<String> hmset(byte[] key, Map<byte[], byte[]> hash) {
		return this.redisPipeline.hmset(key, hash);
	}

	@Override
	public Response<Long> incrBy(String key, long value) {
		return this.redisPipeline.incrBy(key, value);
	}

	@Override
	public Response<Double> incrByFloat(String key, double value) {
		return this.redisPipeline.incrByFloat(key, value);
	}

	@Override
	public Response<Long> hincrBy(byte[] key, byte[] field, long value) {
		return redisPipeline.hincrBy(key, field, value);
	}

	@Override
	public Response<Double> hincrByFloat(String key, String field, double value) {
		return redisPipeline.hincrByFloat(key, field, value);
	}

	@Override
	public Response<Long> lpush(byte[] key, byte[]... args) {
		return redisPipeline.lpush(key, args);
	}

	@Override
	public Response<Long> sadd(byte[] key, byte[]... member) {
		return redisPipeline.sadd(key, member);
	}

	@Override
	public Response<Long> zadd(byte[] key, double score, byte[] member) {
		return redisPipeline.zadd(key, score, member);
	}

	@Override
	public Response<Long> del(byte[] key) {
		return redisPipeline.del(key);
	}

	@Override
	public Response<Long> hdel(byte[] key, byte[]... field) {
		return redisPipeline.hdel(key, field);
	}

	@Override
	public Response<Long> hexpires(String key, int seconds) {
		return redisPipeline.expire(key, seconds);
	}

	@Override
	public Response<Long> hexpires(byte[] key, int seconds) {
		return redisPipeline.expire(key, seconds);
	}

	@Override
	public Response<Long> lexpires(String key, int seconds) {
		return redisPipeline.expire(key, seconds);
	}

	@Override
	public Response<Long> sexpires(String key, int seconds) {
		return redisPipeline.expire(key, seconds);
	}

	@Override
	public Response<Long> zexpires(String key, int seconds) {
		return redisPipeline.expire(key, seconds);
	}

	@Override
	public List<Object> syncAndReturnAll() {
		return redisPipeline.syncAndReturnAll();
	}

	@Override
	public void close() throws Exception {
		if (this.redisPipeline != null) {
			this.redisPipeline.close();
		}
	}
}
