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

import com.bytedance.abase.AbasePipeline;
import redis.clients.jedis.Response;

import java.util.List;
import java.util.Map;

/**
 * abase pipeline wrapper.
 */
public class AbasePipelineWrapper implements ClientPipeline {

	private static final long serialVersionUID = 1L;

	private final AbasePipeline abasePipeline;

	public AbasePipelineWrapper(AbasePipeline abasePipeline) {
		this.abasePipeline = abasePipeline;
	}

	@Override
	public Response<String> set(byte[] key, byte[] value) {
		return abasePipeline.set(key, value);
	}

	@Override
	public Response<String> setex(byte[] key, int seconds, byte[] value) {
		return abasePipeline.setex(key, seconds, value);
	}

	@Override
	public Response<Long> hset(byte[] key, byte[] field, byte[] value) {
		return abasePipeline.hset(key, field, value);
	}

	@Override
	public Response<String> hmset(byte[] key, Map<byte[], byte[]> hash) {
		return abasePipeline.hmset(key, hash);
	}

	@Override
	public Response<Long> incrBy(String key, long value) {
		return abasePipeline.incrBy(key, value);
	}

	@Override
	public Response<Double> incrByFloat(String key, double value) {
		return abasePipeline.incrByFloat(key.getBytes(), value);
	}

	@Override
	public Response<Long> hincrBy(byte[] key, byte[] field, long value) {
		return abasePipeline.hincrBy(key, field, value);
	}

	@Override
	public Response<Double> hincrByFloat(String key, String field, double value) {
		return abasePipeline.hincrByFloat(key, field, value);
	}

	@Override
	public Response<Long> lpush(byte[] key, byte[]... args) {
		return abasePipeline.lpush(key, args);
	}

	@Override
	public Response<Long> sadd(byte[] key, byte[]... member) {
		return abasePipeline.sadd(key, member);
	}

	@Override
	public Response<Long> zadd(byte[] key, double score, byte[] member) {
		return abasePipeline.zadd(key, score, member);
	}

	@Override
	public Response<Long> del(byte[] key) {
		return abasePipeline.del(key);
	}

	@Override
	public Response<Long> hdel(byte[] key, byte[]... field) {
		return abasePipeline.hdel(key, field);
	}

	@Override
	public Response<Long> hexpires(String key, int seconds) {
		return abasePipeline.Hsetexpires(key, seconds);
	}

	@Override
	public Response<Long> hexpires(byte[] key, int seconds) {
		return abasePipeline.Hsetexpires(key, seconds);
	}

	@Override
	public Response<Long> lexpires(String key, int seconds) {
		return abasePipeline.Lsetexpires(key, seconds);
	}

	@Override
	public Response<Long> sexpires(String key, int seconds) {
		return abasePipeline.Ssetexpires(key, seconds);
	}

	@Override
	public Response<Long> zexpires(String key, int seconds) {
		return abasePipeline.Zsetexpires(key, seconds);
	}

	@Override
	public List<Object> syncAndReturnAll() {
		return abasePipeline.syncAndReturnAll();
	}

	@Override
	public void close() throws Exception {
		if (abasePipeline != null) {
			abasePipeline.close();
		}
	}
}
