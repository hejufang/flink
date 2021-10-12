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

import redis.clients.jedis.Response;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * client pipeline.
 *
 * <p>All needed pipeline methods are defined here.
 */
public interface ClientPipeline extends AutoCloseable, Serializable {

	Response<String> set(final byte[] key, final byte[] value);

	Response<String> setex(final byte[] key, final int seconds, final byte[] value);

	Response<Long> hset(byte[] key, byte[] field, byte[] value);

	Response<String> hmset(byte[] key, Map<byte[], byte[]> hash);

	Response<Long> incrBy(final String key, final long value);

	Response<Double> incrByFloat(String key, double value);

	Response<Long> hincrBy(byte[] key, byte[] field, long value);

	Response<Double> hincrByFloat(String key, String field, double value);

	Response<Long> lpush(byte[] key, byte[]... args);

	Response<Long> sadd(byte[] key, byte[]... member);

	Response<Long> zadd(byte[] key, double score, byte[] member);

	Response<Long> del(final byte[] key);

	Response<Long> hdel(byte[] key, byte[]... field);

	Response<Long> hexpires(String key, int seconds);

	Response<Long> lexpires(String key, int seconds);

	Response<Long> sexpires(String key, int seconds);

	Response<Long> zexpires(String key, int seconds);

	List<Object> syncAndReturnAll();

}
