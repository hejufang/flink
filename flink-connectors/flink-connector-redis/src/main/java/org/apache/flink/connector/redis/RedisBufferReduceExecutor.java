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

package org.apache.flink.connector.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.redis.utils.ByteArrayWrapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An command executor used to buffer insert/update/delete
 * events, and reduce them in buffer before submit to abase/redis.
 */
public class RedisBufferReduceExecutor extends RedisBatchExecutor<Tuple2<byte[], byte[]>> {
	private static final long serialVersionUID = 1L;
	// Null value in a mapping indicates the associated key should be deleted.
	private final transient Map<ByteArrayWrapper, byte[]> reduceBuffer;
	private final ValueExtractor keyExtractor;
	private final ValueExtractor valueExtractor;

	public RedisBufferReduceExecutor(
			ExecuteFunction<Tuple2<byte[], byte[]>> execution,
			ValueExtractor keyExtractor,
			ValueExtractor valueExtractor) {
		super(execution);
		this.keyExtractor = keyExtractor;
		this.valueExtractor = valueExtractor;
		this.reduceBuffer = new HashMap<>();
	}

	@Override
	public void open() {}

	@Override
	public void addToBatch(RowData record) {
		byte[] key = keyExtractor.extract(record);
		ByteArrayWrapper keyBytes = new ByteArrayWrapper(key);
		if (record.getRowKind() == RowKind.DELETE) {
			// Indicates the associated key should be deleted.
			reduceBuffer.put(keyBytes, null);
		} else {
			byte[] valueBytes = valueExtractor.extract(record);
			reduceBuffer.put(keyBytes, valueBytes);
		}
	}

	@Override
	public List<Object> executeBatch(Pipeline pipeline) {
		reduceBuffer.forEach((key, value) ->
			execution.execute(pipeline, new Tuple2<>(key.getData(), value)));
		return pipeline.syncAndReturnAll();
	}

	@Override
	public void reset() {
		reduceBuffer.clear();
	}

	@Override
	public boolean isBufferEmpty() {
		return reduceBuffer.isEmpty();
	}

	/**
	 * Logic for get key/value bytes from RowData for writing into redis/abase.
	 */
	@FunctionalInterface
	public interface ValueExtractor {
		byte[] extract(RowData record);
	}
}