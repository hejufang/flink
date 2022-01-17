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

package org.apache.flink.connector.abase.executor;

import org.apache.flink.connector.abase.client.ClientPipeline;
import org.apache.flink.connector.abase.options.AbaseSinkOptions;
import org.apache.flink.connector.abase.utils.ByteArrayWrapper;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import redis.clients.jedis.exceptions.JedisDataException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * abase/redis hash type sink buffer reducer.
 */
public class AbaseSinkHashBufferReduceExecutor extends AbaseSinkBatchExecutor<RowData> {
	private static final long serialVersionUID = 1L;

	private final transient Map<ByteArrayWrapper, Map<ByteArrayWrapper, byte[]>> insertBuffer;
	private final transient Map<ByteArrayWrapper, Set<ByteArrayWrapper>> retractBuffer;

	private final RowData.FieldGetter[] fieldGetters;
	private final DynamicTableSink.DataStructureConverter converter;
	private final AbaseSinkOptions sinkOptions;

	public AbaseSinkHashBufferReduceExecutor(
			RowData.FieldGetter[] fieldGetters,
			DynamicTableSink.DataStructureConverter converter,
			AbaseSinkOptions sinkOptions) {
		super(null);
		this.fieldGetters = fieldGetters;
		this.converter = converter;
		this.sinkOptions = sinkOptions;
		this.insertBuffer = new HashMap<>();
		this.retractBuffer = new HashMap<>();
	}

	@Override
	public void addToBatch(RowData record) {
		ByteArrayWrapper key;
		if (record.getArity() == 2) {
			Row res = (Row) converter.toExternal(record);
			if (res == null) {
				throw new FlinkRuntimeException("Get null when convert to row: " + record);
			}
			Object keyObj = Objects.requireNonNull(res.getField(0), "Get null key with row: " + res);
			key = new ByteArrayWrapper(keyObj.toString().getBytes());
			Object hashMap = res.getField(1);
			if (hashMap == null) {
				throw new FlinkRuntimeException(String.format("The hashmap of %s should not be null.", key));
			}
			Map<ByteArrayWrapper, byte[]> vals = convertToReducedMap((Map<String, String>) hashMap);

			if (record.getRowKind() == RowKind.DELETE) {
				retractBuffer.compute(key, (k, v) -> {
					if (v == null) {
						v = new HashSet<>();
					}
					v.addAll(vals.keySet());
					return v;
				});
				insertBuffer.computeIfPresent(key, (k, v) -> {
					v.keySet().removeAll(vals.keySet());
					return v.isEmpty() ? null : v;
				});
			} else {
				retractBuffer.computeIfPresent(key, (k, v) -> {
					v.removeAll(vals.keySet());
					return v.isEmpty() ? null : v;
				});
				insertBuffer.compute(key, (k, v) -> {
					if (v == null) {
						return vals;
					}
					v.putAll(vals);
					return v;
				});
			}
		} else {
			Object keyObj = Objects.requireNonNull(fieldGetters[0].getFieldOrNull(record),
				"Get null key from record: " + record);
			key = new ByteArrayWrapper(keyObj.toString().getBytes());
			Object hashKey = fieldGetters[1].getFieldOrNull(record);
			Object hashValue = fieldGetters[2].getFieldOrNull(record);
			if (hashKey == null || hashValue == null) {
				throw new FlinkRuntimeException(
					String.format("Neither hash key nor hash value of %s should not be " +
						"null, the hash key: %s, the hash value: %s", keyObj, hashKey, hashValue));
			}
			if (record.getRowKind() == RowKind.DELETE) {
				retractBuffer.compute(key, (k, v) -> {
					if (v == null) {
						v = new HashSet<>();
					}
					v.add(new ByteArrayWrapper(hashKey.toString().getBytes()));
					return v;
				});
				insertBuffer.computeIfPresent(key, (k, v) -> {
					if (v.containsKey(new ByteArrayWrapper(hashKey.toString().getBytes()))) {
						return null;    // delete it
					} else {
						return v;
					}
				});
			} else {
				retractBuffer.computeIfPresent(key, (k, v) -> {
					if (v.contains(new ByteArrayWrapper(hashKey.toString().getBytes()))) {
						return null;
					} else {
						return v;
					}
				});
				insertBuffer.compute(key, (k, v) -> {
					Map<ByteArrayWrapper, byte[]> val;
					if (v == null) {
						val = new HashMap<>();
					} else {
						val = v;
					}
					val.put(new ByteArrayWrapper(hashKey.toString().getBytes()), hashValue.toString().getBytes());
					return val;
				});
			}
		}
	}

	@Override
	public List<Object> executeBatch(ClientPipeline pipeline) {
		// write insert records
		// prefer 'hmset' than 'hset' since the time complexity of both of them is O(1)
		insertBuffer.forEach((key, valmap) -> {
			if (valmap.size() >= 2) {
				pipeline.hmset(key.getData(), convertToValMap(valmap));
			} else {
				valmap.forEach((hashKey, hashVal) -> pipeline.hset(key.getData(), hashKey.getData(), hashVal));
			}
		});

		// handle retract stream
		retractBuffer.forEach((key, valSet) -> pipeline.hdel(key.getData(),
			valSet.stream().map(ByteArrayWrapper::getData).collect(Collectors.toSet()).toArray(new byte[][]{})));

		// commit pipeline
		List<Object> res = pipeline.syncAndReturnAll();
		boolean sucess = true;
		for (Object o : res) {
			if (o instanceof JedisDataException) {
				sucess = false;
				break;
			}
		}

		// handle ttl
		if (sucess && sinkOptions.getTtlSeconds() > 0) {
			insertBuffer.forEach((key, val) -> pipeline.hexpires(key.getData(), sinkOptions.getTtlSeconds()));
			res = pipeline.syncAndReturnAll();
		}
		return res;
	}

	@Override
	public void reset() {
		insertBuffer.clear();
		retractBuffer.clear();
	}

	@Override
	public boolean isBufferEmpty() {
		return insertBuffer.isEmpty() && retractBuffer.isEmpty();
	}

	private static Map<ByteArrayWrapper, byte[]> convertToReducedMap(Map<String, String> originMap) {
		Map<ByteArrayWrapper, byte[]> byteMap = new HashMap<>(originMap.size());
		for (Map.Entry<String, String> entry : originMap.entrySet()) {
			byteMap.put(new ByteArrayWrapper(entry.getKey().getBytes()), entry.getValue().getBytes());
		}
		return byteMap;
	}

	private static Map<byte[], byte[]> convertToValMap(Map<ByteArrayWrapper, byte[]> originMap) {
		Map<byte[], byte[]> byteMap = new HashMap<>(originMap.size());
		for (Map.Entry<ByteArrayWrapper, byte[]> entry : originMap.entrySet()) {
			byteMap.put(entry.getKey().getData(), entry.getValue());
		}
		return byteMap;
	}

}
