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
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.options.AbaseSinkMetricsOptions;
import org.apache.flink.connector.abase.options.AbaseSinkOptions;
import org.apache.flink.connector.abase.utils.ByteArrayWrapper;
import org.apache.flink.connector.abase.utils.KeyFormatterHelper;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
	private static final Logger LOG = LoggerFactory.getLogger(AbaseSinkHashBufferReduceExecutor.class);

	private final transient Map<ByteArrayWrapper, Map<ByteArrayWrapper, byte[]>> insertBuffer;
	private final transient Map<ByteArrayWrapper, Set<ByteArrayWrapper>> retractBuffer;

	private final DynamicTableSink.DataStructureConverter converter;
	private final AbaseNormalOptions normalOptions;
	private final AbaseSinkOptions sinkOptions;
	private final AbaseSinkMetricsOptions sinkMetricsOptions;

	public AbaseSinkHashBufferReduceExecutor(
			AbaseNormalOptions normalOptions,
			AbaseSinkOptions sinkOptions,
			AbaseSinkMetricsOptions sinkMetricsOptions,
			DynamicTableSink.DataStructureConverter converter) {
		super(null);
		this.converter = converter;
		this.normalOptions = normalOptions;
		this.sinkOptions = sinkOptions;
		this.sinkMetricsOptions = sinkMetricsOptions;
		this.insertBuffer = new HashMap<>();
		this.retractBuffer = new HashMap<>();
	}

	@Override
	public void addToBatch(RowData record) {
		ByteArrayWrapper key;
		Row row = (Row) converter.toExternal(record);
		if (row == null) {
			throw new FlinkRuntimeException("Get null when convert to row: " + record);
		}
		String keyStr = getKey(row);
		key = new ByteArrayWrapper(keyStr.getBytes());
		Map<ByteArrayWrapper, byte[]> val = new HashMap<>();
		if (normalOptions.isHashMap()) {
			Map<String, String> hashMap = (Map<String, String>) row.getField(sinkOptions.getValueColIndices()[0]);
			if (hashMap == null) {
				throw new FlinkRuntimeException(String.format("Get null map value of key %s and record is %s.", key, row));
			}
			try {
				addMetricsDataToMap(row, hashMap);
			} catch (Throwable t) {
				LOG.error("Failed to add metrics data to hash map", t);
			}
			val = new HashMap<>(hashMap.size());
			for (Map.Entry<String, String> entry : hashMap.entrySet()) {
				Objects.requireNonNull(entry.getKey(),
					"Get null map key at index " + sinkOptions.getValueColIndices()[0] + " of data " + row);
				if (entry.getValue() == null) {
					if (!sinkOptions.isIgnoreNull()) {
						throw new FlinkRuntimeException("Get null map value at index "
							+ sinkOptions.getValueColIndices()[0] + " of data " + row);
					}
					continue;
				}
				val.put(new ByteArrayWrapper(entry.getKey().getBytes()), entry.getValue().getBytes());
			}
		} else if (normalOptions.isSpecifyHashFields()) {
			for (int i : sinkOptions.getValueColIndices()) {
				Object fieldVal = row.getField(i);
				if (fieldVal == null) {
					if (!sinkOptions.isIgnoreNull()) {
						throw new FlinkRuntimeException("Get null value at index of " + i + " in the record: " + row);
					}
					continue;
				}
				String name = normalOptions.getFieldNames()[i];
				val.put(new ByteArrayWrapper(name.getBytes()), fieldVal.toString().getBytes());
			}
		} else {
			Object hashKey = row.getField(sinkOptions.getValueColIndices()[0]);
			Object hashValue = row.getField(sinkOptions.getValueColIndices()[1]);
			if (hashKey == null) {
				throw new FlinkRuntimeException("Get null hash field name, hash key is " + keyStr
					+ " and record is " + row);
			}
			if (hashValue == null) {
				if (!sinkOptions.isIgnoreNull()) {
					throw new FlinkRuntimeException("Get null hash field value, hash key is " + keyStr
						+ ", field name is " + hashKey + " and  record is " + row);
				}
			} else {
				val.put(new ByteArrayWrapper(hashKey.toString().getBytes()), hashValue.toString().getBytes());
			}
		}
		addToBuffer(key, val, record.getRowKind());
	}

	private void addToBuffer(ByteArrayWrapper key, Map<ByteArrayWrapper, byte[]> val, RowKind rowKind) {
		if (rowKind == RowKind.DELETE) {
			retractBuffer.compute(key, (k, v) -> {
				if (v == null) {
					v = new HashSet<>();
				}
				v.addAll(val.keySet());
				return v;
			});
			insertBuffer.computeIfPresent(key, (k, v) -> {
				v.keySet().removeAll(val.keySet());
				return v.isEmpty() ? null : v;
			});
		} else {
			retractBuffer.computeIfPresent(key, (k, v) -> {
				v.removeAll(val.keySet());
				return v.isEmpty() ? null : v;
			});
			insertBuffer.compute(key, (k, v) -> {
				if (v == null) {
					return val;
				}
				v.putAll(val);
				return v;
			});
		}
	}

	@Override
	public List<Object> executeBatch(ClientPipeline pipeline) {
		// write records to be inserted
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

	private String getKey(Row row) {
		int[] indices = normalOptions.getKeyIndices();
		Object[] keys = new Object[indices.length];
		for (int i = 0; i < indices.length; i++) {
			keys[i] = Objects.requireNonNull(row.getField(indices[i]),
				"Get null key at index " + indices[i] + " with row: " + row);
		}
		return KeyFormatterHelper.formatKey(normalOptions.getKeyFormatter(), keys);
	}

	private void addMetricsDataToMap(Row row, Map<String, String> map) {
		if (sinkMetricsOptions.isCollected() && sinkMetricsOptions.isEventTsWriteable()) {
			Object val = Objects.requireNonNull(row.getField(sinkMetricsOptions.getEventTsColIndex()),
				"Get null value of event-ts column of index " + sinkMetricsOptions.getEventTsColIndex()
					+ ", data is " + row);
			map.putIfAbsent(sinkMetricsOptions.getEventTsColName(), val.toString());
		}
		if (sinkMetricsOptions.isCollected() && sinkMetricsOptions.isTagWriteable()) {
			Iterator<String> nameIter = sinkMetricsOptions.getTagNames().iterator();
			Iterator<Integer> idxIter = sinkMetricsOptions.getTagNameIndices().iterator();
			while (nameIter.hasNext()) {
				String tagName = nameIter.next();
				int tagIdx = idxIter.next();
				Object val = Objects.requireNonNull(row.getField(tagIdx),
					"Get null value of tag column, column name is " + tagName + ", column index is " + tagIdx
						+ " and data is " + row);
				map.putIfAbsent(tagName, val.toString());
			}
		}
	}

	private static Map<byte[], byte[]> convertToValMap(Map<ByteArrayWrapper, byte[]> originMap) {
		Map<byte[], byte[]> byteMap = new HashMap<>(originMap.size());
		for (Map.Entry<ByteArrayWrapper, byte[]> entry : originMap.entrySet()) {
			byteMap.put(entry.getKey().getData(), entry.getValue());
		}
		return byteMap;
	}

}
