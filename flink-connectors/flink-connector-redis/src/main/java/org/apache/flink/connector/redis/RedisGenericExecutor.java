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

import org.apache.flink.table.data.RowData;

import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * An command executor used to buffer insert/update/delete
 * events and then submit to abase/redis.
 */
public class RedisGenericExecutor extends RedisBatchExecutor<RowData> {
	private static final long serialVersionUID = 1L;
	private final transient List<RowData> recordBuffer;
	public RedisGenericExecutor(ExecuteFunction<RowData> execution) {
		super(execution);
		this.recordBuffer = new ArrayList<>();
	}

	@Override
	public void open() {}

	@Override
	public void addToBatch(RowData record) {
		recordBuffer.add(record);
	}

	@Override
	public List<Object> executeBatch(Pipeline pipeline) {
		recordBuffer.forEach(record ->
			execution.execute(pipeline, record));
		return pipeline.syncAndReturnAll();
	}

	@Override
	public void reset() {
		recordBuffer.clear();
	}

	@Override
	public boolean isBufferEmpty() {
		return recordBuffer.isEmpty();
	}
}
