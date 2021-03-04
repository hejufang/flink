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

import java.io.Serializable;
import java.util.List;

/**
 * Executes commands in batch for incoming records.
 */
public abstract class RedisBatchExecutor<T> implements Serializable {
	protected ExecuteFunction<T> execution;
	public RedisBatchExecutor(ExecuteFunction<T> execution) {
		this.execution = execution;
	}

	public abstract void open();

	public abstract void addToBatch(RowData record);

	public abstract List<Object> executeBatch(Pipeline pipeline);

	public abstract void reset();

	/**
	 * Logic for processing a coming record.
	 */
	@FunctionalInterface
	public interface ExecuteFunction<E> {
		void execute(Pipeline pipeline, E record);
	}

	public abstract boolean isBufferEmpty();

}
