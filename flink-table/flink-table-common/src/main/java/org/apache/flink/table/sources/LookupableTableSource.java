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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;

import java.util.Optional;

/**
 * A {@link TableSource} which supports for lookup accessing via key column(s).
 * For example, MySQL TableSource can implement this interface to support lookup accessing.
 * When temporal join this MySQL table, the runtime behavior could be in a lookup fashion.
 *
 * @param <T> type of the result
 */
@Experimental
public interface LookupableTableSource<T> extends TableSource<T> {

	/**
	 * Gets the {@link TableFunction} which supports lookup one key at a time.
	 * @param lookupKeys the chosen field names as lookup keys, it is in the defined order
	 */
	TableFunction<T> getLookupFunction(String[] lookupKeys);

	/**
	 * Gets the {@link AsyncTableFunction} which supports async lookup one key at a time.
	 * @param lookupKeys the chosen field names as lookup keys, it is in the defined order
	 */
	AsyncTableFunction<T> getAsyncLookupFunction(String[] lookupKeys);

	/**
	 * Returns true if async lookup is enabled.
	 *
	 * <p>The lookup function returned by {@link #getAsyncLookupFunction(String[])} will be
	 * used if returns true. Otherwise, the lookup function returned by
	 * {@link #getLookupFunction(String[])} will be used.
	 */
	boolean isAsyncEnabled();

	default int getLaterJoinMs() {
		// -1 means turn off later join.
		return -1;
	}

	/**
	 * Return a flag to indicate whether to hash the input stream by join key.
	 * Note that this flag is of higher priority than TABLE_EXEC_KEYBY_BEFORE_LOOKUP_JOIN
	 * config in ExecutionConfigOptions.
	 * The default value is an empty optional, which means the behavior is controlled by the
	 * TABLE_EXEC_KEYBY_BEFORE_LOOKUP_JOIN config.
	 */
	default Optional<Boolean> isInputKeyByEnabled() {
		return Optional.empty();
	}
}
