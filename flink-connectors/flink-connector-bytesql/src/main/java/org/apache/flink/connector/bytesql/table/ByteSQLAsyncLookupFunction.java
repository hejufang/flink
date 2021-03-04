/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.bytesql.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An async lookup function for reading from ByteSQL.
 */
public class ByteSQLAsyncLookupFunction extends AsyncTableFunction<RowData> {
	private static final long serialVersionUID = 1L;
	private final ByteSQLLookupExecutor lookupExecutor;
	private final int concurrency;
	private transient ExecutorService executor;

	public ByteSQLAsyncLookupFunction(ByteSQLLookupExecutor lookupExecutor, int concurrency) {
		this.lookupExecutor = lookupExecutor;
		this.concurrency = concurrency;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		lookupExecutor.init(context);
		executor = Executors.newFixedThreadPool(concurrency);
	}

	public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object... inputs) {
		CompletableFuture.runAsync(() -> {
			try {
				resultFuture.complete(lookupExecutor.doLookup(inputs));
			}
			catch (Throwable e) {
				resultFuture.completeExceptionally(e);
			}}, executor);
	}

	@Override
	public void close() throws Exception {
		if (executor != null && !executor.isShutdown()) {
			executor.shutdown();
		}
		lookupExecutor.close();
	}
}
