/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.abase;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/**
 * Sink function for writing into Abase.
 */
public class AbaseSinkFunction extends RichSinkFunction<RowData>
	implements CheckpointedFunction, SpecificParallelism {
	private final AbaseOutputFormat outputFormat;
	private final int parallelism;
	private final FlinkConnectorRateLimiter rateLimiter;

	public AbaseSinkFunction(
			AbaseOutputFormat outputFormat,
			int parallelism,
			@Nullable FlinkConnectorRateLimiter rateLimiter) {
		this.outputFormat = outputFormat;
		this.parallelism = parallelism;
		this.rateLimiter = rateLimiter;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		RuntimeContext ctx = getRuntimeContext();
		outputFormat.setRuntimeContext(ctx);
		outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}
	}

	@Override
	public void close() throws Exception {
		outputFormat.close();
		super.close();
	}

	@Override
	public void invoke(RowData value, Context context) throws Exception {
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		outputFormat.writeRecord(value);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		outputFormat.flush();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}
}

