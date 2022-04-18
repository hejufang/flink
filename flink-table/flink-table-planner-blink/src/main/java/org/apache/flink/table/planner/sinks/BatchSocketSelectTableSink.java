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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.socket.SocketResultIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.sink.TaskPushSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.sink.TaskPushStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectTableSink;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * Batch select table sink for socket.
 */
public class BatchSocketSelectTableSink implements SelectTableSink, StreamTableSink<Row> {
	private static final int DEFAULT_MAX_RESULTS_PER_BATCH = 4096;
	private final TableSchema tableSchema;
	private final TaskPushSinkOperatorFactory<Row> factory;
	private final TypeSerializer<Row> typeSerializer;

	public BatchSocketSelectTableSink(TableSchema tableSchema) {
		this.tableSchema = SelectTableSinkSchemaConverter.convertTimeAttributeToRegularTimestamp(
			SelectTableSinkSchemaConverter.changeDefaultConversionClass(tableSchema));

		this.typeSerializer = (TypeSerializer<Row>) TypeInfoDataTypeConverter
			.fromDataTypeToTypeInfo(this.tableSchema.toRowDataType())
			.createSerializer(new ExecutionConfig());

		this.factory = new TaskPushSinkOperatorFactory<>(typeSerializer, DEFAULT_MAX_RESULTS_PER_BATCH);
	}

	@Override
	public DataType getConsumedDataType() {
		return tableSchema.toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public void setJobClient(JobClient jobClient) {
		CloseableIterator<Row> resultIterator = jobClient.getJobResultIterator(Thread.currentThread().getContextClassLoader());
		((SocketResultIterator<Row>) resultIterator).registerResultSerializer(typeSerializer);
	}

	/**
	 * This won't be used when client submits job with socket.
	 *
	 * @return the throwing closeable iterator.
	 */
	@Override
	public CloseableIterator<Row> getResultIterator() {
		return new SocketThrowingCloseableIterator();
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		TaskPushStreamSink<Row> sink = new TaskPushStreamSink<>(dataStream, factory);
		dataStream.getExecutionEnvironment().addOperator(sink.getTransformation());
		return sink.name("Socket select table sink");
	}

	/**
	 * Throwing closeable iterator for socket.
	 */
	private static class SocketThrowingCloseableIterator implements CloseableIterator<Row> {
		@Override
		public void close() throws Exception { }

		@Override
		public boolean hasNext() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Row next() {
			throw new UnsupportedOperationException();
		}
	}
}
