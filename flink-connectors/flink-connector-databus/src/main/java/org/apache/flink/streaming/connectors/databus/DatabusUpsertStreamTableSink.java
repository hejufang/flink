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

package org.apache.flink.streaming.connectors.databus;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

/**
 * Databus upsert stream table sink.
 */
public class DatabusUpsertStreamTableSink implements UpsertStreamTableSink<Row> {

	private final TableSchema tableSchema;
	private final DatabusOptions<Tuple2<Boolean, Row>> databusOptions;

	public DatabusUpsertStreamTableSink(TableSchema tableSchema, DatabusOptions databusOptions) {
		this.tableSchema = tableSchema;
		this.databusOptions = databusOptions;
	}

	@Override
	public void setKeyFields(String[] keys) {
		// ignore.
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		// ignore.
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return tableSchema.toRowType();
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		DataStreamSink<Tuple2<Boolean, Row>> resultStream =
			dataStream.addSink(new DatabusSinkFunction<>(databusOptions));
		int parallelism = databusOptions.getParallelism();
		if (parallelism > 0) {
			resultStream = resultStream.setParallelism(parallelism);
		}
		return resultStream;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new DatabusUpsertStreamTableSink(new TableSchema(fieldNames, fieldTypes), databusOptions);
	}
}
