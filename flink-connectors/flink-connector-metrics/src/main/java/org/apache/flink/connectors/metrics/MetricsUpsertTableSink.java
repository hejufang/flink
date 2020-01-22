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

package org.apache.flink.connectors.metrics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * Metrics upsert table sink.
 */
public class MetricsUpsertTableSink implements UpsertStreamTableSink<Row> {

	private MetricsOptions metricsOptions;

	public MetricsUpsertTableSink(MetricsOptions metricsOptions) {
		this.metricsOptions = metricsOptions;
	}

	@Override
	public void setKeyFields(String[] keys) {
		// ignore key fields.
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		// ignore the append only flag.
	}

	@Override
	public TableSchema getTableSchema() {
		String[] filedName = new String[]{"type", "name", "value", "tags"};
		DataType[] dataTypes = new DataType[]{DataTypes.STRING(), DataTypes.STRING(),
			DataTypes.DOUBLE(), DataTypes.STRING()};
		return TableSchema.builder().fields(filedName, dataTypes).build();
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return Types.ROW(Types.STRING, Types.STRING, Types.DOUBLE, Types.STRING);
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		DataStreamSink<Tuple2<Boolean, Row>> dataStreamSink =
			dataStream.addSink(new MetricsUpsertSinkFunction(metricsOptions));
		int parallelism = metricsOptions.getParallelism();
		if (parallelism > 0) {
			dataStreamSink.setParallelism(parallelism);
		}
		return dataStreamSink;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new MetricsUpsertTableSink(metricsOptions);
	}
}
