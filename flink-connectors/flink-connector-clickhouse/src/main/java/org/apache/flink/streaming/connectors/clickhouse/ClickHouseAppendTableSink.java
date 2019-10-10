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

package org.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * ClickHouse table sink function.
 */
public class ClickHouseAppendTableSink implements AppendStreamTableSink<Row>, BatchTableSink<Row> {
	private ClickHouseOutputFormat outputFormat;
	private String[] fieldNames;
	private TypeInformation[] fieldTypes;

	public ClickHouseAppendTableSink(ClickHouseOutputFormat clickHouseOutputFormat) {
		this.outputFormat = clickHouseOutputFormat;
		this.fieldNames = clickHouseOutputFormat.getTableSchema().getFieldNames();
		this.fieldTypes = clickHouseOutputFormat.getTableSchema().getFieldTypes();
	}

	public static ClickHouseAppendTableSinkBuilder builder() {
		return new ClickHouseAppendTableSinkBuilder();
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return dataStream
			.addSink(new ClickHouseSinkFunction(outputFormat))
			.setParallelism(dataStream.getParallelism())
			.name(TableConnectorUtils.generateRuntimeName(this.getClass(), fieldNames));
	}

	@Override
	public void emitDataSet(DataSet<Row> dataSet) {
		dataSet.output(outputFormat);
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		ClickHouseAppendTableSink copy;
		try {
			copy = new ClickHouseAppendTableSink(InstantiationUtil.clone(outputFormat));
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		copy.fieldNames = fieldNames;
		copy.fieldTypes = fieldTypes;
		return copy;
	}

	@VisibleForTesting
	ClickHouseOutputFormat getOutputFormat() {
		return outputFormat;
	}
}
