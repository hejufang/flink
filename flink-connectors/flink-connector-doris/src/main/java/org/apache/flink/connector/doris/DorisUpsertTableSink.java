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

package org.apache.flink.connector.doris;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * An append {@link UpsertStreamTableSink} for doris.
 */
public class DorisUpsertTableSink implements UpsertStreamTableSink<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(DorisUpsertTableSink.class);

	private final TableSchema tableSchema;
	private final DorisOptions dorisOptions;

	public DorisUpsertTableSink(TableSchema tableSchema, DorisOptions dorisOptions) {
		this.tableSchema = tableSchema;
		this.dorisOptions = dorisOptions;
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
	public TypeInformation<Row> getRecordType() {
		return tableSchema.toRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		DataStreamSink dataStreamSink = dataStream.addSink(new DorisSinkFunction(dorisOptions))
			.setParallelism(dataStream.getParallelism())
			.name(TableConnectorUtils.generateRuntimeName(this.getClass(),
				tableSchema.getFieldNames()));

		if (dorisOptions.getParallelism() > 0) {
			dataStreamSink = dataStreamSink.setParallelism(dorisOptions.getParallelism());
			LOG.info("Set parallelism to {} for doris sink.", dorisOptions.getParallelism());
		}
		return dataStreamSink;
	}

	@Override
	public String[] getFieldNames() {
		return tableSchema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return tableSchema.getFieldTypes();
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
				"Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
				"But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		}
		return new DorisUpsertTableSink(tableSchema, dorisOptions);
	}
}
