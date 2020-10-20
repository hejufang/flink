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

package org.apache.flink.connectors.bytesql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_SINK_PRIMARY_KEY_INDICES;

/**
 * An upsert {@link UpsertStreamTableSink} for ByteSQL.
 */
public class ByteSQLUpsertTableSink implements UpsertStreamTableSink<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(ByteSQLUpsertTableSink.class);
	private final TableSchema schema;
	private final ByteSQLOptions options;
	private final ByteSQLInsertOptions insertOptions;
	private boolean isAppendOnly;
	/** Key field indices provided by users in DDL.*/
	private int[] keyFieldIndices;

	public ByteSQLUpsertTableSink(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			TableSchema schema) {
		this.options = options;
		this.insertOptions = insertOptions;
		this.schema = schema;
		this.keyFieldIndices = insertOptions.getKeyFieldIndices();
	}

	@Override
	public void setKeyFields(String[] keys) {
		// Do nothing.
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		this.isAppendOnly = isAppendOnly;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		if (!isAppendOnly && (keyFieldIndices == null || keyFieldIndices.length == 0)) {
			throw new TableException(String.format("The table must be have unique key fields (Set it through '%s')" +
				" or be append-only.", CONNECTOR_SINK_PRIMARY_KEY_INDICES));
		}
		ByteSQLUpsertOutputFormat outputFormat = new ByteSQLUpsertOutputFormat(
			options,
			insertOptions,
			schema.getFieldNames(),
			keyFieldIndices,
			isAppendOnly
		);
		DataStreamSink<Tuple2<Boolean, Row>> dataStreamSink = dataStream
			.addSink(new ByteSQLUpsertSinkFunction(outputFormat))
			.setParallelism(dataStream.getParallelism())
			.name(TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames()));
		if (insertOptions.getParallelism() > 0) {
			dataStreamSink = dataStreamSink.setParallelism(insertOptions.getParallelism());
			LOG.info("Set parallelism to {} for ByteSQL sink", insertOptions.getParallelism());
		}
		return dataStreamSink;
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());
	}

	@Override
	public String[] getFieldNames() {
		return schema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return schema.getFieldTypes();
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
				"Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
				"But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		}

		ByteSQLUpsertTableSink copy = new ByteSQLUpsertTableSink(
			options, insertOptions, schema);
		copy.keyFieldIndices = keyFieldIndices;
		copy.isAppendOnly = isAppendOnly;
		return copy;
	}
}
