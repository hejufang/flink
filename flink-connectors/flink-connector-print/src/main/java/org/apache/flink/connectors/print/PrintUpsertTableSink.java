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

package org.apache.flink.connectors.print;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

/**
 * An {@link UpsertStreamTableSink} to print records to stdout.
 */
public class PrintUpsertTableSink implements UpsertStreamTableSink<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(PrintUpsertTableSink.class);

	private final TableSchema tableSchema;
	private final double sampleRatio;
	private final int parallelism;

	public PrintUpsertTableSink(TableSchema tableSchema, double sampleRatio) {
		this(tableSchema, sampleRatio, -1);
	}

	public PrintUpsertTableSink(TableSchema tableSchema, double sampleRatio, int parallelism) {
		this.tableSchema = tableSchema;
		this.sampleRatio = sampleRatio;
		this.parallelism = parallelism;
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
	public TypeInformation<Row> getRecordType() {
		return tableSchema.toRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new PrintUpsertTableSink(new TableSchema(fieldNames, fieldTypes),
			this.sampleRatio, this.parallelism);
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		double ratio = sampleRatio;
		if (ratio < 1) {
			dataStream =
				dataStream.filter(value -> ThreadLocalRandom.current().nextDouble() < ratio);
		}
		if (parallelism > 0) {
			LOG.info("Set parallelism to {} for print table sink", parallelism);
			return dataStream.print().setParallelism(parallelism);
		}
		return dataStream.print();
	}
}
