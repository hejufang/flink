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

package org.apache.flink.connectors.redis;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An upsert {@link UpsertStreamTableSink} for redis.
 */
public class RedisUpsertTableSink implements UpsertStreamTableSink<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(RedisUpsertTableSink.class);

	private final TableSchema tableSchema;
	private final RedisOutputFormat outputFormat;

	public RedisUpsertTableSink(TableSchema tableSchema, RedisOutputFormat outputFormat) {
		this.tableSchema = tableSchema;
		this.outputFormat = outputFormat;
	}

	@Override
	public void setKeyFields(String[] keys) {
		// ignore query keys.
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		// redis always upsert on rowkey, even works in append only mode.
	}

	@Override
	public TypeInformation getRecordType() {
		return tableSchema.toRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public void emitDataStream(DataStream dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		DataStreamSink dataStreamSink = dataStream.addSink(new RedisSinkFunction(outputFormat))
			.setParallelism(dataStream.getParallelism())
			.name(TableConnectorUtils.generateRuntimeName(this.getClass(),
				tableSchema.getFieldNames()));

		int parallelism = outputFormat.getParallelism();
		if (parallelism > 0) {
			dataStreamSink = dataStreamSink.setParallelism(parallelism);
			LOG.info("Set parallelism to {} for redis/abase table sink.", parallelism);
		}
		return dataStreamSink;
	}

	@Override
	public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
		return null;
	}
}
