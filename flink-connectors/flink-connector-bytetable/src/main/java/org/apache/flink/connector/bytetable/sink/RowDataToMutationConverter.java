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

package org.apache.flink.connector.bytetable.sink;

import org.apache.flink.connector.bytetable.util.ByteArrayWrapper;
import org.apache.flink.connector.bytetable.util.ByteTableSchema;
import org.apache.flink.connector.bytetable.util.ByteTableSerde;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.bytetable.RowMutation;
import org.apache.hadoop.hbase.client.Mutation;

import java.io.IOException;

/**
 * An implementation of {@link ByteTableMutationConverter} which converts {@link RowData} into {@link Mutation}.
 */
public class RowDataToMutationConverter implements ByteTableMutationConverter<RowData> {
	private static final long serialVersionUID = 1L;

	private final ByteTableSchema schema;
	private final String nullStringLiteral;
	private final long cellTTLMicroSeconds;
	private transient ByteTableSerde serde;

	public RowDataToMutationConverter(ByteTableSchema schema, final String nullStringLiteral, long cellTTLMicroSeconds) {
		this.schema = schema;
		this.nullStringLiteral = nullStringLiteral;
		this.cellTTLMicroSeconds = cellTTLMicroSeconds;
	}

	@Override
	public void open() {
		this.serde = new ByteTableSerde(schema, nullStringLiteral, cellTTLMicroSeconds);
	}

	@Override
	public RowMutation convertToMutation(RowData record) throws IOException {
		RowKind kind = record.getRowKind();
		if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
			return serde.createPutMutation(record);
		} else if (kind == RowKind.DELETE) {
			return serde.createDeleteMutation(record);
		}
		throw new FlinkRuntimeException("Unsupported row kind: " + record.getRowKind());
	}

	@Override
	public ByteArrayWrapper getRowKeyByteArrayWrapper(RowData record) {
		return serde.getRowKeyByteArrayWrapper(record);
	}
}
