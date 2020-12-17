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

package org.apache.flink.table.serialization;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * A SerializationSchema which returns null for the delete msg.
 * 1. if rowData.getRowKind() == RowKind.DELETE, return null.
 * 2. else serialize Tuple2.f1 data with innerSerializationSchema.
 * This class usually used when we want to filter the delete messages in table/sql.
 */
@Public
public class IgnoreDeleteMsgSerializationSchema implements SerializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	private final SerializationSchema<RowData> innerSerializationSchema;

	public IgnoreDeleteMsgSerializationSchema(SerializationSchema<RowData> innerSerializationSchema) {
		this.innerSerializationSchema = innerSerializationSchema;
	}

	@Override
	public byte[] serialize(RowData rowData) {
		if (rowData.getRowKind() == RowKind.DELETE) {
			return null;
		}
		return innerSerializationSchema.serialize(rowData);
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		innerSerializationSchema.open(context);
	}
}
