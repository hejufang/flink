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

package org.apache.flink.connector.rocketmq.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * KeyByPartitionSerializationSchema.
 */
public class KeyByPartitionSerializationSchema extends KeyValueSerializationSchemaWrapper<RowData> {
	private final int[] keyByFields;

	public KeyByPartitionSerializationSchema(
			SerializationSchema<RowData> serializationSchema,
			int[] keyByFields) {
		super(serializationSchema);
		this.keyByFields = keyByFields;
	}

	@Override
	public String getPartitionKey(RowData tuple) {
		GenericRowData rowData = (GenericRowData) tuple;
		return Arrays.stream(keyByFields).mapToObj(
			i -> rowData.getField(i).toString()).collect(Collectors.joining());
	}
}
