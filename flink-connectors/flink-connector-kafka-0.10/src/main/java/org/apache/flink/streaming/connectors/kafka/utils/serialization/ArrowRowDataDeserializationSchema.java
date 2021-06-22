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

package org.apache.flink.streaming.connectors.kafka.utils.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.VarBinaryType;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Deserialize arrow table bytes from consumer record.
 */
public class ArrowRowDataDeserializationSchema implements KafkaDeserializationSchema<RowData> {

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public RowData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		GenericRowData genericRowData = new GenericRowData(1);
		genericRowData.setField(0, record.getFeatherSerializedBytes());
		return genericRowData;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return new RowDataTypeInfo(new VarBinaryType());
	}
}
