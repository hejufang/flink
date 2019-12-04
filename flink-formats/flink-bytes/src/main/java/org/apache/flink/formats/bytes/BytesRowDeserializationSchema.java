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

package org.apache.flink.formats.bytes;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Deserialization bytes to Row.
 */
public class BytesRowDeserializationSchema implements DeserializationSchema<Row> {
	TypeInformation<Row> typeInformation;

	public BytesRowDeserializationSchema(TypeInformation<Row> typeInformation) {
		this.typeInformation = typeInformation;
	}

	@Override
	public Row deserialize(byte[] message) {
		RowTypeInfo typeInfo = (RowTypeInfo) typeInformation;
		if (typeInfo.getArity() > 1) {
			//contains time field.
			if (PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.canEqual(typeInfo.getTypeAt(0))) {
				// the first field is bytes.
				return Row.of(message, new Timestamp(System.currentTimeMillis()));
			} else {
				// the second field is bytes.
				return Row.of(new Timestamp(System.currentTimeMillis()), message);
			}
		}
		return Row.of(message);
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInformation;
	}
}
