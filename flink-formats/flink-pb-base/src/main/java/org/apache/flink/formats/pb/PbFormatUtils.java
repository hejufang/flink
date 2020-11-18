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

package org.apache.flink.formats.pb;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.Descriptors;

/**
 * PbFormatUtils.
 */
public class PbFormatUtils {
	public static FieldsDataType createDataType(Descriptors.Descriptor root, boolean withWrapper) {
		int size = root.getFields().size();
		DataTypes.Field[] rowFields = new DataTypes.Field[size];

		for (int i = 0; i < size; i++) {
			Descriptors.FieldDescriptor field = root.getFields().get(i);
			String fieldName = field.getName();
			DataType dataType = createFieldDataType(field);
			rowFields[i] = DataTypes.FIELD(fieldName, dataType);
		}

		FieldsDataType dataType = (FieldsDataType) DataTypes.ROW(rowFields);

		if (withWrapper) {
			dataType = (FieldsDataType) DataTypes.ROW(DataTypes.FIELD(PbConstant.FORMAT_PB_WRAPPER_NAME, dataType));
		}

		return dataType;
	}

	public static DataType createFieldDataType(Descriptors.FieldDescriptor field) {
		Descriptors.FieldDescriptor.JavaType fieldType = field.getJavaType();

		DataType dataType;
		if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
			if (field.isMapField()) {
				return DataTypes.MAP(
					createFieldDataType(field.getMessageType().findFieldByName(PbConstant.KEY)),
					createFieldDataType(field.getMessageType().findFieldByName(PbConstant.VALUE)));
			} else if (field.isRepeated()) {
				return DataTypes.ARRAY(createDataType(field.getMessageType(), false));
			} else {
				return createDataType(field.getMessageType(), false);
			}
		} else {
			if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.STRING)) {
				dataType = DataTypes.STRING();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.LONG)) {
				dataType = DataTypes.BIGINT();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.BOOLEAN)) {
				dataType = DataTypes.BOOLEAN();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.INT)) {
				dataType = DataTypes.INT();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.DOUBLE)) {
				dataType = DataTypes.DOUBLE();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.FLOAT)) {
				dataType = DataTypes.FLOAT();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {
				dataType = DataTypes.STRING();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.BYTE_STRING)) {
				dataType = DataTypes.BYTES();
			} else {
				throw new FlinkRuntimeException(String.format("Unsupported fieldType: %s.", fieldType));
			}
			if (field.isRepeated()) {
				return DataTypes.ARRAY(dataType);
			}
			return dataType;
		}
	}
}
