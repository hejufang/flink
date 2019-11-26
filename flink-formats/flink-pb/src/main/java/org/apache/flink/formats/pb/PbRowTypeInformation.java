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

package org.apache.flink.formats.pb;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.Optional;

/**
 * Generate Row type information according to pb descriptors.
 */

public class PbRowTypeInformation {
	public static TypeInformation<Row> generateRow(Descriptors.Descriptor root) {
		return generateRow(root, Optional.empty());
	}

	public static TypeInformation<Row> generateRow(Descriptors.Descriptor root,
		Optional<Integer> timestemp) {
		return generateRow(root, timestemp, false);
	}

	public static TypeInformation<Row> generateRow(Descriptors.Descriptor root,
		Optional<Integer> timestemp, boolean withWrapper) {
		int size = root.getFields().size();
		TypeInformation[] types = new TypeInformation[size];
		String[] rowFieldNames = new String[size];
		int timestampIndex = timestemp.orElse(-1);

		for (int i = 0; i < size; i++) {
			Descriptors.FieldDescriptor field = root.getFields().get(i);
			rowFieldNames[i] = field.getName();
			types[i] = i == timestampIndex ? Types.SQL_TIMESTAMP : generateFieldTypeInformation(field);
		}

		RowTypeInfo pbTypeInfo = new RowTypeInfo(types, rowFieldNames);

		if (withWrapper) {
			pbTypeInfo = new RowTypeInfo(new RowTypeInfo[]{pbTypeInfo},
				new String[]{PbConstant.FORMAT_PB_WRAPPER_NAME});
		}

		return pbTypeInfo;
	}

	public static TypeInformation<?> generateFieldTypeInformation(Descriptors.FieldDescriptor field) {
		JavaType fieldType = field.getJavaType();

		TypeInformation typeInfo;
		if (fieldType.equals(JavaType.MESSAGE)) {
			if (field.isMapField()) {
				return Types.MAP(generateFieldTypeInformation(field.getMessageType().findFieldByName(PbConstant.KEY)),
					generateFieldTypeInformation(field.getMessageType().findFieldByName(PbConstant.VALUE)));
			} else if (field.isRepeated()) {
				return ObjectArrayTypeInfo.getInfoFor(generateRow(field.getMessageType()));
			} else {
				return generateRow(field.getMessageType());
			}
		} else {
			if (fieldType.equals(JavaType.STRING)) {
				typeInfo = Types.STRING;
			} else if (fieldType.equals(JavaType.LONG)) {
				typeInfo = Types.LONG;
			} else if (fieldType.equals(JavaType.BOOLEAN)) {
				typeInfo = Types.BOOLEAN;
			} else if (fieldType.equals(JavaType.INT)) {
				typeInfo = Types.INT;
			} else if (fieldType.equals(JavaType.DOUBLE)) {
				typeInfo = Types.DOUBLE;
			} else if (fieldType.equals(JavaType.FLOAT)) {
				typeInfo = Types.FLOAT;
			} else if (fieldType.equals(JavaType.ENUM)) {
				typeInfo = Types.STRING;
			} else if (fieldType.equals(JavaType.BYTE_STRING)) {
				typeInfo = Types.PRIMITIVE_ARRAY(Types.BYTE);
			} else {
				typeInfo = Types.STRING;
			}
			if (field.isRepeated()) {
				return ObjectArrayTypeInfo.getInfoFor(typeInfo);
			}
			return typeInfo;
		}
	}
}
