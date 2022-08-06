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

package org.apache.flink.formats.protobuf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtobufInternalUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Protobuf function util.
 */
public class PbFormatUtils {

	private static String getJavaPackageFromProtoFile(Descriptors.Descriptor descriptor) {
		boolean hasJavaPackage = descriptor.getFile().getOptions().hasJavaPackage();
		if (hasJavaPackage) {
			String javaPackage = descriptor.getFile().getOptions().getJavaPackage();
			if (StringUtils.isBlank(javaPackage)) {
				throw new FlinkRuntimeException("java_package cannot be blank string");
			}
			return javaPackage;
		} else {
			String packageName = descriptor.getFile().getPackage();
			if (StringUtils.isBlank(packageName)) {
				throw new FlinkRuntimeException("package and java_package cannot both be empty");
			}
			return packageName;
		}
	}

	public static String getFullJavaName(Descriptors.Descriptor descriptor) {
		String javaPackageName = getJavaPackageFromProtoFile(descriptor);
		if (descriptor.getFile().getOptions().getJavaMultipleFiles()) {
			// multiple_files=true
			if (null != descriptor.getContainingType()) {
				// nested type
				String parentJavaFullName = getFullJavaName(descriptor.getContainingType());
				return parentJavaFullName + "." + descriptor.getName();
			} else {
				// top level message
				return javaPackageName + "." + descriptor.getName();
			}
		} else {
			// multiple_files=false
			if (null != descriptor.getContainingType()) {
				// nested type
				String parentJavaFullName = getFullJavaName(descriptor.getContainingType());
				return parentJavaFullName + "." + descriptor.getName();
			} else {
				// top level message
				if (!descriptor.getFile().getOptions().hasJavaOuterClassname()) {
					// user do not define outer class name in proto file
					return javaPackageName
						+ "."
						+ descriptor.getName()
						+ PbConstant.PB_OUTER_CLASS_SUFFIX
						+ "."
						+ descriptor.getName();

				} else {
					String outerName = descriptor.getFile().getOptions().getJavaOuterClassname();
					// user define outer class name in proto file
					return javaPackageName + "." + outerName + "." + descriptor.getName();
				}
			}
		}
	}

	public static String getFullJavaName(Descriptors.EnumDescriptor enumDescriptor) {
		if (null != enumDescriptor.getContainingType()) {
			return getFullJavaName(enumDescriptor.getContainingType())
				+ "."
				+ enumDescriptor.getName();
		} else {
			return enumDescriptor.getFullName();
		}
	}

	public static boolean isSimpleType(LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
				return true;
			default:
				return false;
		}
	}

	public static String getStrongCamelCaseJsonName(String name) {
		return ProtobufInternalUtils.underScoreToCamelCase(name, true);
	}

	public static Descriptors.Descriptor getDescriptor(String className) {
		try {
			Class<?> pbClass =
				Class.forName(className, true, Thread.currentThread().getContextClassLoader());
			return (Descriptors.Descriptor)
				pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
		} catch (Exception y) {
			throw new IllegalArgumentException(
				String.format("get %s descriptors error!", className), y);
		}
	}

	public static boolean isRepeatedType(LogicalType type) {
		return type instanceof MapType || type instanceof ArrayType;
	}

	public static boolean isArrayType(LogicalType type) {
		return type instanceof ArrayType;
	}

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
					createFieldDataType(field.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME)),
					createFieldDataType(field.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME)));
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
