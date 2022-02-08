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

package org.apache.flink.connector.rpc.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connector.rpc.thrift.ThriftUtil.isPrimitivePackageClass;
import static org.apache.flink.connector.rpc.util.ObjectUtil.getClzAndGenericTypes;

/**
 * Utils for generate FieldsDataType for inferred schema.
 */
public class DataTypeUtil {
	/**
	 * Use thrift class information to generate RowTypeInfo to construct DDL.
	 * Breakup request class to generate the first partition of DDL. And then
	 * use response class simple name and a RowTypeInfo to generate the second
	 * partition of DDL.
	 * @param requestClass the RPC request class.
	 * @param responseClass the RPC response class.
	 * @return RowTypeInfo to construct table schema.
	 */
	public static FieldsDataType generateFieldsDataType(Class<?> requestClass, Class<?> responseClass) {
		Field[] fields = requestClass.getFields();
		/* The last field generate by thrift is metaDataMap which contains the thrift fields information.
		 * Thrift version 0.13.0 is suitable for this implementation. Because we use reflect to get fields
		 * information. So we doesn't need last field. We construct dimension DDL with break up request class
		 * which number is size-1 and response class at last which use one ROW. So the final number is
		 * (fields.length - 1) + 1 */
		int size = fields.length;
		DataTypes.Field[] rowFields = new DataTypes.Field[size];
		for (int i = 0; i < size - 1; i++) {
			Field field = fields[i];
			DataType dataType = generateFieldDataType(field.getGenericType(), new HashSet<>());
			rowFields[i] = DataTypes.FIELD(field.getName(), dataType);
		}
		DataType responseDataType = generateFieldsDataType(responseClass, new HashSet<>());
		rowFields[size - 1] = DataTypes.FIELD(responseClass.getSimpleName(), responseDataType);
		return (FieldsDataType) DataTypes.ROW(rowFields);
	}

	/**
	 * Use thrift class information to generate RowTypeInfo to construct DDL.
	 * We use reflect to get class fields information such as field name and
	 * field type.
	 * @param clz the thrift generated java class
	 * @return RowTypeInfo construct by class corresponding fields.
	 */
	public static FieldsDataType generateFieldsDataType(Class<?> clz, Set<String> processedTypes) {
		processedTypes.add(clz.getTypeName());
		Field[] fields = clz.getFields();
		/* The last field generate by thrift is metaDataMap which contains the thrift fields information.
		 * Thrift version 0.13.0 is suitable for this implementation. Because we use reflect to get fields
		 * information and use reflect to set field. So we doesn't need last field. */
		int size = fields.length - 1;
		DataTypes.Field[] rowFields = new DataTypes.Field[size];
		for (int i = 0; i < size; i++) {
			Field field = fields[i];
			DataType dataType = generateFieldDataType(field.getGenericType(), processedTypes);
			rowFields[i] = DataTypes.FIELD(field.getName(), dataType);
		}
		processedTypes.remove(clz.getTypeName());
		return (FieldsDataType) DataTypes.ROW(rowFields);
	}

	private static DataType generateFieldDataType(Type type, Set<String> processedTypes) {
		DataType dataType;
		Tuple2<Class<?>, Type[]> infos = getClzAndGenericTypes(type);
		Class<?> clz = infos.f0;
		Type[] innerTypes = infos.f1;
		if (processedTypes.contains(clz.getTypeName())) {
				throw new IllegalArgumentException(String.format("Struct %s is self-contained, please turn off auto " +
					"schema inferring.", clz.getTypeName()));
			}
		if (clz.isPrimitive() || isPrimitivePackageClass(clz)) {
			dataType = generatePrimitiveDataType(clz);
		} else if (clz.equals(List.class) || clz.equals(Set.class)) {
			dataType = DataTypes.ARRAY(generateFieldDataType(innerTypes[0], processedTypes));
		} else if (clz.equals(Map.class)) {
			dataType = DataTypes.MAP(generateFieldDataType(innerTypes[0], processedTypes),
				generateFieldDataType(innerTypes[1], processedTypes));
		} else if (clz.isEnum()) {
			dataType = DataTypes.STRING();
		} else {
			dataType = generateFieldsDataType(clz, processedTypes);
		}
		return dataType;
	}

	private static DataType generatePrimitiveDataType(Class<?> clz) {
		DataType dataType;
		if (clz.equals(String.class)) {
			dataType = DataTypes.STRING();
		} else if (clz.equals(short.class) || clz.equals(Short.class)) {
			dataType = DataTypes.SMALLINT();
		} else if (clz.equals(int.class) || clz.equals(Integer.class)) {
			dataType = DataTypes.INT();
		} else if (clz.equals(long.class) || clz.equals(Long.class)) {
			dataType = DataTypes.BIGINT();
		} else if (clz.equals(boolean.class) || clz.equals(Boolean.class)) {
			dataType = DataTypes.BOOLEAN();
		} else if (clz.equals(double.class) || clz.equals(Double.class)) {
			dataType = DataTypes.DOUBLE();
		} else if (clz.equals(byte.class) || clz.equals(Byte.class)) {
			dataType = DataTypes.TINYINT();
		} else if (clz.equals(ByteBuffer.class)) {
			dataType = DataTypes.BYTES();
		} else {
			throw new UnsupportedOperationException("Unsupported class : " + clz);
		}
		return dataType;
	}
}
