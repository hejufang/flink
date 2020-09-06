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

package org.apache.flink.connectors.rpc.thrift;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generate Row type information according to thrift struct.
 */
public class ThriftRowTypeInformationUtil {

	/**
	 * Use thrift class information to generate RowTypeInfo to construct DDL.
	 * Breakup request class to generate the first partition of DDL. And then
	 * use response class simple name and a RowTypeInfo to generate the second
	 * partition of DDL.
	 * @param requestClass the RPC request class.
	 * @param responseClass the RPC response class.
	 * @return RowTypeInfo to construct table schema.
	 */
	public static TypeInformation<Row> generateDimensionRowTypeInformation(Class<?> requestClass, Class<?> responseClass) {
		Field[] fields = requestClass.getFields();
		/* The last field generate by thrift is metaDataMap which contains the thrift fields information.
		 * Thrift version 0.13.0 is suitable for this implementation. Because we use reflect to get fields
		 * information. So we doesn't need last field. We construct dimension DDL with break up request class
		 * which number is size-1 and response class at last which use one ROW. So the final number is
		 * (fields.length - 1) + 1 */
		int size = fields.length;
		TypeInformation<?>[] types = new TypeInformation[size];
		String[] rowFieldNames = new String[size];
		for (int i = 0; i < size - 1; i++) {
			Field field = fields[i];
			rowFieldNames[i] = field.getName();
			types[i] = generateFieldTypeInformation(field);
		}
		TypeInformation<Row> responseTypeInfo = generateRowTypeInformation(responseClass);
		types[size - 1] = responseTypeInfo;
		rowFieldNames[size - 1] = responseClass.getSimpleName();
		return new RowTypeInfo(types, rowFieldNames);
	}

	/**
	 * Use thrift class information to generate RowTypeInfo to construct DDL.
	 * We use reflect to get class fields information such as field name and
	 * field type.
	 * @param clz the thrift generated java class
	 * @return RowTypeInfo construct by class corresponding fields.
	 */
	public static TypeInformation<Row> generateRowTypeInformation(Class<?> clz) {
		Field[] fields = clz.getFields();
		/* The last field generate by thrift is metaDataMap which contains the thrift fields information.
		* Thrift version 0.13.0 is suitable for this implementation. Because we use reflect to get fields
		* information and use reflect to set field. So we doesn't need last field. */
		int size = fields.length - 1;
		TypeInformation<?>[] types = new TypeInformation[size];
		String[] rowFieldNames = new String[size];
		for (int i = 0; i < size; i++) {
			Field field = fields[i];
			rowFieldNames[i] = field.getName();
			types[i] = generateFieldTypeInformation(field);
		}
		return new RowTypeInfo(types, rowFieldNames);
	}

	private static TypeInformation<?> generateFieldTypeInformation(Field field) {
		TypeInformation<?> typeInfo;
		Class<?> clz = field.getType();
		if (clz.isPrimitive() || clz.equals(String.class)) {
			typeInfo = generatePrimitiveTypeInformation(clz);
		} else if (clz.equals(List.class) || clz.equals(Set.class)) {
			typeInfo = Types.OBJECT_ARRAY(generateInnerTypeInformation(field, 0));
		} else if (clz.equals(Map.class)) {
			typeInfo = Types.MAP(generateInnerTypeInformation(field, 0),
				generateInnerTypeInformation(field, 1));
		} else if (clz.isEnum()) {
			typeInfo = Types.STRING;
		} else {
			typeInfo = generateRowTypeInformation(clz);
		}
		return typeInfo;
	}

	private static TypeInformation<?> generatePrimitiveTypeInformation(Class<?> clz) {
		TypeInformation<?> typeInfo;
		if (clz.equals(String.class)) {
			typeInfo = Types.STRING;
		} else if (clz.equals(short.class) || clz.equals(Short.class)) {
			typeInfo = Types.SHORT;
		} else if (clz.equals(int.class) || clz.equals(Integer.class)) {
			typeInfo = Types.INT;
		} else if (clz.equals(long.class) || clz.equals(Long.class)) {
			typeInfo = Types.LONG;
		} else if (clz.equals(boolean.class) || clz.equals(Boolean.class)) {
			typeInfo = Types.BOOLEAN;
		} else if (clz.equals(double.class) || clz.equals(Double.class)) {
			typeInfo = Types.DOUBLE;
		} else if (clz.equals(byte.class) || clz.equals(Byte.class)) {
			typeInfo = Types.BYTE;
		} else {
			throw new UnsupportedOperationException("Unsupported class : " + clz);
		}
		return typeInfo;
	}

	private static TypeInformation<?> generateInnerTypeInformation(Field field, int index) {
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> innerClass = (Class<?>) genericType.getActualTypeArguments()[index];
		TypeInformation<?> innerTypeInfo;
		if (ThriftUtil.isPrimitivePackageClass(innerClass)) {
			innerTypeInfo = generatePrimitiveTypeInformation(innerClass);
		} else if (innerClass.equals(List.class) || innerClass.equals(Set.class) || innerClass.equals(Map.class)) {
			throw new FlinkRuntimeException("Nesting of list, set and map is not supported yet, " +
				"you can raise an Oncall for Flink Team @Bytedance if you have this requirement.");
		} else {
			innerTypeInfo = generateRowTypeInformation(innerClass);
		}
		return innerTypeInfo;
	}

}
