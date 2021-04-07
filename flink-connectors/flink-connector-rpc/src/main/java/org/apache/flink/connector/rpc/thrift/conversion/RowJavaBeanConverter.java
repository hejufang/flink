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

package org.apache.flink.connector.rpc.thrift.conversion;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.rpc.util.ObjectUtil.generateSetMethodName;

/**
 * Converter for converting between {@link RowData} and RPC request/response object.
 */
public class RowJavaBeanConverter implements DataStructureConverter<RowData, Object> {
	private final DataStructureConverter<Object, Object>[] fieldConverters;
	private final RowData.FieldGetter[] fieldGetters;
	private final Class<?> objectClass;
	private final Field[] fields;
	private final List<String> fieldNames;
	private final Map<String, Method> setFieldMethods;
	// num of object fields
	private final int actualFieldLength;
	// num of rowData fields.
	private final int length;
	private RowJavaBeanConverter(
			DataStructureConverter<Object, Object>[] fieldConverters,
			RowData.FieldGetter[] fieldGetters,
			Class<?> objectClass,
			int actualFieldLength,
			int length,
			Field[] fields,
			List<String> fieldNames,
			Map<String, Method> setFieldMethods
			) {
		this.fieldConverters = fieldConverters;
		this.fieldGetters = fieldGetters;
		this.objectClass = objectClass;
		this.actualFieldLength = actualFieldLength;
		this.length = length;
		this.fields = fields;
		this.fieldNames = fieldNames;
		this.setFieldMethods = setFieldMethods;
	}

	@SuppressWarnings({"unchecked"})
	public static RowJavaBeanConverter create(
			Class<?> objectClass,
			DataType dataType) {
		Field[] fields = objectClass.getFields();
		// The java class which generate by thrift will add one more field.
		int actualFieldLength = fields.length - 1;
		final List<DataType> fieldTypes = dataType.getChildren();
		RowType rowType = (RowType) dataType.getLogicalType();
		List<String> fieldNames = rowType.getFieldNames();
		int len = fieldNames.size();
		int convertIndex = 0;
		Map<String, Method> setFieldMethods = new HashMap<>();
		final List<DataStructureConverter<Object, Object>> fieldConverters = new ArrayList<>(len);
		final List<RowData.FieldGetter> fieldGetters = new ArrayList<>(len);
		try {
			for (int i = 0; i < actualFieldLength; i++) {
				if (convertIndex == len) {
					break;
				}
				if (fields[i].getName().equals(fieldNames.get(convertIndex))) {
					Class<?> innerClass = fields[i].getType();
					setFieldMethods.put(fieldNames.get(convertIndex),
						objectClass.getMethod(generateSetMethodName(fieldNames.get(convertIndex)), innerClass));
					fieldConverters.add((DataStructureConverter<Object, Object>)
						JavaBeanConverters.getConverter(fields[i].getGenericType(), fieldTypes.get(convertIndex)));
					fieldGetters.add(RowData.createFieldGetter(
						fieldTypes.get(convertIndex).getLogicalType(), convertIndex));
					convertIndex++;
				}
			}
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(String.format("Cannot find set method for %s", fieldNames.get(convertIndex)), e);
		}
		return new RowJavaBeanConverter(
			fieldConverters.toArray(new DataStructureConverter[]{}),
			fieldGetters.toArray(new RowData.FieldGetter[]{}),
			objectClass,
			actualFieldLength,
			len,
			fields,
			fieldNames,
			setFieldMethods);
	}

	public void open(ClassLoader classLoader) {
		for (DataStructureConverter<Object, Object> fieldConverter : fieldConverters) {
			fieldConverter.open(classLoader);
		}
	}

	@Override
	public RowData toInternal(Object external) {
		final int length = fieldConverters.length;
		GenericRowData res = new GenericRowData(length);
		int messageIndex = 0;
		try {
			for (int i = 0; i < actualFieldLength; i++) {
				if (messageIndex == length) {
					break;
				}
				if (fields[i].getName().equals(fieldNames.get(messageIndex))) {
					Object innerObject = fieldConverters[messageIndex].toInternalOrNull(fields[i].get(external));
					res.setField(messageIndex, innerObject);
					messageIndex++;
				}
			}
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Not expected exception, please contact Flink team", e);
		}
		return res;
	}

	public Object toExternal(RowData internal) {
		try {
			Object result = objectClass.newInstance();
			int messageIndex = 0;
			Method setFieldMethod;
			for (int i = 0; i < actualFieldLength; i++) {
				if (messageIndex == length) {
					break;
				}
				if (fields[i].getName().equals(fieldNames.get(messageIndex))) {
					Object innerObject = fieldConverters[messageIndex].toExternalOrNull(fieldGetters[messageIndex].getFieldOrNull(internal));
					messageIndex++;
					setFieldMethod = setFieldMethods.get(fields[i].getName());
					if (setFieldMethod != null) {
						setFieldMethod.invoke(result, innerObject);
					}
				}
			}
			return result;
		} catch (Exception e) {
			throw new RuntimeException("Not expected exception, please contact Flink team", e);
		}
	}
}
