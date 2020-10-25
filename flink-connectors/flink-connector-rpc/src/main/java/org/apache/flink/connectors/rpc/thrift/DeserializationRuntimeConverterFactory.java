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

package org.apache.flink.connectors.rpc.thrift;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connectors.rpc.thrift.ThriftUtil.isPrimitivePackageClass;

/**
 * Factory of deserialization runtime converter.
 */
public class DeserializationRuntimeConverterFactory {

	public static DeserializationRuntimeConverter createRowConverter(Class<?> responseClass, RowType rowType) {
		Field[] fields = responseClass.getFields();
		int len = rowType.getFieldCount();
		List<String> fieldNames = rowType.getFieldNames();
		int convertIndex = 0; // The actually convert field index.
		int actualFieldLength = fields.length - 1; // The java class which generate by thrift will add one more field.
		DeserializationRuntimeConverter[] converters = new DeserializationRuntimeConverter[len];
		for (int i = 0; i < actualFieldLength; i++) {
			if (convertIndex == len) {
				break;
			}
			if (fields[i].getName().equals(fieldNames.get(convertIndex))) {
				Class<?> innerClass = fields[i].getType();
				DeserializationRuntimeConverter converter;
				if (innerClass.isEnum()) {
					converter = createEnumConverter();
				} else if (innerClass.equals(List.class) || innerClass.equals(Set.class)) {
					converter = createObjectArrayConverter(fields[i], (ArrayType) rowType.getTypeAt(convertIndex));
				} else if (innerClass.equals(Map.class)) {
					converter = createMapConverter(fields[i], (MapType) rowType.getTypeAt(convertIndex));
				} else if (isPrimitivePackageClass(innerClass) || innerClass.isPrimitive()) {
					converter = createPrimitiveConverter(innerClass);
				} else {
					converter = createRowConverter(innerClass, (RowType) rowType.getTypeAt(convertIndex));
				}
				converters[convertIndex++] = wrapIntoNullableConverter(converter);
			}
		}
		return (message) -> {
			Row result = new Row(len);
			int messageIndex = 0;
			for (int i = 0; i < actualFieldLength; i++) {
				if (messageIndex == len) {
					break;
				}
				if (fields[i].getName().equals(fieldNames.get(messageIndex))) {
					Object innerObject = converters[messageIndex].convert(fields[i].get(message));
					result.setField(messageIndex, innerObject);
					messageIndex++;
				}
			}
			return result;
		};
	}

	private static DeserializationRuntimeConverter wrapIntoNullableConverter(DeserializationRuntimeConverter converter) {
		return (message) -> {
			if (message == null) {
				return null;
			}
			return converter.convert(message);
		};
	}

	private static DeserializationRuntimeConverter createPrimitiveConverter(Class<?> responseClass) {
		if (isPrimitivePackageClass(responseClass) || responseClass.isPrimitive()) {
			if (responseClass.equals(ByteBuffer.class)) {
				return (message) -> {
					byte[] arr = new byte[((ByteBuffer) message).remaining()];
					((ByteBuffer) message).get(arr);
					return arr;
				};
			} else {
				return (message) -> message;
			}
		} else {
			throw new UnsupportedOperationException(String.format("Class %s is not primitive or primitive package class",
				responseClass));
		}
	}

	private static DeserializationRuntimeConverter createEnumConverter() {
		return Object::toString;
	}

	private static DeserializationRuntimeConverter createObjectArrayConverter(Field field, ArrayType arrType) {
		Class<?> outerClass = field.getType();
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> innerClass = (Class<?>) genericType.getActualTypeArguments()[0];
		DeserializationRuntimeConverter elementConverter;
		if (isPrimitivePackageClass(innerClass)) {
			elementConverter = createPrimitiveConverter(innerClass);
		} else {
			elementConverter = createRowConverter(innerClass, (RowType) arrType.getElementType());
		}
		if (outerClass.equals(List.class)) {
			return (message) -> {
				List<Object> originMessage = (List<Object>) message;
				Object[] result = new Object[originMessage.size()];
				for (int i = 0; i < originMessage.size(); i++) {
					result[i] = elementConverter.convert(originMessage.get(i));
				}
				return result;
			};
		} else if (outerClass.equals(Set.class)) {
			return (message) -> {
				Object[] originMessage = ((Set<Object>) message).toArray();
				Object[] result = new Object[originMessage.length];
				for (int i = 0; i < originMessage.length; i++) {
					result[i] = elementConverter.convert(originMessage[i]);
				}
				return result;
			};
		} else {
			throw new RuntimeException(String.format("request class for ARRAY should only be Set/List," +
				" but we got %s here", outerClass.getCanonicalName()));
		}

	}

	private static DeserializationRuntimeConverter createMapConverter(Field field, MapType mapType) {
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> keyClass = (Class<?>) genericType.getActualTypeArguments()[0];
		Class<?> valueClass = (Class<?>) genericType.getActualTypeArguments()[1];
		DeserializationRuntimeConverter keyConverter;
		DeserializationRuntimeConverter valueConverter;
		if (isPrimitivePackageClass(keyClass)) {
			keyConverter = createPrimitiveConverter(keyClass);
		} else {
			keyConverter = createRowConverter(keyClass, (RowType) mapType.getKeyType());
		}
		if (isPrimitivePackageClass(valueClass)) {
			valueConverter = createPrimitiveConverter(valueClass);
		} else {
			valueConverter = createRowConverter(valueClass, (RowType) mapType.getValueType());
		}

		return (message) -> {
			HashMap<Object, Object> result = new HashMap<>();
			for (Map.Entry<?, ?> entry : ((Map<?, ?>) message).entrySet()) {
				Object mapKey = entry.getKey();
				Object mapValue = entry.getValue();
				result.put(keyConverter.convert(mapKey),
					valueConverter.convert(mapValue));
			}
			return result;
		};
	}

}
