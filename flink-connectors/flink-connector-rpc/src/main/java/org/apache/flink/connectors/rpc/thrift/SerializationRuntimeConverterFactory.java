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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connectors.rpc.thrift.ThriftUtil.isPrimitivePackageClass;
import static org.apache.flink.connectors.rpc.util.ObjectUtil.generateSetMethodName;

/**
 * Factory of serialization runtime converter.
 */
public class SerializationRuntimeConverterFactory {
	private static final Logger LOG = LoggerFactory.getLogger(SerializationRuntimeConverterFactory.class);
	public static SerializationRuntimeConverter createRowConverter(Class<?> requestClass, RowType rowType) {
		Field[] fields = requestClass.getFields();
		int len = rowType.getFieldCount();
		List<String> fieldNames = rowType.getFieldNames();
		int convertIndex = 0; // The actually convert field index.
		int actualFieldLength = fields.length - 1; // The java class which generate by thrift will add one more field.
		SerializationRuntimeConverter[] converters = new SerializationRuntimeConverter[len];
		for (int i = 0; i < actualFieldLength; i++) {
			if (convertIndex == len) {
				break;
			}
			if (fields[i].getName().equals(fieldNames.get(convertIndex))) {
				Class<?> innerClass = fields[i].getType();
				SerializationRuntimeConverter converter;
				if (innerClass.isEnum()) {
					converter = createEnumConverter(innerClass);
				} else if (innerClass.equals(List.class) || innerClass.equals(Set.class)) {
					converter = createObjectArrayConverter(fields[i], (ArrayType) rowType.getTypeAt(convertIndex));
				} else if (innerClass.equals(Map.class)) {
					converter = createMapConverter(fields[i], (MapType) rowType.getTypeAt(convertIndex));
				} else if (isPrimitivePackageClass(innerClass) || innerClass.isPrimitive()){
					converter = createPrimitiveConverter(innerClass);
				} else {
					converter = createRowConverter(innerClass, (RowType) rowType.getTypeAt(convertIndex));
				}
				converters[convertIndex++] = wrapIntoNullableConverter(converter);
			}
		}
		Map<String, Method> setFieldMethods =
			Arrays.stream(fields).collect(HashMap::new, (setFieldMethodMap, field) -> {
				try {
					setFieldMethodMap.put(field.getName(),
						requestClass.getMethod(generateSetMethodName(field.getName()), field.getType()));
				} catch (NoSuchMethodException e) {
					throw new RuntimeException(String.format("Cannot find set method for %s", field.getName()), e);
				}
			}, HashMap::putAll);
		return (message) -> {
			Object result = requestClass.newInstance();
			int messageIndex = 0;
			Method setFieldMethod;
			for (int i = 0; i < actualFieldLength; i++) {
				if (messageIndex == len) {
					break;
				}
				if (fields[i].getName().equals(fieldNames.get(messageIndex))) {
					Row innerValue = (Row) message;
					Object innerObject = converters[messageIndex].convert(innerValue.getField(messageIndex));
					messageIndex++;
					setFieldMethod = setFieldMethods.get(fields[i].getName());
					if (setFieldMethod != null) {
						setFieldMethod.invoke(result, innerObject);
					}
				}
			}
			return result;
		};
	}

	private static SerializationRuntimeConverter wrapIntoNullableConverter(SerializationRuntimeConverter converter) {
		return (message) -> {
			if (message == null) {
				return null;
			}
			return converter.convert(message);
		};
	}

	private static SerializationRuntimeConverter createPrimitiveConverter(Class<?> requestClass) {
		if (isPrimitivePackageClass(requestClass) || requestClass.isPrimitive()) {
			if (requestClass.equals(ByteBuffer.class)) {
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
				requestClass));
		}
	}

	private static SerializationRuntimeConverter createEnumConverter(Class<?> enumClass) {
		return (message) -> {
			//noinspection JavaReflectionInvocation
			return enumClass.getMethod("valueOf", String.class).invoke(null, message);
		};
	}

	private static SerializationRuntimeConverter createObjectArrayConverter(Field field, ArrayType arrType) {
		Class<?> outerClass = field.getType();
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> innerClass = (Class<?>) genericType.getActualTypeArguments()[0];
		SerializationRuntimeConverter elementConverter;
		if (isPrimitivePackageClass(innerClass)) {
			elementConverter = createPrimitiveConverter(innerClass);
		} else {
			elementConverter = createRowConverter(innerClass, (RowType) arrType.getElementType());
		}
		if (outerClass.equals(List.class)) {
			return (message) -> {
				Object[] originMessage = ((Object[]) message);
				List<Object> result = new ArrayList<>();
				for (Object o : originMessage) {
					result.add(elementConverter.convert(o));
				}
				return result;
			};
		} else if (outerClass.equals(Set.class)) {
			return (message) -> {
				Object[] originMessage = ((Object[]) message);
				Set<Object> result = new HashSet<>();
				for (Object o : originMessage) {
					result.add(elementConverter.convert(o));
				}
				return result;
			};
		} else {
			throw new RuntimeException(String.format("request class for ARRAY should only be Set/List," +
				" but we got %s here", outerClass.getCanonicalName()));
		}

	}

	private static SerializationRuntimeConverter createMapConverter(Field field, MapType mapType) {
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> keyClass = (Class<?>) genericType.getActualTypeArguments()[0];
		Class<?> valueClass = (Class<?>) genericType.getActualTypeArguments()[1];
		SerializationRuntimeConverter keyConverter;
		SerializationRuntimeConverter valueConverter;
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
