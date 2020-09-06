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

import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connectors.rpc.thrift.ThriftUtil.isPrimitivePackageClass;

/**
 * Factory of deserialization runtime converter.
 */
public class DeserializationRuntimeConverterFactory {

	public static DeserializationRuntimeConverter createRowConverter(Class<?> responseClass) {
		Field[] fields = responseClass.getFields();
		int actualFieldLength = fields.length - 1; // The java class which generate by thrift will add one more field.
		DeserializationRuntimeConverter[] converters = new DeserializationRuntimeConverter[actualFieldLength];
		for (int i = 0; i < actualFieldLength; i++) {
			Class<?> innerClass = fields[i].getType();
			DeserializationRuntimeConverter converter;
			if (innerClass.isEnum()) {
				converter = createEnumConverter();
			} else if (innerClass.equals(List.class) || innerClass.equals(Set.class)) {
				converter = createObjectArrayConverter(fields[i]);
			} else if (innerClass.equals(Map.class)) {
				converter = createMapConverter(fields[i]);
			} else if (isPrimitivePackageClass(innerClass) || innerClass.isPrimitive()){
				converter = createPrimitiveConverter(innerClass);
			} else {
				converter = createRowConverter(innerClass);
			}
			converters[i] = wrapIntoNullableConverter(converter);

		}
		return (message) -> {
			Row result = new Row(actualFieldLength);
			for (int i = 0; i < actualFieldLength; i++) {
				Object innerObject = converters[i].convert(message.getClass().getFields()[i].get(message));
				result.setField(i, innerObject);
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
			return (message) -> message;
		} else {
			throw new UnsupportedOperationException(String.format("Class %s is not primitive or primitive package class",
				responseClass));
		}
	}

	private static DeserializationRuntimeConverter createEnumConverter() {
		return Object::toString;
	}

	private static DeserializationRuntimeConverter createObjectArrayConverter(Field field) {
		Class<?> outerClass = field.getType();
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> innerClass = (Class<?>) genericType.getActualTypeArguments()[0];
		DeserializationRuntimeConverter elementConverter;
		if (isPrimitivePackageClass(innerClass)) {
			elementConverter = createPrimitiveConverter(innerClass);
		} else {
			elementConverter = createRowConverter(innerClass);
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

	private static DeserializationRuntimeConverter createMapConverter(Field field) {
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> keyClass = (Class<?>) genericType.getActualTypeArguments()[0];
		Class<?> valueClass = (Class<?>) genericType.getActualTypeArguments()[1];
		DeserializationRuntimeConverter keyConverter;
		DeserializationRuntimeConverter valueConverter;
		if (isPrimitivePackageClass(keyClass)) {
			keyConverter = createPrimitiveConverter(keyClass);
		} else {
			keyConverter = createRowConverter(keyClass);
		}
		if (isPrimitivePackageClass(valueClass)) {
			valueConverter = createPrimitiveConverter(valueClass);
		} else {
			valueConverter = createRowConverter(valueClass);
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
