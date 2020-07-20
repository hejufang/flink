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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory of deserialization runtime converter.
 */
public class DeserializationRuntimeConverterFactory {

	public static DeserializationRuntimeConverter createRowConverter(Class<?> requestClass, String[] fieldNames) {
		Field[] fields = requestClass.getFields();
		int convertIndex = 0; // The actually convert field index.
		int actualFieldLength = fields.length - 1; // The java class which generate by thrift will add one more field.
		DeserializationRuntimeConverter[] converters = new DeserializationRuntimeConverter[fieldNames.length];
		for (int i = 0; i < actualFieldLength; i++) {
			if (convertIndex == fieldNames.length) {
				break;
			}
			if (fields[i].getName().equals(fieldNames[convertIndex])) {
				Class<?> innerClass = fields[i].getType();
				String[] innerFieldsName = getFieldNamesOfClass(innerClass);
				DeserializationRuntimeConverter converter;
				if (innerClass.isEnum()) {
					converter = createEnumConverter(innerClass);
				} else if (innerClass.equals(List.class) || innerClass.equals(Set.class)) {
					converter = createObjectArrayConverter(fields[i]);
				} else if (innerClass.equals(Map.class)) {
					converter = createMapConverter(fields[i]);
				} else if (isPrimitivePackageClass(innerClass) || innerClass.isPrimitive()){
					converter = createPrimitiveConverter(innerClass);
				} else {
					converter = createRowConverter(innerClass, innerFieldsName);
				}
				converters[convertIndex++] = wrapIntoNullableConverter(converter);
			}
		}
		return (message) -> {
			Object result = requestClass.newInstance();
			int messageIndex = 0;
			for (int i = 0; i < actualFieldLength; i++) {
				if (messageIndex == fieldNames.length) {
					break;
				}
				if (fields[i].getName().equals(fieldNames[messageIndex])) {
					Row innerValue = (Row) message;
					Object innerObject = converters[messageIndex].convert(innerValue.getField(messageIndex));
					messageIndex++;
					fields[i].set(result, innerObject);
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

	private static DeserializationRuntimeConverter createPrimitiveConverter(Class<?> requestClass) {
		if (isPrimitivePackageClass(requestClass) || requestClass.isPrimitive()) {
			return (message) -> message;
		} else {
			throw new UnsupportedOperationException(String.format("Class %s is not primitive or primitive package class",
				requestClass));
		}
	}

	private static DeserializationRuntimeConverter createEnumConverter(Class<?> enumClass) {
		return (message) -> {
			//noinspection JavaReflectionInvocation
			return enumClass.getMethod("valueOf", String.class).invoke(null, message);
		};
	}

	private static DeserializationRuntimeConverter createObjectArrayConverter(Field field) {
		Class<?> outerClass = field.getType();
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> innerClass = (Class<?>) genericType.getActualTypeArguments()[0];
		DeserializationRuntimeConverter elementConverter;
		if (isPrimitivePackageClass(innerClass)) {
			elementConverter = createPrimitiveConverter(innerClass);
		} else {
			String[] innerFieldsName = getFieldNamesOfClass(innerClass);
			elementConverter = createRowConverter(innerClass, innerFieldsName);
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

	private static DeserializationRuntimeConverter createMapConverter(Field field) {
		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
		Class<?> keyClass = (Class<?>) genericType.getActualTypeArguments()[0];
		Class<?> valueClass = (Class<?>) genericType.getActualTypeArguments()[1];
		DeserializationRuntimeConverter keyConverter;
		DeserializationRuntimeConverter valueConverter;
		if (isPrimitivePackageClass(keyClass)) {
			keyConverter = createPrimitiveConverter(keyClass);
		} else {
			String[] innerFieldsName = getFieldNamesOfClass(keyClass);
			keyConverter = createRowConverter(keyClass, innerFieldsName);
		}
		if (isPrimitivePackageClass(valueClass)) {
			valueConverter = createPrimitiveConverter(valueClass);
		} else {
			String[] innerFieldsName = getFieldNamesOfClass(valueClass);
			valueConverter = createRowConverter(valueClass, innerFieldsName);
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

	/**
	 * Get the user-defined class fields name. If requestClass is a java type,
	 * it is unnecessary to know a java type inner field. So we just return
	 * new String[0].
	 * @param requestClass the request class
	 * @return the field names of request class
	 */
	private static String[] getFieldNamesOfClass(Class<?> requestClass) {
		Field[] innerFields = requestClass.getFields();
		String[] innerFieldsName = new String[0];
		if (requestClass.getClassLoader() != null) {
			/* The last field generate by thrift is metaDataMap which contains the thrift fields information.
			 * Thrift version 0.13.0 is suitable for this implementation. Because we use reflect to get fields
			 * information and use reflect to set field. So we doesn't need last field. */
			innerFieldsName = new String[innerFields.length - 1];
			for (int j = 0; j < innerFieldsName.length; j++) {
				innerFieldsName[j] = innerFields[j].getName();
			}
		}
		return innerFieldsName;
	}

	public static boolean isPrimitivePackageClass(Class<?> classType) {
		return classType.equals(Boolean.class)
			|| classType.equals(Byte.class)
			|| classType.equals(Short.class)
			|| classType.equals(Integer.class)
			|| classType.equals(Long.class)
			|| classType.equals(Double.class)
			|| classType.equals(String.class);
	}

}
