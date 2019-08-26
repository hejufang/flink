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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Factory of deserialization runtime converter.
 */
public class DeserializationRuntimeConverterFactory {
	public static DeserializationRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
		DeserializationRuntimeConverter baseConverter = createConverterForSimpleType(typeInfo)
			.orElseGet(() -> createContainerConverter(typeInfo).get());
		return baseConverter;
	}

	private static Optional<DeserializationRuntimeConverter> createConverterForSimpleType(TypeInformation<?> simpleTypeInfo) {
		if (simpleTypeInfo == Types.VOID) {
			return Optional.of((message, fieldDescriptors) -> null);
		} else if (simpleTypeInfo == Types.BOOLEAN) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.STRING) {
			return Optional.of((message, fieldDescriptors) -> message.toString());
		} else if (simpleTypeInfo == Types.INT) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.LONG) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.DOUBLE) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.FLOAT) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.SHORT) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.BYTE) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.BIG_DEC) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.BIG_INT) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.SQL_TIMESTAMP) {
			return Optional.of(createTimestampConverter());
		} else {
			return Optional.empty();
		}
	}

	private static DeserializationRuntimeConverter createTimestampConverter() {
		return (message, fieldDescriptors) -> {
			Descriptors.FieldDescriptor fieldDescriptor = fieldDescriptors.get(0);
			if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.LONG) {
				return new Timestamp((long) message);
			} else if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.INT) {
				return new Timestamp(((int) message) * 1000L);
			}
			return new Timestamp(System.currentTimeMillis());
		};
	}

	private static Optional<DeserializationRuntimeConverter> createContainerConverter(TypeInformation<?> typeInfo) {
		if (typeInfo instanceof RowTypeInfo) {
			return Optional.of(createRowConverter((RowTypeInfo) typeInfo));
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((ObjectArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (typeInfo instanceof BasicArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((BasicArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (isPrimitiveByteArray(typeInfo)) {
			return Optional.of(createByteArrayConverter());
		} else if (typeInfo instanceof MapTypeInfo) {
			MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
			return Optional.of(createMapConverter(mapTypeInfo.getKeyTypeInfo(), mapTypeInfo.getValueTypeInfo()));
		} else {
			return Optional.empty();
		}
	}

	private static DeserializationRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
		List<DeserializationRuntimeConverter> fieldConverters = Arrays.stream(typeInfo.getFieldTypes())
			.map(DeserializationRuntimeConverterFactory::createConverter)
			.collect(Collectors.toList());

		return assembleRowConverter(fieldConverters);
	}

	private static DeserializationRuntimeConverter assembleRowConverter(
		List<DeserializationRuntimeConverter> fieldConverters) {
		return (message, fieldDescriptors) -> {
			DynamicMessage dynamicMessage = (DynamicMessage) message;
			int arity = fieldConverters.size();
			Row row = new Row(arity);
			for (int i = 0; i < arity; i++) {
				Descriptors.FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);
				Object convertField = fieldConverters.get(i).convert(
					dynamicMessage.getField(fieldDescriptor),
					fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE ? fieldDescriptor.getMessageType().getFields() : Arrays.asList(fieldDescriptor));
				row.setField(i, convertField);
			}

			return row;
		};
	}

	private static DeserializationRuntimeConverter createObjectArrayConverter(TypeInformation elementTypeInfo) {
		DeserializationRuntimeConverter elementConverter = createConverter(elementTypeInfo);
		return assembleArrayConverter(elementConverter);
	}

	private static DeserializationRuntimeConverter assembleArrayConverter(DeserializationRuntimeConverter elementConverter) {
		return (message, fieldDescriptors) -> {
			List<?> messageList = (List<?>) message;
			List<Object> fieldValues = new ArrayList<>();
			for (Object single : messageList) {
				fieldValues.add(elementConverter.convert(single, fieldDescriptors));
			}
			return fieldValues.toArray();
		};
	}

	private static DeserializationRuntimeConverter createMapConverter(TypeInformation keyTypeInfo, TypeInformation valueTypeInfo) {
		DeserializationRuntimeConverter keyConverter = createConverter(keyTypeInfo);
		DeserializationRuntimeConverter valueConverter = createConverter(valueTypeInfo);

		return assembleMapConverter(keyConverter, valueConverter);
	}

	private static DeserializationRuntimeConverter assembleMapConverter(
		DeserializationRuntimeConverter keyConverter,
		DeserializationRuntimeConverter valueConverter) {
		return (message, fieldDescriptors) -> {
			Map<Object, Object> map = new HashMap<>();
			for (DynamicMessage mapMessage : (List<DynamicMessage>) message) {
				Object k = null, v = null;
				for (Map.Entry<Descriptors.FieldDescriptor, Object> singleMessage : mapMessage.getAllFields().entrySet()) {
					if (singleMessage.getKey().getJsonName().equals(PbConstant.KEY)) {
						k = keyConverter.convert(singleMessage.getValue(), Arrays.asList(singleMessage.getKey()));
					} else if (singleMessage.getKey().getJsonName().equals(PbConstant.VALUE)) {
						v = valueConverter.convert(singleMessage.getValue(), Arrays.asList(singleMessage.getKey()));
					}
				}
				map.put(k, v);
			}
			return map;
		};
	}

	private static boolean isPrimitiveByteArray(TypeInformation<?> typeInfo) {
		return typeInfo.equals(Types.PRIMITIVE_ARRAY(Types.BYTE));
	}

	private static DeserializationRuntimeConverter createByteArrayConverter() {
		return (message, fieldDescriptors) -> ((ByteString) message).toByteArray();
	}
}
