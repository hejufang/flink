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

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory of deserialization runtime converter.
 */
public class DeserializationRuntimeConverterFactory {

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	public static DeserializationRuntimeConverter createConverter(
			LogicalType type,
			Descriptors.GenericDescriptor genericDescriptor) {

		Preconditions.checkNotNull(type, "type cannot be null!");
		switch (type.getTypeRoot()) {
			case VARCHAR:
				return (message) -> StringData.fromString(message.toString());
			case BIGINT:
			case BOOLEAN:
			case INTEGER:
			case DOUBLE:
			case FLOAT:
				return (message) -> message;
			case VARBINARY:
				return (message) -> ((ByteString) message).toByteArray();
			case ARRAY:
				return createArrayConverter((ArrayType) type, (Descriptors.FieldDescriptor) genericDescriptor);
			case MAP:
				return createMapConverter((MapType) type, (Descriptors.FieldDescriptor) genericDescriptor);
			case ROW:
				return createRowConverter((RowType) type, (Descriptors.Descriptor) genericDescriptor);
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private static DeserializationRuntimeConverter createRowConverter(
			RowType rowType,
			Descriptors.Descriptor descriptor) {

		List<Descriptors.FieldDescriptor> fieldDescriptors = descriptor.getFields();
		AtomicInteger index = new AtomicInteger();
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map((logicType) -> DeserializationRuntimeConverterFactory.createConverter(
				logicType, fieldDescriptors.get(index.getAndIncrement())))
			.toArray(DeserializationRuntimeConverter[]::new);
		return (message) -> {
			DynamicMessage dynamicMessage = (DynamicMessage) message;
			int arity = fieldConverters.length;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < fieldDescriptors.size(); i++) {
				// We have to use fieldDescriptor here, so that DeserializationRuntimeConverter cannot be Serializable.
				Descriptors.FieldDescriptor fieldDescriptor = fieldDescriptors.get(i);
				Object convertField = fieldConverters[i].convert(dynamicMessage.getField(fieldDescriptor));
				row.setField(i, convertField);
			}
			return row;
		};
	}

	private static DeserializationRuntimeConverter createArrayConverter(
			ArrayType type,
			Descriptors.FieldDescriptor fieldDescriptor) {

		LogicalType elementType = type.getElementType();
		Descriptors.Descriptor elementDescriptor = fieldDescriptor.getMessageType();

		DeserializationRuntimeConverter elementConverter = createConverter(elementType, elementDescriptor);
		return (message) -> {
			List<?> messageList = (List<?>) message;
			Object[] fieldValues = new Object[messageList.size()];
			for (int i = 0; i < messageList.size(); i++) {
				fieldValues[i] = elementConverter.convert(messageList.get(i));
			}
			return new GenericArrayData(fieldValues);
		};
	}

	private static DeserializationRuntimeConverter createMapConverter(
			MapType type,
			Descriptors.FieldDescriptor fieldDescriptor) {

		Descriptors.Descriptor descriptor = fieldDescriptor.getMessageType();
		DeserializationRuntimeConverter keyConverter = createConverter(type.getKeyType(),
			descriptor.getFields().get(0));
		DeserializationRuntimeConverter valueConverter = createConverter(type.getValueType(),
			descriptor.getFields().get(1));

		return (message) -> {
			Map<Object, Object> map = new HashMap<>();
			//noinspection unchecked
			for (DynamicMessage mapMessage : (List<DynamicMessage>) message) {
				Object k = null, v = null;
				for (Map.Entry<Descriptors.FieldDescriptor, Object> singleMessage : mapMessage.getAllFields().entrySet()) {
					if (singleMessage.getKey().getJsonName().equals(PbConstant.KEY)) {
						k = keyConverter.convert(singleMessage.getValue());
					} else if (singleMessage.getKey().getJsonName().equals(PbConstant.VALUE)) {
						v = valueConverter.convert(singleMessage.getValue());
					}
				}
				map.put(k, v);
			}
			return new GenericMapData(map);
		};
	}

	/**
	 * The converter to convert message according field descriptor.
	 */
	@FunctionalInterface
	public interface DeserializationRuntimeConverter {
		/**
		 * Convert pb message to data in inner type.
		 * @param message origin message.
		 * @return Converted message.
		 */
		Object convert(Object message);
	}
}
