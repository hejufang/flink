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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MapEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory of serialization runtime converter.
 */
public class SerializationRuntimeConverterFactory {

	/**
	 * Creates a runtime converter which is null safe.
	 */
	public static SerializationRuntimeConverter createConverter(
			LogicalType type,
			Descriptors.GenericDescriptor genericDescriptor) {

		SerializationRuntimeConverter notNullConverter = createNotNullConverter(type, genericDescriptor);
		return wrapIntoNullableConverter(notNullConverter);
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private static SerializationRuntimeConverter createNotNullConverter(
			LogicalType type,
			Descriptors.GenericDescriptor genericDescriptor) {

		Preconditions.checkNotNull(type, "type cannot be null!");
		switch (type.getTypeRoot()) {
			case VARCHAR:
				return createEnumOrStringConverter((Descriptors.FieldDescriptor) genericDescriptor);
			case BIGINT:
			case BOOLEAN:
			case INTEGER:
			case DOUBLE:
			case FLOAT:
			case VARBINARY:
				return (value) -> value;
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

	private static SerializationRuntimeConverter createEnumOrStringConverter(
			Descriptors.FieldDescriptor fieldDescriptor) {
		Descriptors.FieldDescriptor.JavaType javaType = fieldDescriptor.getJavaType();
		if (javaType.equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {

			Descriptors.EnumDescriptor enumDescriptor = fieldDescriptor.getEnumType();

			return (value) -> {
				Descriptors.EnumValueDescriptor enumValue = enumDescriptor.findValueByName(value.toString());

				if (enumValue == null) {
					throw new FlinkRuntimeException(String.format("Cannot find enum value '%s' in '%s'.",
						value, enumDescriptor.getFullName()));
				}
				return enumValue;
			};
		} else {
			return Object::toString;
		}
	}

	private static SerializationRuntimeConverter createRowConverter(
			RowType rowType,
			Descriptors.Descriptor descriptor) {

		List<Descriptors.FieldDescriptor> fieldDescriptors = descriptor.getFields();
		AtomicInteger converterIndex = new AtomicInteger();
		final SerializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map((logicType) -> SerializationRuntimeConverterFactory.createConverter(
				logicType, fieldDescriptors.get(converterIndex.getAndIncrement())))
			.toArray(SerializationRuntimeConverter[]::new);

		AtomicInteger getterIndex = new AtomicInteger();
		final RowData.FieldGetter[] fieldGetters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(fieldType -> RowData.createFieldGetter(fieldType, getterIndex.getAndIncrement()))
			.toArray(RowData.FieldGetter[]::new);

		return (value) -> {
			RowData row = (RowData) value;
			final DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);
			for (int i = 0; i < row.getArity(); i++) {
				Object field = fieldGetters[i].getFieldOrNull(row);
				if (field != null) {
					// We have to use fieldDescriptor here, so that SerializationRuntimeConverter cannot be Serializable.
					dynamicMessageBuilder.setField(fieldDescriptors.get(i), fieldConverters[i].convert(field));
				}
			}
			return dynamicMessageBuilder.build();
		};
	}

	private static SerializationRuntimeConverter createArrayConverter(
			ArrayType arrayType,
			Descriptors.FieldDescriptor fieldDescriptor) {

		final Descriptors.Descriptor elementDescriptor = fieldDescriptor.getMessageType();
		LogicalType elementType = arrayType.getElementType();
		ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
		SerializationRuntimeConverter elementConverter = createConverter(elementType, elementDescriptor);
		return (value) -> {
			ArrayData arrayData = (ArrayData) value;
			List<Object> fieldValues = new ArrayList<>();
			for (int i = 0; i < arrayData.size(); i++) {
				fieldValues.add(elementConverter.convert(elementGetter.getElementOrNull(arrayData, i)));
			}
			return fieldValues;
		};
	}

	private static SerializationRuntimeConverter createMapConverter(
			MapType mapType,
			Descriptors.FieldDescriptor fieldDescriptor) {

		final Descriptors.Descriptor messageType = fieldDescriptor.getMessageType();
		Descriptors.FieldDescriptor keyFieldDescriptor = messageType.getFields().get(0);
		Descriptors.FieldDescriptor valueFieldDescriptor = messageType.getFields().get(1);
		SerializationRuntimeConverter keyConverter =
			createConverter(mapType.getKeyType(), keyFieldDescriptor);
		SerializationRuntimeConverter valueConverter =
			createConverter(mapType.getValueType(), valueFieldDescriptor);

		ArrayData.ElementGetter keyElementGetter = ArrayData.createElementGetter(mapType.getKeyType());
		ArrayData.ElementGetter valueElementGetter = ArrayData.createElementGetter(mapType.getValueType());

		return (value) -> {
			MapData genericMapData = (MapData) value;
			List<MapEntry<?, ?>> list = new ArrayList<>();

			ArrayData keyArray = genericMapData.keyArray();
			ArrayData valueArray = genericMapData.valueArray();

			int mapSize = genericMapData.size();

			for (int i = 0; i < mapSize; i++) {
				list.add(MapEntry.newDefaultInstance(
					messageType,
					keyFieldDescriptor.getLiteType(),
					keyConverter.convert(keyElementGetter.getElementOrNull(keyArray, i)),
					valueFieldDescriptor.getLiteType(),
					valueConverter.convert(valueElementGetter.getElementOrNull(valueArray, i))));
			}
			return list;
		};
	}

	private static SerializationRuntimeConverter wrapIntoNullableConverter(SerializationRuntimeConverter converter) {
		return (value) -> {
			if (value == null) {
				return null;
			}
			return converter.convert(value);
		};
	}

	/**
	 * The converter to convert value to message.
	 */
	@FunctionalInterface
	public interface SerializationRuntimeConverter {
		/**
		 * Convert value to pb message.
		 * @param value origin value.
		 * @return Converted message.
		 */
		Object convert(Object value);
	}
}
