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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.IdentityConverter;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Converter for {@link ArrayData} of {@link List} external type.
 */
public class ArrayArrayConverter<E> implements DataStructureConverter<ArrayData, List<E>> {
	private static final long serialVersionUID = 1L;

	private final int elementSize;

	private final BinaryArrayWriter.NullSetter  writerNullSetter;

	private final BinaryWriter.ValueSetter writerValueSetter;

	private final GenericToJavaArrayConverter<E> genericToJavaArrayConverter;

	private transient BinaryArrayData reuseArray;

	private transient BinaryArrayWriter reuseWriter;

	final boolean hasInternalElements;

	final ArrayData.ElementGetter elementGetter;

	final DataStructureConverter<Object, E> elementConverter;

	private ArrayArrayConverter(
			int elementSize,
			BinaryArrayWriter.NullSetter writerNullSetter,
			BinaryWriter.ValueSetter writerValueSetter,
			GenericToJavaArrayConverter<E> genericToJavaArrayConverter,
			ArrayData.ElementGetter elementGetter,
			DataStructureConverter<Object, E> elementConverter) {
		this.elementSize = elementSize;
		this.writerNullSetter = writerNullSetter;
		this.writerValueSetter = writerValueSetter;
		this.genericToJavaArrayConverter = genericToJavaArrayConverter;
		this.hasInternalElements = elementConverter instanceof IdentityConverter;
		this.elementGetter = elementGetter;
		this.elementConverter = elementConverter;
	}

	@Override
	public void open(ClassLoader classLoader) {
		reuseArray = new BinaryArrayData();
		reuseWriter = new BinaryArrayWriter(reuseArray, 0, elementSize);
		elementConverter.open(classLoader);
	}

	@Override
	public ArrayData toInternal(List<E> external) {
		return hasInternalElements ? new GenericArrayData(external.toArray()) : toBinaryArrayData(external);
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<E> toExternal(ArrayData internal) {
		if (hasInternalElements && internal instanceof GenericArrayData) {
			final GenericArrayData genericArray = (GenericArrayData) internal;
			if (genericArray.isPrimitiveArray()) {
				return genericToJavaArrayConverter.convert((GenericArrayData) internal);
			}
			return (List<E>) Arrays.asList(genericArray.toObjectArray());
		}
		return toJavaList(internal);
	}

	private ArrayData toBinaryArrayData(List<E> external) {
		final int length = external.size();
		allocateWriter(length);
		for (int pos = 0; pos < length; pos++) {
			writeElement(pos, external.get(pos));
		}
		return completeWriter().copy();
	}

	private List<E> toJavaList(ArrayData internal) {
		final int size = internal.size();
		final List<E> values = new ArrayList<>();
		for (int pos = 0; pos < size; pos++) {
			final Object value = elementGetter.getElementOrNull(internal, pos);
			values.add(elementConverter.toExternalOrNull(value));
		}
		return values;
	}

	interface GenericToJavaArrayConverter<E> extends Serializable {
		List<E> convert(GenericArrayData internal);
	}

	// --------------------------------------------------------------------------------------------
	// Shared code
	// --------------------------------------------------------------------------------------------

	void allocateWriter(int length) {
		if (reuseWriter.getNumElements() != length) {
			reuseWriter = new BinaryArrayWriter(reuseArray, length, elementSize);
		} else {
			reuseWriter.reset();
		}
	}

	void writeElement(int pos, E element) {
		if (element == null) {
			writerNullSetter.setNull(reuseWriter, pos);
		} else {
			writerValueSetter.setValue(reuseWriter, pos, elementConverter.toInternalOrNull(element));
		}
	}

	BinaryArrayData completeWriter() {
		reuseWriter.complete();
		return reuseArray;
	}

	// --------------------------------------------------------------------------------------------
	// Factory method
	// --------------------------------------------------------------------------------------------
	public static ArrayArrayConverter<?> create(Type elementType, DataType dataType) {
		DataType element = dataType.getChildren().get(0);
		return createForElement(elementType, element);
	}
//	public static ArrayArrayConverter<?> create(Field field, DataType dataType) {
//		DataType element = dataType.getChildren().get(0);
//		if (field == null) {
//			if (isPrimitivePackageClass(getClassForDataType(element))) {
//				return createForElement(null, element);
//			}
//			throw new IllegalStateException("Nested list cannot contains non-primitive element.");
//		}
//		ParameterizedType genericType = (ParameterizedType) field.getGenericType();
//		Class<?> innerClass = (Class<?>) genericType.getActualTypeArguments()[0];
//		return createForElement(innerClass, element);
//	}

	@SuppressWarnings("unchecked")
	public static <E> ArrayArrayConverter<E> createForElement(
			Type elementType,
			DataType elementDataType) {
		final LogicalType elementLogicalType = elementDataType.getLogicalType();
		return new ArrayArrayConverter<>(
			BinaryArrayData.calculateFixLengthPartSize(elementLogicalType),
			BinaryArrayWriter.createNullSetter(elementLogicalType),
			BinaryWriter.createValueSetter(elementLogicalType),
			createGenericToJavaArrayConverter(elementLogicalType),
			ArrayData.createElementGetter(elementLogicalType),
			(DataStructureConverter<Object, E>) JavaBeanConverters.getConverter(elementType, elementDataType)
		);
	}

	@SuppressWarnings("unchecked")
	private static <E> GenericToJavaArrayConverter<E> createGenericToJavaArrayConverter(LogicalType elementType) {
		switch (elementType.getTypeRoot()) {
			case BOOLEAN:
				return internal -> (List<E>) Arrays.asList(ArrayUtils.toObject(internal.toBooleanArray()));
			case TINYINT:
				return internal -> (List<E>) Arrays.asList(ArrayUtils.toObject(internal.toByteArray()));
			case SMALLINT:
				return internal -> (List<E>) Arrays.asList(ArrayUtils.toObject(internal.toShortArray()));
			case INTEGER:
				return internal -> (List<E>) Arrays.asList(ArrayUtils.toObject(internal.toIntArray()));
			case BIGINT:
				return internal -> (List<E>) Arrays.asList(ArrayUtils.toObject(internal.toLongArray()));
			case FLOAT:
				return internal -> (List<E>) Arrays.asList(ArrayUtils.toObject(internal.toFloatArray()));
			case DOUBLE:
				return internal -> (List<E>) Arrays.asList(ArrayUtils.toObject(internal.toDoubleArray()));
			default:
				return internal -> {
					throw new IllegalStateException(String.format("Unsupported element type: %s", elementType));
				};
		}
	}
}
