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

package org.apache.flink.formats.pb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.descriptors.PbValidator;
import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from PB to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a PB object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@PublicEvolving
public class PbRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = -4040917522067315718L;

	/** Type information describing the result type. */
	private final RowTypeInfo typeInfo;

	private final String pbDescriptorClass;

	private final DeserializationRuntimeConverter runtimeConverter;

	private Descriptors.Descriptor pbDescriptor = null;

	private PbRowDeserializationSchema(TypeInformation<Row> typeInfo, String pbDescriptorClass) {
		checkNotNull(typeInfo, "Type information");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.pbDescriptorClass = pbDescriptorClass;
		this.runtimeConverter = createConverter(this.typeInfo);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		// lazy initial pbDescriptor
		if (pbDescriptor == null) {
			pbDescriptor = PbValidator.validateAndReturnDescriptor(pbDescriptorClass);
		}
		try {
			DynamicMessage dynamicMessage = DynamicMessage.parseFrom(pbDescriptor, message);
			return (Row) runtimeConverter.convert(dynamicMessage, pbDescriptor.getFields());
		} catch (Throwable t) {
			throw new IOException("Failed to deserialize PB object.", t);
		}
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final PbRowDeserializationSchema that = (PbRowDeserializationSchema) o;
		return Objects.equals(typeInfo, this.typeInfo) &&
			Objects.equals(pbDescriptor, that.pbDescriptor);
	}

	@Override
	public int hashCode() {
		return Objects.hash(typeInfo, pbDescriptor);
	}

	/**
	 * Builder for {@link PbRowDeserializationSchema}.
	 */
	public static class Builder {
		private TypeInformation<Row> typeInfo;
		private String pbDescriptorClass;

		public static Builder newBuilder() {
			return new Builder();
		}

		public Builder setTypeInfo(TypeInformation<Row> typeInfo) {
			this.typeInfo = typeInfo;
			return this;
		}

		public Builder setPbDescriptorClass(String pbDescriptorClass) {
			this.pbDescriptorClass = pbDescriptorClass;
			return this;
		}

		public PbRowDeserializationSchema build() {
			return new PbRowDeserializationSchema(this.typeInfo, this.pbDescriptorClass);
		}
	}

	/*
		Runtime converter
	 */

	/**
	 * Runtime converter that maps between Pb nodes and Java objects.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(Object message, List<Descriptors.FieldDescriptor> fieldDescriptors);
	}

	private DeserializationRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
		DeserializationRuntimeConverter baseConverter = createConverterForSimpleType(typeInfo)
			.orElseGet(() -> createContainerConverter(typeInfo).get());
		return baseConverter;
	}

	private Optional<DeserializationRuntimeConverter> createConverterForSimpleType(TypeInformation<?> simpleTypeInfo) {
		if (simpleTypeInfo == Types.VOID) {
			return Optional.of((message, fieldDescriptors) -> null);
		} else if (simpleTypeInfo == Types.BOOLEAN) {
			return Optional.of((message, fieldDescriptors) -> message);
		} else if (simpleTypeInfo == Types.STRING) {
			return Optional.of((message, fieldDescriptors) -> message);
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

	private DeserializationRuntimeConverter createTimestampConverter() {
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

	private Optional<DeserializationRuntimeConverter> createContainerConverter(TypeInformation<?> typeInfo) {
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

	private DeserializationRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
		List<DeserializationRuntimeConverter> fieldConverters = Arrays.stream(typeInfo.getFieldTypes())
			.map(this::createConverter)
			.collect(Collectors.toList());

		return assembleRowConverter(fieldConverters);
	}

	private DeserializationRuntimeConverter assembleRowConverter(
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

	private DeserializationRuntimeConverter createObjectArrayConverter(TypeInformation elementTypeInfo) {
		DeserializationRuntimeConverter elementConverter = createConverter(elementTypeInfo);
		return assembleArrayConverter(elementConverter);
	}

	private DeserializationRuntimeConverter assembleArrayConverter(DeserializationRuntimeConverter elementConverter) {
		return (message, fieldDescriptors) -> {
			List<?> messageList = (List<?>) message;
			List<Object> fieldValues = new ArrayList<>();
			for (int i = 0; i < messageList.size(); i++) {
				fieldValues.add(elementConverter.convert(messageList.get(i), fieldDescriptors));
			}
			return fieldValues.toArray();
		};
	}

	private DeserializationRuntimeConverter createMapConverter(TypeInformation keyTypeInfo, TypeInformation valueTypeInfo) {
		DeserializationRuntimeConverter keyConverter = createConverter(keyTypeInfo);
		DeserializationRuntimeConverter valueConverter = createConverter(valueTypeInfo);

		return assembleMapConverter(keyConverter, valueConverter);
	}

	private DeserializationRuntimeConverter assembleMapConverter(
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

	private boolean isPrimitiveByteArray(TypeInformation<?> typeInfo) {
		return typeInfo.equals(Types.PRIMITIVE_ARRAY(Types.BYTE));
	}

	private DeserializationRuntimeConverter createByteArrayConverter() {
		return (message, fieldDescriptors) -> ((ByteString) message).toByteArray();
	}
}
