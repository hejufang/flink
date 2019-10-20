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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Spliterators.spliterator;
import static java.util.stream.StreamSupport.stream;

/**
 * Default value schema that gives a default value to a json field.
 */
public class JsonRowDefaultValue implements Serializable {

	private static final long serialVersionUID = -6345049740204168767L;

	public DefaultValueRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
		DefaultValueRuntimeConverter baseConverter = createConverterForSimpleType(typeInfo)
			.orElseGet(() ->
				createContainerConverter(typeInfo)
					.orElseGet(() -> null));
		return baseConverter;
	}

	private DefaultValueRuntimeConverter createObjectArrayConverter(TypeInformation elementTypeInfo) {
		DefaultValueRuntimeConverter elementConverter = createConverter(elementTypeInfo);
		return assembleArrayConverter(elementConverter);
	}

	private DefaultValueRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
		List<DefaultValueRuntimeConverter> fieldConverters = Arrays.stream(typeInfo.getFieldTypes())
			.map(this::createConverter)
			.collect(Collectors.toList());

		return assembleRowConverter(typeInfo.getFieldNames(), fieldConverters);
	}

	private DefaultValueRuntimeConverter assembleArrayConverter(DefaultValueRuntimeConverter elementConverter) {
		return () -> {
			return stream(spliterator(new Object[1], 0), false)
				.map(x -> elementConverter.convert())
				.toArray();
		};
	}

	private DefaultValueRuntimeConverter assembleRowConverter(
		String[] fieldNames,
		List<DefaultValueRuntimeConverter> fieldConverters) {
		return () -> {
			int arity = fieldNames.length;
			Row row = new Row(arity);
			for (int i = 0; i < arity; i++) {
				Object convertField = fieldConverters.get(i).convert();
				row.setField(i, convertField);
			}

			return row;
		};
	}

	private Optional<DefaultValueRuntimeConverter> createConverterForSimpleType(TypeInformation<?> simpleTypeInfo) {
		if (simpleTypeInfo == Types.VOID) {
			return Optional.of(() -> null);
		} else if (simpleTypeInfo == Types.BOOLEAN) {
			return Optional.of(() -> false);
		} else if (simpleTypeInfo == Types.STRING) {
			return Optional.of(() -> "");
		} else if (simpleTypeInfo == Types.INT) {
			return Optional.of(() -> 0);
		} else if (simpleTypeInfo == Types.LONG) {
			return Optional.of(() -> 0L);
		} else if (simpleTypeInfo == Types.DOUBLE) {
			return Optional.of(() -> 0.0);
		} else if (simpleTypeInfo == Types.FLOAT) {
			return Optional.of(() -> 0.0);
		} else if (simpleTypeInfo == Types.SHORT) {
			return Optional.of(() -> 0);
		} else if (simpleTypeInfo == Types.BYTE) {
			return Optional.of(() -> 0);
		} else if (simpleTypeInfo == Types.BIG_DEC) {
			return Optional.of(() -> BigDecimal.ZERO);
		} else if (simpleTypeInfo == Types.BIG_INT) {
			return Optional.of(() -> BigInteger.ZERO);
		} else if (simpleTypeInfo == Types.SQL_DATE) {
			return Optional.of(createDateConverter());
		} else if (simpleTypeInfo == Types.SQL_TIME) {
			return Optional.of(createTimeConverter());
		} else if (simpleTypeInfo == Types.SQL_TIMESTAMP) {
			return Optional.of(createTimestampConverter());
		} else {
			return Optional.empty();
		}
	}

	private Optional<DefaultValueRuntimeConverter> createContainerConverter(TypeInformation<?> typeInfo) {
		if (typeInfo instanceof RowTypeInfo) {
			return Optional.of(createRowConverter((RowTypeInfo) typeInfo));
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((ObjectArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (typeInfo instanceof BasicArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((BasicArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (isPrimitiveByteArray(typeInfo)) {
			return Optional.of(createByteArrayConverter());
		} else if (typeInfo instanceof MapTypeInfo) {
			return Optional.of(createMapConverter());
		} else {
			return Optional.empty();
		}
	}

	private DefaultValueRuntimeConverter createMapConverter() {
		return HashMap::new;
	}

	private boolean isPrimitiveByteArray(TypeInformation<?> typeInfo) {
		return typeInfo instanceof PrimitiveArrayTypeInfo &&
			((PrimitiveArrayTypeInfo) typeInfo).getComponentType() == Types.BYTE;
	}

	private DefaultValueRuntimeConverter createByteArrayConverter() {
		return () -> new byte[0];
	}

	private DefaultValueRuntimeConverter createDateConverter() {
		return () -> Date.valueOf(LocalDate.now());
	}

	private DefaultValueRuntimeConverter createTimestampConverter() {
		return () -> new Timestamp(System.currentTimeMillis());
	}

	private DefaultValueRuntimeConverter createTimeConverter() {
		return () -> new Time(System.currentTimeMillis());
	}

	/**
	 * Runtime converter to default value.
	 */
	@FunctionalInterface
	public interface DefaultValueRuntimeConverter extends Serializable {
		Object convert();
	}

}
