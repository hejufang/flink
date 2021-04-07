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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connector.rpc.util.ObjectUtil.getClzAndGenericTypes;

/**
 * Registry of java bean converters.
 */
public final class JavaBeanConverters {
	@SuppressWarnings({"unchecked"})
	public static DataStructureConverter<?, Object> getConverter(Type type, DataType dataType) {
		return (DataStructureConverter<Object, Object>) getConverterInternal(type, dataType);
	}

	private static DataStructureConverter<?, ?> getConverterInternal(
			Type type,
			DataType dataType) {
		Tuple2<Class<?>, Type[]> infos = getClzAndGenericTypes(type);
		Class<?> clz = infos.f0;
		Type[] innerTypes = infos.f1;
		switch (dataType.getLogicalType().getTypeRoot()) {
			case ROW:
				return RowJavaBeanConverter.create(clz, dataType);
			case MAP:
				return MapMapConverter.createForMapType(innerTypes, dataType);
			case ARRAY:
				if (clz == List.class) {
					return ArrayArrayConverter.create(innerTypes[0], dataType);
				} else if (clz == Set.class){
					return ArraySetConverter.create(innerTypes[0], dataType);
				} else {
					return DataStructureConverters.getConverter(dataType);
				}
			case VARCHAR:
				if (clz.isEnum()) {
					return new StringDataEnumConverter(clz);
				} else {
					return DataStructureConverters.getConverter(dataType);
				}
			case VARBINARY:
			case BINARY:
				if (clz == ByteBuffer.class) {
					return new ByteArrayByteBufferConverter();
				} else {
					return DataStructureConverters.getConverter(dataType);
				}
			default:
				return DataStructureConverters.getConverter(dataType);
		}
	}
}
