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
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.rpc.thrift.conversion.ArrayArrayConverter.createForElement;

/**
 * Converter for {@link ArrayData} of {@link Set} external type.
 */
public class ArraySetConverter<E> implements DataStructureConverter<ArrayData, Set<E>> {
	private final ArrayArrayConverter<E> arrayConverter;

	public ArraySetConverter(ArrayArrayConverter<E> arrayConverter) {
		this.arrayConverter = arrayConverter;
	}

	@Override
	public void open(ClassLoader classLoader) {
		arrayConverter.open(classLoader);
	}

	@Override
	public ArrayData toInternal(Set<E> external) {
		return arrayConverter.toInternal(new ArrayList<>(external));
	}

	@Override
	public Set<E> toExternal(ArrayData internal) {
		return new HashSet<>(arrayConverter.toExternal(internal));
	}

	// --------------------------------------------------------------------------------------------
	// Factory method
	// --------------------------------------------------------------------------------------------

	public static ArraySetConverter<?> create(Type elementType, DataType dataType) {
		DataType element = dataType.getChildren().get(0);
		return new ArraySetConverter<>(createForElement(elementType, element));
	}

}
