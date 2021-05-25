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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter for {@link MapData} of {@link Map} external type.
 * @param <K>
 * @param <V>
 */
public class MapMapConverter<K, V> implements DataStructureConverter<MapData, Map<K, V>> {
	private static final long serialVersionUID = 1L;

	private final ArrayArrayConverter<K> keyConverter;

	private final ArrayArrayConverter<V> valueConverter;

	private MapMapConverter(
			ArrayArrayConverter<K> keyConverter,
			ArrayArrayConverter<V> valueConverter) {
		this.keyConverter = keyConverter;
		this.valueConverter = valueConverter;
	}

	@Override
	public void open(ClassLoader classLoader) {
		keyConverter.open(classLoader);
		valueConverter.open(classLoader);
	}

	@Override
	public MapData toInternal(Map<K, V> external) {
		Map<Object, Object> newMap = new HashMap<>();
		for (Map.Entry<K, V> item: external.entrySet()) {
			newMap.put(keyConverter.elementConverter.toInternal(item.getKey()),
				valueConverter.elementConverter.toInternal(item.getValue()));
		}
		return new GenericMapData(newMap);
	}

	@Override
	public Map<K, V> toExternal(MapData internal) {
		final ArrayData keyArray = internal.keyArray();
		final ArrayData valueArray = internal.valueArray();
		final int length = internal.size();
		final Map<K, V> map = new HashMap<>();
		for (int pos = 0; pos < length; pos++) {
			final Object keyValue = keyConverter.elementGetter.getElementOrNull(keyArray, pos);
			final Object valueValue = valueConverter.elementGetter.getElementOrNull(valueArray, pos);
			if (keyValue == null || valueValue == null) {
				throw new RuntimeException(String.format("Both key and value of the map should not be null. " +
					"The key is %s, the value is %s.", keyValue, valueValue));
			}
			map.put(
				keyConverter.elementConverter.toExternal(keyValue),
				valueConverter.elementConverter.toExternal(valueValue));
		}
		return map;
	}
	// --------------------------------------------------------------------------------------------
	// Factory method
	// --------------------------------------------------------------------------------------------

	public static MapMapConverter<?, ?> createForMapType(Type[] types, DataType dataType) {
		final DataType keyDataType = dataType.getChildren().get(0);
		final DataType valueDataType = dataType.getChildren().get(1);
		return new MapMapConverter<>(
			ArrayArrayConverter.createForElement(types[0], keyDataType),
			ArrayArrayConverter.createForElement(types[1], valueDataType)
		);
	}
}
