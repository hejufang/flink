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

package org.apache.flink.state.table.connector.converter;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

/**
 *  A class used to convert the keyed State to the {@link RowData}
 *  with schema  {@link org.apache.flink.state.table.catalog.SavepointCatalogUtils#STATE_META_TABLE_SCHEMA}.
 */
public class KeyedStateRowDataConverter<V> implements RowDataConverter<V> {

	private final DynamicTableSource.DataStructureConverter converter;

	public KeyedStateRowDataConverter(DynamicTableSource.DataStructureConverter converter){
		this.converter = converter;
	}

	@Override
	public RowData converterToRowData(V value, Context context){

		KeyedStateConverterContext keyedStateConverterContext = (KeyedStateConverterContext) context;

		Row row = new Row(3);
		row.setField(0, keyedStateConverterContext.getKey().toString());
		row.setField(1, keyedStateConverterContext.getNamespace().toString());
		row.setField(2, value.toString());

		return (RowData) converter.toInternal(row);
	}


	/**
	 * @param <K> current key
	 * @param <N> current namespace
	 */
	public static class KeyedStateConverterContext<K , N> implements Context {

		private K key;
		private N namespace;

		public K getKey() {
			return key;
		}

		public void setKey(K key) {
			this.key = key;
		}

		public N getNamespace() {
			return namespace;
		}

		public void setNamespace(N namespace) {
			this.namespace = namespace;
		}
	}

}


