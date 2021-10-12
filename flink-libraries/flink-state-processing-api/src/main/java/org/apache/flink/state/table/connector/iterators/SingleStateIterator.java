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

package org.apache.flink.state.table.connector.iterators;

import org.apache.flink.state.table.connector.converter.Context;
import org.apache.flink.state.table.connector.converter.RowDataConverter;
import org.apache.flink.table.data.RowData;

import java.util.Iterator;
import java.util.Map;

/**
 * An iterator for traversing a single state.
 * For {@link org.apache.flink.api.common.state.MapState}, the result of next() is the {@link Map.Entry} in MapState,
 * for {@link org.apache.flink.api.common.state.ListState}, the corresponding is the element in the list,
 * for other type of State, it will only visit the single value and then reach the end.
 * Note that if it is a keyedState, it will only be accessed under the specific key and namespace.
 */
public class SingleStateIterator<V> implements Iterator<RowData>{

	private final Iterator<V> stateIterator;
	private RowDataConverter<V> rowDataConverter;
	private Context context;

	public SingleStateIterator(Iterator<V> stateIterator, RowDataConverter<V> rowDataConverter, Context context) {
		this.stateIterator = stateIterator;
		this.rowDataConverter = rowDataConverter;
		this.context = context;
	}

	@Override
	public boolean hasNext() {
		return stateIterator.hasNext();
	}

	@Override
	public RowData next() {
		return rowDataConverter.converterToRowData(stateIterator.next(), context);
	}
}
