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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.state.table.connector.converter.KeyedStateRowDataConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * This class provides entry points for reading keyed state with all keys and namespaces.
 * It will use {@link AbstractKeyedStateBackend#getKeysAndNamespaces(String)} to traverse each key and namespace,
 * then create {@link SingleStateIterator} to traverse every single State.
 */
public class KeyedStateIterator<K, N, S extends State, T> implements CloseableIterator<RowData> {

	private final StateDescriptor<S, T> descriptor;
	private final AbstractKeyedStateBackend<K> backend;
	private final Iterator<Tuple2<K, N>> keysAndNamespaces;
	private final TypeSerializer<N> namespaceSerializer;
	private final AutoCloseable resource;

	private SingleStateIterator<RowData> singleStateIterator;
	private Tuple2<K, N> curKeyAndNamespace;
	private KeyedStateRowDataConverter rowDataConverter;
	private KeyedStateRowDataConverter.KeyedStateConverterContext context;

	public KeyedStateIterator(StateDescriptor<S, T> descriptor, AbstractKeyedStateBackend<K> backend, TypeSerializer<N> namespaceSerializer, KeyedStateRowDataConverter rowDataConverter) {

		this.descriptor = descriptor;
		this.backend = backend;
		this.namespaceSerializer = namespaceSerializer;
		this.resource = backend.getKeysAndNamespaces(descriptor.getName());
		this.rowDataConverter = rowDataConverter;
		this.keysAndNamespaces = ((Stream<Tuple2<K, N>>) resource).iterator();
		this.context = new KeyedStateRowDataConverter.KeyedStateConverterContext();

	}

	@Override
	public boolean hasNext() {

		while (true) {
			if (singleStateIterator != null) {
				if (singleStateIterator.hasNext()) {
					return true;
				} else {
					singleStateIterator = null;
				}
			} else if (keysAndNamespaces.hasNext()) {

				curKeyAndNamespace = keysAndNamespaces.next();
				try {
					S state = backend.getPartitionedState(curKeyAndNamespace.f1, namespaceSerializer, descriptor);
					context.setKey(curKeyAndNamespace.f0);
					context.setNamespace(curKeyAndNamespace.f1);
					backend.setCurrentKey(curKeyAndNamespace.f0);
					singleStateIterator = new SingleStateIterator(SingleStateIteratorUtil.getStateIterator(descriptor, state), rowDataConverter, context);

				} catch (Exception e) {
					throw new RuntimeException("get Partitioned State failed", e);
				}
			} else {
				return false;
			}
		}
	}

	@Override
	public RowData next() {
		return singleStateIterator.next();
	}

	@Override
	public void close() throws Exception {
		resource.close();
	}

}
