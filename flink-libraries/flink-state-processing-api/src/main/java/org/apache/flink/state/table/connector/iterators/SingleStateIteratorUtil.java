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

package org.apache.flink.state.table.connector.iterators;

import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Util class to create StateIterator.
 */
public class SingleStateIteratorUtil {

	/**
	 * StateIteratorFactory.
	 */
	public interface SingleStateIteratorFactory {
		<S extends State> Iterator createIteratorFactory(S state) throws Exception;
	}

	private static final Map<StateDescriptor.Type, SingleStateIteratorFactory> STATE_ITERATOR_FACTORIES =
		Stream.of(
			Tuple2.of(
					StateDescriptor.Type.MAP,
					(SingleStateIteratorFactory) SingleStateIteratorUtil::createMapStateIterator),
			Tuple2.of(
					StateDescriptor.Type.LIST,
					(SingleStateIteratorFactory) SingleStateIteratorUtil::createListStateIterator),
			Tuple2.of(
					StateDescriptor.Type.VALUE,
					(SingleStateIteratorFactory) SingleStateIteratorUtil::createSingleValueStateIterator),
			Tuple2.of(
					StateDescriptor.Type.AGGREGATING,
					(SingleStateIteratorFactory) SingleStateIteratorUtil::createSingleValueStateIterator),
			Tuple2.of(
					StateDescriptor.Type.REDUCING,
					(SingleStateIteratorFactory) SingleStateIteratorUtil::createSingleValueStateIterator))
			.collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	private static <S extends State> Iterator createMapStateIterator(S state) throws Exception {
			return ((MapState) state).iterator();
		}

	private static <S extends State> Iterator createListStateIterator(S state) throws Exception {
		return ((Iterable) ((ListState) state).get()).iterator();
	}

	private static<S extends State, T> Iterator createSingleValueStateIterator(S state){

		return new Iterator() {
			boolean hasVisit = false;

			@Override
			public boolean hasNext() {
				return !hasVisit;
			}

			@Override
			public T next(){
				hasVisit = true;
				try {
					if (state instanceof ValueState) {
						return ((ValueState<T>) state).value();
					} else if (state instanceof AppendingState) {
						return ((AppendingState<?, T>) state).get();
					} else {
						throw new RuntimeException("Unsupported state type");
					}
				} catch (Exception e){
					throw new RuntimeException("get value failed");
				}
			}
		};
	}

	public static SingleStateIteratorFactory getStateIteratorFactory(StateDescriptor stateDescriptor){
		return STATE_ITERATOR_FACTORIES.get(stateDescriptor.getType());
	}

	public static <S extends State> Iterator getStateIterator(StateDescriptor stateDescriptor, S state) throws Exception {
		return getStateIteratorFactory(stateDescriptor).createIteratorFactory(state);
	}

}
