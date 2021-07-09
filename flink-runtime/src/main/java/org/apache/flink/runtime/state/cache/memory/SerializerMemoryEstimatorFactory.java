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

package org.apache.flink.runtime.state.cache.memory;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.cache.CacheEntryKey;
import org.apache.flink.runtime.state.cache.CacheEntryValue;

/**
 * Provide corresponding memory estimators for different state types and wrap them as {@link SampleStateMemoryEstimator}.
 * Currently, the estimation of {@link org.apache.flink.api.common.state.ListState} is not supported.
 */
public class SerializerMemoryEstimatorFactory {
	public static <K, N, V> SampleStateMemoryEstimator<CacheEntryKey<?, N>, CacheEntryValue<?>> createSampleEstimator(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<V> valueSerializer,
		StateDescriptor.Type stateType,
		int estimateSampleCount) {

		TypeSerializer<K> duplicateKeySerializer = keySerializer.duplicate();
		TypeSerializer<N> duplicateNamespaceSerializer = namespaceSerializer.duplicate();
		TypeSerializer<V> duplicateValueSerializer = valueSerializer.duplicate();
		MemoryEstimator memoryEstimator;
		switch (stateType) {
			case VALUE:
			case FOLDING:
			case REDUCING:
			case AGGREGATING:
				memoryEstimator = new ValueStateSerializerEstimator<>(duplicateKeySerializer, duplicateNamespaceSerializer, duplicateValueSerializer);
				break;
			case MAP:
				memoryEstimator = new MapStateMemoryEstimator<>(duplicateKeySerializer, duplicateNamespaceSerializer, (MapSerializer) duplicateValueSerializer);
				break;
			case LIST:
				throw new UnsupportedOperationException("Unsupported state type " + stateType.name());
			default:
				throw new UnsupportedOperationException("Unknown state type " + stateType.name());
		}

		return new SampleStateMemoryEstimator<>(memoryEstimator, estimateSampleCount);
	}
}
