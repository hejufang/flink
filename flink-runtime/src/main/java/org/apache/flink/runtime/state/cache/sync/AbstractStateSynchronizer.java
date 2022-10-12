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

package org.apache.flink.runtime.state.cache.sync;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyedStateBackend;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * The abstract parent class of the State synchronizer, the underlying key is set
 * through {@link org.apache.flink.runtime.state.KeyedStateBackend}, and the specific
 * state operations are performed by the subclass.
 */
public abstract class AbstractStateSynchronizer<K, N, UK, UV> implements DataSynchronizer<Tuple3<K, N, UK>, UV> {
	protected final KeyedStateBackend<K> keyedStateBackend;

	public AbstractStateSynchronizer(KeyedStateBackend<K> keyedStateBackend) {
		this.keyedStateBackend = keyedStateBackend;
	}

	@Override
	public void saveState(Tuple3<K, N, UK> key, UV value) throws Exception {
		keyedStateBackend.setCurrentKey(key.f0);
		updateDelegateState(key, value);
	}

	@Override
	@Nullable
	public UV loadState(Tuple3<K, N, UK> key) throws Exception {
		keyedStateBackend.setCurrentKey(key.f0);
		UV data = loadFromDelegateState(key);
		return data;
	}

	@Override
	public void removeState(Tuple3<K, N, UK> key) throws Exception {
		keyedStateBackend.setCurrentKey(key.f0);
		removeFromDelegateState(key);
	}

	@Override
	public void flush(boolean force) throws IOException {
		//currently do nothing
	}

	protected abstract void updateDelegateState(Tuple3<K, N, UK> key, UV value) throws Exception;

	protected abstract UV loadFromDelegateState(Tuple3<K, N, UK> key) throws Exception;

	protected abstract void removeFromDelegateState(Tuple3<K, N, UK> key) throws Exception;
}