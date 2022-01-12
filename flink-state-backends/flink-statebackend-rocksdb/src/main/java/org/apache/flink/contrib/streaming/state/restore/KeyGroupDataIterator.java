/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.contrib.streaming.state.RocksDBIncrementalCheckpointUtils;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.IOException;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;

/**
 * Iterator for State traversal in the order of KeyGroup.
 */
public interface KeyGroupDataIterator<K, V> extends AutoCloseable {

	/** Returns {@code true} if the iteration has more elements. */
	boolean hasNext();

	/** Move to the next element in the iteration. */
	void next() throws IOException;

	/** Return the key for the current entry. */
	K key();

	/** Return the value for the current entry. */
	V value();

	/**
	 * The wrapper class of RocksIteratorWrapper.
	 */
	class RocksDBKeyGroupIterator implements KeyGroupDataIterator<byte[], byte[]> {

		private final RocksIteratorWrapper iterator;
		private final byte[] startKeyGroupPrefixBytes;
		private final byte[] stopKeyGroupPrefixBytes;

		public RocksDBKeyGroupIterator(RocksIteratorWrapper iterator, byte[] startKeyGroupPrefixBytes, byte[] stopKeyGroupPrefixBytes) {
			this.iterator = iterator;
			this.startKeyGroupPrefixBytes = startKeyGroupPrefixBytes;
			this.stopKeyGroupPrefixBytes = stopKeyGroupPrefixBytes;
			this.iterator.seek(this.startKeyGroupPrefixBytes);
		}

		@Override
		public boolean hasNext() {
			return iterator.isValid() && RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes);
		}

		@Override
		public void next() {
			iterator.next();
		}

		@Override
		public byte[] key() {
			return iterator.key();
		}

		@Override
		public byte[] value() {
			return iterator.value();
		}

		@Override
		public void close() throws Exception {
			iterator.close();
		}
	}

	/**
	 * The wrapper class of full snpashot RocksIteratorWrapper.
	 */
	class FullSnapshotRocksDBKeyGroupIterator implements KeyGroupDataIterator<byte[], byte[]> {
		private final DataInputViewStreamWrapper compressedKgInputView;
		private byte[] key;
		private byte[] value;
		private boolean lastKey = false;

		public FullSnapshotRocksDBKeyGroupIterator(DataInputViewStreamWrapper compressedKgInputView, byte[] key, byte[] value) {
			this.compressedKgInputView = compressedKgInputView;
			this.key = key;
			this.value = value;
		}

		@Override
		public boolean hasNext() {
			if (lastKey) {
				return false;
			}
			if (hasMetaDataFollowsFlag(key)) {
				clearMetaDataFollowsFlag(key);
				lastKey = true;
			}
			return true;
		}

		@Override
		public void next() throws IOException {
			if (!lastKey) {
				this.key = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
				this.value = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
			}
		}

		@Override
		public byte[] key() {
			return this.key;
		}

		@Override
		public byte[] value() {
			return this.value;
		}

		@Override
		public void close() throws Exception {}
	}
}
