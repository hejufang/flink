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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Snapshot class for the {@link TimerSerializer}.
 */
@Internal
public class TimerSerializerSnapshot<K, N> extends CompositeTypeSerializerSnapshot<TimerHeapInternalTimer<K, N>, TimerSerializer<K, N>> {

//	private static final int VERSION = 2;
	/** Upgrade this version to determine whether the TimerSerializer serialized the payload. */
	private static final int VERSION = 3;

	private boolean serializePayload;

	public TimerSerializerSnapshot() {
		super(TimerSerializer.class);
	}

	public TimerSerializerSnapshot(TimerSerializer<K, N> timerSerializer) {
		super(timerSerializer);
		serializePayload = timerSerializer.isSerializePayload();
	}

	@Override
	protected int getCurrentOuterSnapshotVersion() {
		return VERSION;
	}

	@Override
	protected TimerSerializer<K, N> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		@SuppressWarnings("unchecked")
		final TypeSerializer<K> keySerializer = (TypeSerializer<K>) nestedSerializers[0];

		@SuppressWarnings("unchecked")
		final TypeSerializer<N> namespaceSerializer = (TypeSerializer<N>) nestedSerializers[1];

		return new TimerSerializer<>(keySerializer, namespaceSerializer, serializePayload);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(TimerSerializer<K, N> outerSerializer) {
		return new TypeSerializer<?>[] { outerSerializer.getKeySerializer(), outerSerializer.getNamespaceSerializer() };
	}

	@Override
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {
		out.writeBoolean(serializePayload);
	}

	@Override
	protected void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		if (readOuterSnapshotVersion < VERSION) {
			serializePayload = false;
		} else {
			serializePayload = in.readBoolean();
		}
	}

	@Override
	protected boolean isOuterSnapshotCompatible(TimerSerializer<K, N> newSerializer) {
		return serializePayload == newSerializer.isSerializePayload();
	}
}
