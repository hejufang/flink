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

package org.apache.flink.rocketmq.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** The serializer of RocketMQ source enumerator. */
public class RocketMQEnumStateSerializer implements SimpleVersionedSerializer<RocketMQEnumState> {
	private static final int VERSION_0 = 0;

	@Override
	public int getVersion() {
		return VERSION_0;
	}

	@Override
	public byte[] serialize(RocketMQEnumState obj) throws IOException {
		return new byte[0];
	}

	@Override
	public RocketMQEnumState deserialize(int version, byte[] serialized) throws IOException {
		return new RocketMQEnumState();
	}
}
