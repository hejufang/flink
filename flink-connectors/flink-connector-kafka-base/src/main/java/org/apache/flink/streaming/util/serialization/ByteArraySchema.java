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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Schema for serialize/deserialize byte array data.
 */
public class ByteArraySchema implements DeserializationSchema<byte[]>, SerializationSchema<byte[]> {
	@Override
	public byte[] deserialize(byte[] bytes) throws IOException {
		return bytes;
	}

	@Override
	public boolean isEndOfStream(byte[] bytes) {
		return false;
	}

	@Override
	public TypeInformation<byte[]> getProducedType() {
		TypeInformation<byte[]> info = TypeInformation.of(byte[].class);
		return info;
	}

	@Override
	public byte[] serialize(byte[] bytes) {
		return bytes;
	}
}