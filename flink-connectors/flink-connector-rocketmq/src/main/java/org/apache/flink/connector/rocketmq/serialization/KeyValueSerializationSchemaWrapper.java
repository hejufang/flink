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

package org.apache.flink.connector.rocketmq.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Wrap a SerializationSchema to KeyValueDeserializationSchema.
 */
public class KeyValueSerializationSchemaWrapper<T> implements KeyValueSerializationSchema<T>{
	private static final long serialVersionUID = 1L;

	private final SerializationSchema<T> serializationSchema;

	public KeyValueSerializationSchemaWrapper(SerializationSchema<T> serializationSchema) {
		this.serializationSchema = serializationSchema;
	}

	@Override
	public byte[] serializeKey(T tuple) {
		return null;
	}

	@Override
	public byte[] serializeValue(T tuple) {
		return serializationSchema.serialize(tuple);
	}
}
