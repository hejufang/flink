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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.metadata.savepoint.SavepointSimpleMetadata;
import org.apache.flink.runtime.checkpoint.metadata.savepoint.SavepointSimpleMetadataV1Serializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test for SavepointSimpleMetadataSerializer.
 */
public class SavepointSimpleMetadataTest {
	@Test
	public void testSavepointSimpleMetaSerializerV1() throws IOException {
		SavepointSimpleMetadataV1Serializer serializer = SavepointSimpleMetadataV1Serializer.INSTANCE;
		ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
		DataOutputStream out = new DataOutputViewStreamWrapper(baos);

		SavepointSimpleMetadata metadata = new SavepointSimpleMetadata(10, "savepoint-path");
		SavepointSimpleMetadataV1Serializer.serialize(metadata, out);
		out.close();

		byte[] bytes = baos.toByteArray();

		DataInputStream in = new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(bytes));
		SavepointSimpleMetadata deserialized = serializer.deserialize(in, getClass().getClassLoader());
		assertEquals(10, deserialized.getCheckpointId());
		assertEquals("savepoint-path", deserialized.getActualSavepointPath());
	}
}
