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

package org.apache.flink.formats.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.junit.Test;

/**
 * Test {@link PbRowDeserializationSchema}.
 */
public class PbRowDeserializationSchemaTest {
	@Test
	public void testSchemaDeserialization() throws Exception {
		PbDeserializeTest.TestPbDeserailize.Builder deserializedRowBuilder = PbDeserializeTest.TestPbDeserailize.newBuilder()
			.setIntTest(5)
			.setLongTest(123424234L)
			.setStringTest("string Test")
			.setBoolTest(true)
			.setDoubleTest(2314.3)
			.setFloatTest(1.4f)
			.setEnumTest(PbDeserializeTest.EnumCase.FOUR)
			.setBytesTest(ByteString.copyFrom("bytes test".getBytes()));

		PbDeserializeTest.MessageCase.Builder messageCaseBuilder1 = PbDeserializeTest.MessageCase.newBuilder()
			.setStringTestInMessage("string in message 1")
			.putMapTestInMessage("map string 1", 1)
			.putMapTestInMessage("map string 2", 2);

		PbDeserializeTest.MessageCase.Builder messageCaseBuilder2 = PbDeserializeTest.MessageCase.newBuilder()
			.setStringTestInMessage("string in message 2")
			.putMapTestInMessage("map string 4", 4)
			.putMapTestInMessage("map string 5", 5);

		deserializedRowBuilder.addArrayTest(messageCaseBuilder1);
		deserializedRowBuilder.addArrayTest(messageCaseBuilder2);

		Descriptors.Descriptor descriptor = PbDeserializeTest.TestPbDeserailize.getDescriptor();

		PbRowDeserializationSchema schema = PbRowDeserializationSchema.Builder.newBuilder()
			.setTypeInfo(PbRowTypeInformation.generateRow(descriptor))
			.setPbDescriptorClass("org.apache.flink.formats.pb.PbDeserializeTest$TestPbDeserailize")
			.build();

		System.out.println(schema.deserialize(deserializedRowBuilder.build().toByteArray()));
	}
}
