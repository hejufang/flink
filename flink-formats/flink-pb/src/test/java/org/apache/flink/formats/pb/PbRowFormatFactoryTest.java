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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;

import com.google.protobuf.Descriptors;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PbRowFormatFactory}.
 */
public class PbRowFormatFactoryTest {
	@Test
	public void testDeserializationSchema() {
		String pbClass = "org.apache.flink.formats.pb.PbDeserializeTest$TestPbDeserailize";
		final Map<String, String> properties = new HashMap<>();
		properties.put("format.type", "pb");
		properties.put("format.skip-bytes", "1");
		properties.put("format.pb-class", pbClass);

		final DeserializationSchema<?> deserializationSchema = TableFactoryService
			.find(DeserializationSchemaFactory.class, properties)
			.createDeserializationSchema(properties);

		Descriptors.Descriptor descriptor = PbDeserializeTest.TestPbDeserailize.getDescriptor();
		final PbRowDeserializationSchema expectedSchema = PbRowDeserializationSchema.Builder.newBuilder()
			.setPbDescriptorClass(pbClass)
			.setTypeInfo(PbRowTypeInformation.generateRow(descriptor))
			.setSkipBytes(1)
			.build();

		assertEquals(expectedSchema, deserializationSchema);
	}
}
