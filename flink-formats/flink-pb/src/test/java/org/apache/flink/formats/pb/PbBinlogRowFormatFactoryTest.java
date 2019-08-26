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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PbBinlogRowFormatFactory}.
 */
public class PbBinlogRowFormatFactoryTest {
	@Test
	public void testDeserializationSchema() {
		final Map<String, String> properties = new HashMap<>();
		properties.put("format.type", "pb_binlog");

		final DeserializationSchema<?> deserializationSchema = TableFactoryService
			.find(DeserializationSchemaFactory.class, properties)
			.createDeserializationSchema(properties);

		TypeInformation[] types = ((RowTypeInfo) PbBinlogRowFormatFactory.getBinlogRowTypeInformation()).getFieldTypes();

		final PbBinlogRowDeserializationSchema expectedSchema = PbBinlogRowDeserializationSchema.Builder.newBuilder()
			.setHeaderType(types[0])
			.setEntryTypeType(types[1])
			.setTransactionBeginTypeInfo(types[2])
			.setRowChangeTypeInfo(types[3])
			.setTransactionEndTypeInfo(types[4])
			.build();

		assertEquals(expectedSchema, deserializationSchema);
	}
}
