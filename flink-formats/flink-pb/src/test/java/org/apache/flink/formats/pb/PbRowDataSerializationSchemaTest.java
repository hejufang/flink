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
 *
 */

package org.apache.flink.formats.pb;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.apache.flink.formats.pb.PbSchemaTestUtil.TEST_PB_CLASS_NAME;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link PbRowDataSerializationSchema}.
 */
public class PbRowDataSerializationSchemaTest {
	@Test
	public void testSerialize() {
		byte[] pbBytes = PbSchemaTestUtil.generatePbBytes();
		RowData rowData = PbSchemaTestUtil.generateRowData();

		testSerialize(getSerializationSchema(false), rowData, pbBytes);
		testSerialize(getSerializationSchema(true), GenericRowData.of(rowData), pbBytes);
	}

	private static void testSerialize(
			PbRowDataSerializationSchema serializationSchema,
			RowData rowData,
			byte[] expectedResult) {
		serializationSchema.open(null);
		byte[] serializedResult = serializationSchema.serialize(rowData);
		assertArrayEquals(expectedResult, serializedResult);
	}

	private static PbRowDataSerializationSchema getSerializationSchema(boolean withWrapper) {
		Descriptors.Descriptor descriptor = PbUtils.validateAndGetDescriptor(TEST_PB_CLASS_NAME);
		DataType dataType = PbFormatFactory.createDataType(descriptor, withWrapper);

		return PbRowDataSerializationSchema.builder()
			.setRowType((RowType) dataType.getLogicalType())
			.setPbDescriptorClass(TEST_PB_CLASS_NAME)
			.setWithWrapper(withWrapper)
			.build();
	}
}
