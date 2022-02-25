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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.pb.proto.ProtoFile;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.formats.pb.PbSchemaTestUtil.TEST_PB_CLASS_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PbRowDataDeserializationSchema}.
 */
public class PbRowDataDeserializationSchemaTest {
	@Test
	public void testDeserialize() throws IOException {
		byte[] pbBytes = PbSchemaTestUtil.generatePbBytes();
		RowData rowData = PbSchemaTestUtil.generateRowData();

		testDeserialization(getDeserializationSchema(false, true, null), pbBytes, rowData);
		testDeserialization(getDeserializationSchema(true, true, null), pbBytes, GenericRowData.of(rowData));

		// Fields order in schema inferred from proto file are not the same with
		// the order in schema inferred from pb class.
		//So the expected row are the same fields with different order.
		RowData rowDataForProtoFile = PbSchemaTestUtil.generateRowDataForProtoFile();
		testDeserialization(getDeserializationSchema(false, false, null), pbBytes, rowDataForProtoFile);
		testDeserialization(getDeserializationSchema(true, false, null), pbBytes, GenericRowData.of(rowDataForProtoFile));
	}

	@Test
	public void testDeserializeSelectedField() throws IOException {
		byte[] pbBytes = PbSchemaTestUtil.generatePbBytes();
		RowData rowData = PbSchemaTestUtil.generateSelectedRowData();
		RowType rowType = PbSchemaTestUtil.generateSelectedRowType();

		testDeserialization(getDeserializationSchema(false, true, rowType), pbBytes, rowData);
		testDeserialization(getDeserializationSchema(false, false, rowType), pbBytes, rowData);
	}

	@Test
	public void testDeserializeSelectedFieldWithRuntimePbCut() throws IOException {
		byte[] pbBytes = PbSchemaTestUtil.generatePbBytes();
		RowData rowData = PbSchemaTestUtil.generateSelectedRowData();
		RowType rowType = PbSchemaTestUtil.generateSelectedRowType();

		PbRowDataDeserializationSchema derByClass = getDeserializationSchema(false, true, true, rowType);
		testDeserialization(derByClass, pbBytes, rowData);
		assertEquals(rowType.getFieldCount(), derByClass.getPbDescriptor().getFields().size());
		PbRowDataDeserializationSchema derByFile = getDeserializationSchema(false, false, true, rowType);
		testDeserialization(derByFile, pbBytes, rowData);
		assertEquals(rowType.getFieldCount(), derByClass.getPbDescriptor().getFields().size());
	}

	private static void testDeserialization(
			PbRowDataDeserializationSchema deserializationSchema,
			byte[] originBytes,
			RowData expectedResult) throws IOException {
		deserializationSchema.open(null);
		RowData deserializedResult = deserializationSchema.deserialize(originBytes);
		assertArrayEquals(new Object[]{expectedResult}, new Object[]{deserializedResult});
	}

	private static PbRowDataDeserializationSchema getDeserializationSchema(
			boolean withWrapper,
			boolean withPbClassFullName,
			@Nullable RowType selectedRowType) throws IOException {
		return getDeserializationSchema(withWrapper, withPbClassFullName, false, selectedRowType);
	}

	private static PbRowDataDeserializationSchema getDeserializationSchema(
			boolean withWrapper,
			boolean withPbClassFullName,
			boolean runtimePbCut,
			@Nullable RowType selectedRowType) throws IOException {
		Descriptors.Descriptor descriptor;
		String pbClassName = null;
		ProtoFile protoFile = null;
		if (withPbClassFullName) {
			pbClassName = TEST_PB_CLASS_NAME;
		} else {
			protoFile = PbSchemaTestUtil.getProtoFile();
		}
		descriptor = PbUtils.validateAndGetDescriptor(pbClassName, protoFile);
		if (selectedRowType == null) {
			selectedRowType =
				(RowType) PbFormatUtils.createDataType(descriptor, withWrapper).getLogicalType();
		}

		return PbRowDataDeserializationSchema.builder()
			.setRowType(selectedRowType)
			.setResultTypeInfo(TypeInformation.of(RowData.class))
			.setPbDescriptorClass(pbClassName)
			.setProtoFile(protoFile)
			.setWithWrapper(withWrapper)
			.setRuntimeCutPb(runtimePbCut)
			.build();
	}
}
