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

package org.apache.flink.formats.protobuf.serialize;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.formats.protobuf.PbSchemaValidator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.Descriptors;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Serialization schema from Flink to Protobuf types.
 *
 * <p>Serializes a {@link RowData } to protobuf binary data.
 *
 * <p>Failures during deserialization are forwarded as wrapped {@link FlinkRuntimeException}.
 */
public class PbRowDataSerializationSchema implements SerializationSchema<RowData> {

	private final RowType rowType;

	private final PbFormatConfig pbFormatConfig;

	private transient ByteBuffer byteBuffer;

	private transient RowToProtoConverter rowToProtoConverter;

	public PbRowDataSerializationSchema(RowType rowType, PbFormatConfig pbFormatConfig) {
		this.pbFormatConfig = pbFormatConfig;
		Descriptors.Descriptor descriptor =
			PbFormatUtils.getDescriptor(pbFormatConfig.getPbDescriptorClass());
		new PbSchemaValidator(descriptor, rowType).validate();
		if (pbFormatConfig.isWithWrapper()) {
			int index = rowType.getFieldIndex(PbConstant.FORMAT_PB_WRAPPER_NAME);
			Preconditions.checkState(index >= 0,
				String.format("Field with name '%s' must exist while withWrapper is true.",
					PbConstant.FORMAT_PB_WRAPPER_NAME));
			this.rowType = (RowType) rowType.getTypeAt(index);
		} else {
			this.rowType = rowType;
		}
		try {
			// validate converter in client side to early detect errors
			rowToProtoConverter = new RowToProtoConverter(rowType, pbFormatConfig);
		} catch (PbCodegenException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		ByteOrder byteOrder =
			pbFormatConfig.isSizeHeaderWithLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byteBuffer = ByteBuffer.allocate(Long.BYTES).order(byteOrder);
		rowToProtoConverter = new RowToProtoConverter(rowType, pbFormatConfig);
	}

	@Override
	public byte[] serialize(RowData element) {
		if (pbFormatConfig.isWithWrapper()) {
			element = element.getRow(0, rowType.getFieldCount());
		}
		try {
			byte[] serializedBytes = rowToProtoConverter.convertRowToProtoBinary(element);
			if (pbFormatConfig.isSinkWithSizeHeader()) {
				int size = serializedBytes.length;
				byte[] sizeByte = byteBuffer.putLong(size).array();
				byte[] newBytes = new byte[serializedBytes.length + sizeByte.length];
				System.arraycopy(sizeByte, 0, newBytes, 0, sizeByte.length);
				System.arraycopy(serializedBytes, 0, newBytes, sizeByte.length, serializedBytes.length);
				byteBuffer.clear();
				return newBytes;
			}
			return serializedBytes;
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		}
	}
}
