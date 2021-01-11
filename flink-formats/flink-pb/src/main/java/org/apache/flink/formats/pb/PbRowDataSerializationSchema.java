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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.pb.SerializationRuntimeConverterFactory.SerializationRuntimeConverter;
import org.apache.flink.formats.pb.proto.ProtoFile;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.DynamicMessage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Deserialization RowData to pb bytes.
 */
public class PbRowDataSerializationSchema implements SerializationSchema<RowData> {

	private final boolean withWrapper;
	private final boolean sinkWithSizeHeader;
	private final String pbDescriptorClass;
	private final ProtoFile protoFile;
	private final RowType pbTypeInfo;
	private transient SerializationRuntimeConverter runtimeConverter;
	private transient ByteBuffer byteBuffer;
	private final boolean sizeHeaderWithLittleEndian;

	private PbRowDataSerializationSchema(
			RowType rowType,
			String pbDescriptorClass,
			ProtoFile protoFile,
			boolean withWrapper,
			boolean sinkWithSizeHeader,
			boolean sizeHeaderWithLittleEndian) {
		this.withWrapper = withWrapper;
		this.sinkWithSizeHeader = sinkWithSizeHeader;
		this.pbDescriptorClass = pbDescriptorClass;
		this.protoFile = protoFile;
		if (this.withWrapper) {
			int index = rowType.getFieldIndex(PbConstant.FORMAT_PB_WRAPPER_NAME);
			Preconditions.checkState(index >= 0,
				String.format("Field with name '%s' must exist while withWrapper is true.",
					PbConstant.FORMAT_PB_WRAPPER_NAME));
			pbTypeInfo = (RowType) rowType.getTypeAt(index);
		} else {
			pbTypeInfo = rowType;
		}
		this.sizeHeaderWithLittleEndian = sizeHeaderWithLittleEndian;
	}

	@Override
	public void open(InitializationContext context) {
		ByteOrder byteOrder =
			sizeHeaderWithLittleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		byteBuffer = ByteBuffer.allocate(Long.BYTES).order(byteOrder);
		runtimeConverter = SerializationRuntimeConverterFactory.createConverter(pbTypeInfo,
			PbUtils.validateAndGetDescriptor(pbDescriptorClass, protoFile));
	}

	@Override
	public byte[] serialize(RowData element) {
		if (withWrapper) {
			element = element.getRow(0, pbTypeInfo.getFieldCount());
		}
		DynamicMessage dynamicMessage = (DynamicMessage) runtimeConverter.convert(element);
		byte[] serializedBytes = dynamicMessage.toByteArray();
		if (sinkWithSizeHeader) {
			int size = serializedBytes.length;
			byte[] sizeByte = byteBuffer.putLong(size).array();
			byte[] newBytes = new byte[serializedBytes.length + sizeByte.length];
			System.arraycopy(sizeByte, 0, newBytes, 0, sizeByte.length);
			System.arraycopy(serializedBytes, 0, newBytes, sizeByte.length, serializedBytes.length);
			byteBuffer.clear();
			return newBytes;
		}
		return serializedBytes;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link PbRowDataSerializationSchema}.
	 * */
	public static class Builder {
		RowType rowType;
		private String pbDescriptorClass;
		private ProtoFile protoFile;
		private boolean withWrapper;
		private boolean sinkWithSizeHeader;
		private boolean sizeHeaderWithLittleEndian;

		private Builder() {}

		public Builder setRowType(RowType rowType) {
			this.rowType = rowType;
			return this;
		}

		public Builder setPbDescriptorClass(String pbDescriptorClass) {
			this.pbDescriptorClass = pbDescriptorClass;
			return this;
		}

		public Builder setProtoFile(ProtoFile protoFile) {
			this.protoFile = protoFile;
			return this;
		}

		public Builder setWithWrapper(boolean withWrapper) {
			this.withWrapper = withWrapper;
			return this;
		}

		public Builder setSinkWithSizeHeader(boolean sinkWithSizeHeader) {
			this.sinkWithSizeHeader = sinkWithSizeHeader;
			return this;
		}

		public Builder setSizeHeaderWithLittleEndian(boolean sizeHeaderWithLittleEndian) {
			this.sizeHeaderWithLittleEndian = sizeHeaderWithLittleEndian;
			return this;
		}

		public PbRowDataSerializationSchema build() {
			Preconditions.checkNotNull(rowType, "rowType is cannot be null!");
			checkState(pbDescriptorClass != null || protoFile != null,
				"'pbDescriptorClass' and 'protoFile' can not be null at the same time.");
			return new PbRowDataSerializationSchema(
				rowType,
				pbDescriptorClass,
				protoFile,
				withWrapper,
				sinkWithSizeHeader,
				sizeHeaderWithLittleEndian);
		}
	}
}
