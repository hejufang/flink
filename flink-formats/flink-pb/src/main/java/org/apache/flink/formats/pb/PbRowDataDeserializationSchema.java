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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.pb.DeserializationRuntimeConverterFactory.DeserializationRuntimeConverter;
import org.apache.flink.formats.pb.proto.ProtoCutUtil;
import org.apache.flink.formats.pb.proto.ProtoFile;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.CodedStreamHelper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Deserialization schema from PB to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a PB object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@PublicEvolving
public class PbRowDataDeserializationSchema implements DeserializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	/** Type information describing the result type. */
	private final TypeInformation<RowData> resultTypeInfo;
	private final String pbDescriptorClass;
	private final ProtoFile protoFile;
	private final int skipBytes;
	private final boolean withWrapper;
	private final boolean isAdInstanceFormat;
	private final RowType pbTypeInfo;
	private final boolean runtimeCutPb;
	private final boolean discardUnknownField;

	private transient DeserializationRuntimeConverter runtimeConverter;
	private transient Descriptors.Descriptor pbDescriptor;
	private transient DynamicMessage.Builder reusedBuilder;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	private PbRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			String pbDescriptorClass,
			ProtoFile protoFile,
			int skipBytes,
			boolean withWrapper,
			boolean isAdInstanceFormat,
			boolean ignoreParseErrors,
			boolean runtimeCutPb,
			boolean discardUnknownField) {

		this.resultTypeInfo = resultTypeInfo;
		this.pbDescriptorClass = pbDescriptorClass;
		this.protoFile = protoFile;
		this.skipBytes = skipBytes;
		this.withWrapper = withWrapper;
		this.isAdInstanceFormat = isAdInstanceFormat;
		this.ignoreParseErrors = ignoreParseErrors;
		if (this.withWrapper) {
			int index = rowType.getFieldIndex(PbConstant.FORMAT_PB_WRAPPER_NAME);
			pbTypeInfo = (RowType) rowType.getTypeAt(index);
		} else {
			pbTypeInfo = rowType;
		}
		this.runtimeCutPb = runtimeCutPb;
		this.discardUnknownField = discardUnknownField;
	}

	@Override
	public void open(InitializationContext context) {
		pbDescriptor = PbUtils.validateAndGetDescriptor(pbDescriptorClass, protoFile);
		if (runtimeCutPb) {
			try {
				pbDescriptor = ProtoCutUtil.cutPbDescriptor(pbDescriptor, pbTypeInfo);
			} catch (Exception e) {
				throw new FlinkRuntimeException("cut pb descriptor failed", e);
			}
		}
		reusedBuilder = DynamicMessage.newBuilder(pbDescriptor);
		runtimeConverter = DeserializationRuntimeConverterFactory.createConverter(pbTypeInfo, pbDescriptor);
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			if (isAdInstanceFormat) {
				//ad instance format: {sort_id_size(8 byte)} {sort_id} {data_size(8 byte)} {data}
				byte[] sortIdSizeBytes = Arrays.copyOfRange(message, 0, 8);
				int sortIdSize = (int) bytesToLong(sortIdSizeBytes);
				int totalHeaderSize = 8 + sortIdSize + 8;
				if (message.length <= totalHeaderSize) {
					throw new FlinkRuntimeException(String.format("The message length is %s, " +
						"which is less than or equal to totalHeaderSize %s", message.length, totalHeaderSize));
				}
				message = Arrays.copyOfRange(message, totalHeaderSize, message.length);
			} else if (skipBytes > 0) {
				if (message.length < skipBytes) {
					throw new FlinkRuntimeException(String.format("The message length is %s, " +
						"which is less than the skip bytes %s", message.length, skipBytes));
				}
				message = Arrays.copyOfRange(message, skipBytes, message.length);
			}

			final DynamicMessage dynamicMessage;
			if (discardUnknownField) {
				dynamicMessage = reusedBuilder.mergeFrom(CodedStreamHelper.discardUnknownFields(message)).build();
				reusedBuilder.clear();
			} else {
				dynamicMessage = DynamicMessage.parseFrom(pbDescriptor, message);
			}
			GenericRowData pbRow = (GenericRowData) runtimeConverter.convert(dynamicMessage);
			if (withWrapper) {
				return GenericRowData.of(pbRow);
			}
			return pbRow;
		} catch (Throwable t) {
			if (ignoreParseErrors) {
				return null;
			}
			throw new IOException("Failed to deserialize PB object.", t);
		}
	}

	@VisibleForTesting
	public Descriptors.Descriptor getPbDescriptor() {
		return pbDescriptor;
	}

	/**
	 * The first byte is the least significant byte.
	 * For example, new byte[] {4, 0, 0, 0, 0, 0, 0, 0} => 4L.
	 * */
	private static long bytesToLong(byte[] bytes) {
		if (bytes.length > 8) {
			throw new FlinkRuntimeException("byte[] size cannot be greater than 8.");
		}
		long value = 0;
		for (int i = 0; i < bytes.length; i++) {
			value += ((long) bytes[i] & 0xffL) << (8 * i);
		}
		return value;
	}

	@Override
	public boolean isEndOfStream(RowData rowData) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link PbRowDataDeserializationSchema}.
	 */
	public static class Builder {
		private String pbDescriptorClass;
		private ProtoFile protoFile;
		private RowType rowType;
		private TypeInformation<RowData> resultTypeInfo;
		private int skipBytes = 0;
		private boolean withWrapper = false;
		private boolean isAdInstanceFormat = false;
		private boolean ignoreParseErrors = false;
		private boolean runtimeCutPb = false;
		private boolean discardKnownFields = true;

		private Builder() {
		}

		public Builder setPbDescriptorClass(String pbDescriptorClass) {
			this.pbDescriptorClass = pbDescriptorClass;
			return this;
		}

		public Builder setProtoFile(ProtoFile protoFile) {
			this.protoFile = protoFile;
			return this;
		}

		public Builder setRowType(RowType rowType) {
			this.rowType = rowType;
			return this;
		}

		public Builder setResultTypeInfo(TypeInformation<RowData> resultTypeInfo) {
			this.resultTypeInfo = resultTypeInfo;
			return this;
		}

		public Builder setSkipBytes(int skipBytes) {
			this.skipBytes = skipBytes;
			return this;
		}

		public Builder setWithWrapper(boolean withWrapper) {
			this.withWrapper = withWrapper;
			return this;
		}

		public Builder setAdInstanceFormat(boolean isAdInstanceFormat) {
			this.isAdInstanceFormat = isAdInstanceFormat;
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public Builder setRuntimeCutPb(boolean runtimeCutPb) {
			this.runtimeCutPb = runtimeCutPb;
			return this;
		}

		public boolean isDiscardKnownFields() {
			return discardKnownFields;
		}

		public Builder setDiscardKnownFields(boolean discardKnownFields) {
			this.discardKnownFields = discardKnownFields;
			return this;
		}

		public PbRowDataDeserializationSchema build() {
			checkState(pbDescriptorClass != null || protoFile != null,
				"'pbDescriptorClass' and 'protoFile' can not be null at the same time.");
			checkNotNull(resultTypeInfo, "resultTypeInfo cannot be null!");
			checkNotNull(rowType, "rowType cannot be null!");

			return new PbRowDataDeserializationSchema(
				this.rowType,
				this.resultTypeInfo,
				this.pbDescriptorClass,
				this.protoFile,
				this.skipBytes,
				this.withWrapper,
				this.isAdInstanceFormat,
				this.ignoreParseErrors,
				this.runtimeCutPb,
				this.discardKnownFields);
		}
	}
}
