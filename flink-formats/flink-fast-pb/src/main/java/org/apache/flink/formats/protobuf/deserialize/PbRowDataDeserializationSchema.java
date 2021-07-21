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

package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.formats.protobuf.PbSchemaValidator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from Protobuf to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a protobuf object and reads the specified
 * fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
public class PbRowDataDeserializationSchema implements DeserializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	private final RowType rowType;
	private final TypeInformation<RowData> resultTypeInfo;

	private final PbFormatConfig pbFormatConfig;

	private transient ProtoToRowConverter protoToRowConverter;

	public PbRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			PbFormatConfig pbFormatConfig) {
		checkNotNull(rowType, "rowType cannot be null");
		this.resultTypeInfo = resultTypeInfo;
		this.pbFormatConfig = pbFormatConfig;
		// do it in client side to report error in the first place
		new PbSchemaValidator(
			PbFormatUtils.getDescriptor(pbFormatConfig.getPbDescriptorClass()), rowType)
			.validate();
		if (pbFormatConfig.isWithWrapper()) {
			int index = rowType.getFieldIndex(PbConstant.FORMAT_PB_WRAPPER_NAME);
			this.rowType = (RowType) rowType.getTypeAt(index);
		} else {
			this.rowType = rowType;
		}
		// this step is only used to validate codegen in client side in the first place
		try {
			// validate converter in client side to early detect errors
			protoToRowConverter = new ProtoToRowConverter(rowType, pbFormatConfig);
		} catch (PbCodegenException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		protoToRowConverter = new ProtoToRowConverter(rowType, pbFormatConfig);
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			if (pbFormatConfig.isAdInstanceFormat()) {
				//ad instance format: {sort_id_size(8 byte)} {sort_id} {data_size(8 byte)} {data}
				byte[] sortIdSizeBytes = Arrays.copyOfRange(message, 0, 8);
				int sortIdSize = (int) bytesToLong(sortIdSizeBytes);
				int totalHeaderSize = 8 + sortIdSize + 8;
				if (message.length <= totalHeaderSize) {
					throw new FlinkRuntimeException(String.format("The message length is %s, " +
						"which is less than or equal to totalHeaderSize %s", message.length, totalHeaderSize));
				}
				message = Arrays.copyOfRange(message, totalHeaderSize, message.length);
			} else if (pbFormatConfig.getSkipBytes() > 0) {
				int skipBytes = pbFormatConfig.getSkipBytes();
				if (message.length < pbFormatConfig.getSkipBytes()) {
					throw new FlinkRuntimeException(String.format("The message length is %s, " +
						"which is less than the skip bytes %s", message.length, skipBytes));
				}
				message = Arrays.copyOfRange(message, skipBytes, message.length);
			}
			GenericRowData pbRow = (GenericRowData) protoToRowConverter.convertProtoBinaryToRow(message);
			if (pbFormatConfig.isWithWrapper()) {
				return GenericRowData.of(pbRow);
			}
			return pbRow;
		} catch (Throwable t) {
			if (pbFormatConfig.isIgnoreParseErrors()) {
				return null;
			}
			throw new IOException("Failed to deserialize PB object.", t);
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return this.resultTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PbRowDataDeserializationSchema that = (PbRowDataDeserializationSchema) o;
		return Objects.equals(rowType, that.rowType)
			&& Objects.equals(resultTypeInfo, that.resultTypeInfo)
			&& Objects.equals(pbFormatConfig, that.pbFormatConfig);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowType, resultTypeInfo, pbFormatConfig);
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
}
