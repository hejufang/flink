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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.descriptors.PbValidator;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from PB to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a PB object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@PublicEvolving
public class PbRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = -4040917522067315718L;

	/**
	 * Type information describing the result type.
	 */
	private final RowTypeInfo typeInfo;

	/**
	 * Type information describing the pb class. When there is no wrapper, it is the same as typeInfo.
	 */
	private final RowTypeInfo pbTypeInfo;

	private final int columnCount;

	private final String pbDescriptorClass;

	private final DeserializationRuntimeConverter runtimeConverter;
	private final int skipBytes;
	private final boolean withWrapper;
	private Descriptors.Descriptor pbDescriptor = null;
	private boolean isAdInstanceFormat = false;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	private PbRowDeserializationSchema(TypeInformation<Row> typeInfo, String pbDescriptorClass,
		int skipBytes) {
		this(typeInfo, pbDescriptorClass, skipBytes, false, false, false);
	}

	private PbRowDeserializationSchema(TypeInformation<Row> typeInfo, String pbDescriptorClass,
		int skipBytes, boolean withWrapper, boolean isAdInstanceFormat, boolean ignoreParseErrors) {
		checkNotNull(typeInfo, "Type information");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.pbDescriptorClass = pbDescriptorClass;
		this.skipBytes = skipBytes;
		this.withWrapper = withWrapper;
		this.isAdInstanceFormat = isAdInstanceFormat;
		this.ignoreParseErrors = ignoreParseErrors;
		if (this.withWrapper) {
			TypeInformation ti = this.typeInfo.getTypeAt(PbConstant.FORMAT_PB_WRAPPER_NAME);
			pbTypeInfo = (RowTypeInfo) ti;
		} else {
			pbTypeInfo = this.typeInfo;
		}
		columnCount = this.typeInfo.getFieldTypes().length;
		this.runtimeConverter = DeserializationRuntimeConverterFactory.createConverter(this.pbTypeInfo);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		if (isAdInstanceFormat) {
			//ad instance format: {sort_id_size(8 byte)} {sort_id} {data_size(8 byte)} {data}
			byte[] sortIdSizeBytes = Arrays.copyOfRange(message, 0, 8);
			int sortIdSize = (int) bytesToLong(sortIdSizeBytes);
			int totalHeaderSize = 8 + sortIdSize + 8;
			if (message.length <= totalHeaderSize) {
				throw new FlinkRuntimeException(String.format("The message length is %s, " +
						"which is less than or equal to totalHeaderSize %s",
					message.length, totalHeaderSize));
			}
			message = Arrays.copyOfRange(message, totalHeaderSize, message.length);
		} else if (skipBytes > 0) {
			if (message.length < skipBytes) {
				throw new FlinkRuntimeException(String.format("The message length is %s, " +
					"which is less than the skip bytes %s", message.length, skipBytes));
			}
			message = Arrays.copyOfRange(message, skipBytes, message.length);
		}

		// lazy initial pbDescriptor
		if (pbDescriptor == null) {
			pbDescriptor = PbValidator.validateAndReturnDescriptor(pbDescriptorClass);
		}
		try {
			DynamicMessage dynamicMessage = DynamicMessage.parseFrom(pbDescriptor, message);
			Row pbRow = (Row) runtimeConverter.convert(dynamicMessage, pbDescriptor.getFields());
			if (withWrapper) {
				Row newRow = new Row(columnCount);
				newRow.setField(0, pbRow);
				for (int i = 1; i < columnCount; i++) {
					// We assume that all computed columns are time columns.
					newRow.setField(i, new Timestamp(System.currentTimeMillis()));
				}
				return newRow;
			}
			return pbRow;
		} catch (Throwable t) {
			if (ignoreParseErrors) {
				return null;
			}
			throw new IOException("Failed to deserialize PB object.", t);
		}
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
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PbRowDeserializationSchema that = (PbRowDeserializationSchema) o;
		return columnCount == that.columnCount &&
			skipBytes == that.skipBytes &&
			withWrapper == that.withWrapper &&
			isAdInstanceFormat == that.isAdInstanceFormat &&
			ignoreParseErrors == that.ignoreParseErrors &&
			Objects.equals(typeInfo, that.typeInfo) &&
			Objects.equals(pbTypeInfo, that.pbTypeInfo) &&
			Objects.equals(pbDescriptorClass, that.pbDescriptorClass) &&
			Objects.equals(runtimeConverter, that.runtimeConverter) &&
			Objects.equals(pbDescriptor, that.pbDescriptor);
	}

	@Override
	public int hashCode() {
		return Objects.hash(typeInfo, pbDescriptor);
	}

	/**
	 * Builder for {@link PbRowDeserializationSchema}.
	 */
	public static class Builder {
		private TypeInformation<Row> typeInfo;
		private String pbDescriptorClass;
		private int skipBytes;
		private boolean withWrapper;
		private boolean isAdInstanceFormat = false;
		private boolean ignoreParseErrors = false;

		public static Builder newBuilder() {
			return new Builder();
		}

		public Builder setTypeInfo(TypeInformation<Row> typeInfo) {
			this.typeInfo = typeInfo;
			return this;
		}

		public Builder setPbDescriptorClass(String pbDescriptorClass) {
			this.pbDescriptorClass = pbDescriptorClass;
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

		public PbRowDeserializationSchema build() {
			return new PbRowDeserializationSchema(this.typeInfo, this.pbDescriptorClass,
				this.skipBytes, this.withWrapper, this.isAdInstanceFormat, this.ignoreParseErrors);
		}
	}
}
