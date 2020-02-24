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
import org.apache.flink.types.Row;

import com.bytedance.dbus.DRCEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * Deserialize drc binlog data to row.
 */
public class PbBinlogDRCRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = -6289549847439544478L;

	private final TypeInformation<Row> headerTypeInfo;
	private final TypeInformation<Row> bodyTypeInfo;
	private final RowTypeInfo typeInfo;

	private final DeserializationRuntimeConverter headerRuntimeConverter;
	private final DeserializationRuntimeConverter bodyRuntimeConverter;

	/**
	 * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
	 */
	private final boolean ignoreParseErrors;

	private PbBinlogDRCRowDeserializationSchema(
		RowTypeInfo typeInfo,
		TypeInformation<Row> headerTypeInfo,
		TypeInformation<Row> bodyTypeInfo,
		boolean ignoreParseErrors) {
		this.typeInfo = typeInfo;
		this.headerTypeInfo = headerTypeInfo;
		this.headerRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(headerTypeInfo);
		this.bodyTypeInfo = bodyTypeInfo;
		this.bodyRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(bodyTypeInfo);
		this.ignoreParseErrors = ignoreParseErrors;
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			Descriptors.Descriptor binlogDescriptor = DRCEntry.Message.getDescriptor();
			DynamicMessage binlogMessage = DynamicMessage.parseFrom(binlogDescriptor, message);
			Descriptors.Descriptor entryDescriptor = DRCEntry.Entry.getDescriptor();
			Descriptors.FieldDescriptor payloadField =
				binlogDescriptor.findFieldByName(PbConstant.FORMAT_BINLOG_DRC_TYPE_PAYLOAD);
			DynamicMessage entryMessage = DynamicMessage.parseFrom(
				entryDescriptor, (ByteString) binlogMessage.getField(payloadField));

			Row row = new Row(typeInfo.getFieldNames().length);

			Descriptors.FieldDescriptor headerField =
				entryDescriptor.findFieldByName(PbConstant.FORMAT_BINLOG_DRC_TYPE_HEADER);
			Object header = headerRuntimeConverter.convert(
				entryMessage.getField(headerField),
				DRCEntry.EntryHeader.getDescriptor().getFields());
			row.setField(0, header);

			Descriptors.FieldDescriptor bodyField =
				entryDescriptor.findFieldByName(PbConstant.FORMAT_BINLOG_DRC_TYPE_BODY);
			Object body = bodyRuntimeConverter.convert(
				entryMessage.getField(bodyField),
				DRCEntry.EntryBody.getDescriptor().getFields());
			row.setField(1, body);

			// Assume all computed columns are Timestamp.
			// TODO: Remove computed columns definition from source.
			if (row.getArity() > 2) {
				for (int i = 2; i < row.getArity(); ++i) {
					row.setField(i, new Timestamp(System.currentTimeMillis()));
				}
			}

			return row;
		} catch (Throwable t) {
			if (ignoreParseErrors) {
				return null;
			}
			throw new IOException("Failed to deserialize PB Binlog object.", t);
		}
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	public static Builder builder(RowTypeInfo typeInfo) {
		return new Builder(typeInfo);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PbBinlogDRCRowDeserializationSchema that = (PbBinlogDRCRowDeserializationSchema) o;
		return ignoreParseErrors == that.ignoreParseErrors &&
			Objects.equals(headerTypeInfo, that.headerTypeInfo) &&
			Objects.equals(bodyTypeInfo, that.bodyTypeInfo) &&
			Objects.equals(typeInfo, that.typeInfo) &&
			Objects.equals(headerRuntimeConverter, that.headerRuntimeConverter) &&
			Objects.equals(bodyRuntimeConverter, that.bodyRuntimeConverter);
	}

	@Override
	public int hashCode() {
		return Objects.hash(headerTypeInfo, bodyTypeInfo, typeInfo, headerRuntimeConverter,
			bodyRuntimeConverter, ignoreParseErrors);
	}

	/**
	 * Builder for {@link PbBinlogDRCRowDeserializationSchema}.
	 */
	public static class Builder {
		private final RowTypeInfo typeInfo;
		private TypeInformation<Row> headerTypeInfo;
		private TypeInformation<Row> bodyTypeInfo;
		private boolean ignoreParseErrors;

		private Builder(RowTypeInfo typeInfo) {
			this.typeInfo = typeInfo;
		}

		public Builder setHeaderType(TypeInformation<Row> headerTypeInfo) {
			this.headerTypeInfo = headerTypeInfo;
			return this;
		}

		public Builder setBodyTypeInfo(TypeInformation<Row> bodyTypeInfo) {
			this.bodyTypeInfo = bodyTypeInfo;
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public PbBinlogDRCRowDeserializationSchema build() {
			return new PbBinlogDRCRowDeserializationSchema(
				this.typeInfo,
				this.headerTypeInfo,
				this.bodyTypeInfo,
				this.ignoreParseErrors);
		}
	}
}
