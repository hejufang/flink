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

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;

/**
 * Deserialize binlog data to row.
 */
public class PbBinlogRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = -6289549847439544478L;

	private final TypeInformation<Row> headerTypeInfo;
	private final TypeInformation entryTypeTypeInfo;
	private final TypeInformation<Row> transactionBeginTypeInfo;
	private final TypeInformation<Row> rowChangeTypeInfo;
	private final TypeInformation<Row> transactionEndTypeInfo;
	private final RowTypeInfo typeInfo;

	private final DeserializationRuntimeConverter headerRuntimeConverter;
	private final DeserializationRuntimeConverter entryTypeRuntimeConverter;
	private final DeserializationRuntimeConverter transactionBeginRuntimeConverter;
	private final DeserializationRuntimeConverter rowChangeRuntimeConverter;
	private final DeserializationRuntimeConverter transactionEndRuntimeConverter;

	private PbBinlogRowDeserializationSchema(
		RowTypeInfo typeInfo,
		TypeInformation<Row> headerTypeInfo,
		TypeInformation entryTypeTypeInfo,
		TypeInformation<Row> transactionBeginTypeInfo,
		TypeInformation<Row> rowChangeTypeInfo,
		TypeInformation<Row> transactionEndTypeInfo) {
		this.typeInfo = typeInfo;
		this.headerTypeInfo = headerTypeInfo;
		this.headerRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(headerTypeInfo);
		this.entryTypeTypeInfo = entryTypeTypeInfo;
		this.entryTypeRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(entryTypeTypeInfo);
		this.transactionBeginTypeInfo = transactionBeginTypeInfo;
		this.transactionBeginRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(transactionBeginTypeInfo);
		this.rowChangeTypeInfo = rowChangeTypeInfo;
		this.rowChangeRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(rowChangeTypeInfo);
		this.transactionEndTypeInfo = transactionEndTypeInfo;
		this.transactionEndRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(transactionEndTypeInfo);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			Descriptors.Descriptor binlogDescriptor = CanalEntry.Entry.getDescriptor();
			DynamicMessage dynamicMessage = DynamicMessage.parseFrom(binlogDescriptor, message);
			Row row = new Row(typeInfo.getFieldNames().length);

			Descriptors.FieldDescriptor headerField = binlogDescriptor.findFieldByName(PbConstant.FORMAT_BINLOG_TYPE_HEADER);
			Object header = headerRuntimeConverter.convert(
				dynamicMessage.getField(headerField),
				CanalEntry.Header.getDescriptor().getFields());
			row.setField(0, header);

			Descriptors.FieldDescriptor entryTypeField = binlogDescriptor.findFieldByName(PbConstant.FORMAT_BINLOG_TYPE_ENTRY_TYPE);
			Object entryType = entryTypeRuntimeConverter.convert(
				dynamicMessage.getField(entryTypeField),
				Arrays.asList(entryTypeField));
			row.setField(1, entryType);

			Descriptors.FieldDescriptor storeValueField = binlogDescriptor.findFieldByName(PbConstant.FORMAT_BINLOG_TYPE_STORE_VALUE);
			if (entryType.equals(CanalEntry.EntryType.TRANSACTIONBEGIN.toString())) {
				Object transactionBegin = transactionBeginRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.TransactionBegin.getDescriptor(),
						(ByteString) dynamicMessage.getField(storeValueField)),
					CanalEntry.TransactionBegin.getDescriptor().getFields());
				row.setField(2, transactionBegin);

				Object rowChange = rowChangeRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.RowChange.getDescriptor(),
						CanalEntry.RowChange.getDefaultInstance().toByteString()),
					CanalEntry.RowChange.getDescriptor().getFields());
				row.setField(3, rowChange);

				Object transactionEnd = transactionEndRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.TransactionEnd.getDescriptor(),
						CanalEntry.TransactionEnd.getDefaultInstance().toByteString()),
					CanalEntry.TransactionEnd.getDescriptor().getFields());
				row.setField(4, transactionEnd);

			} else if (entryType.equals(CanalEntry.EntryType.ROWDATA.toString())) {
				Object transactionBegin = transactionBeginRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.TransactionBegin.getDescriptor(),
						CanalEntry.TransactionBegin.getDefaultInstance().toByteString()),
					CanalEntry.TransactionBegin.getDescriptor().getFields());
				row.setField(2, transactionBegin);

				Object rowChange = rowChangeRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.RowChange.getDescriptor(),
						(ByteString) dynamicMessage.getField(storeValueField)),
					CanalEntry.RowChange.getDescriptor().getFields());
				row.setField(3, rowChange);

				Object transactionEnd = transactionEndRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.TransactionEnd.getDescriptor(),
						CanalEntry.TransactionEnd.getDefaultInstance().toByteString()),
					CanalEntry.TransactionEnd.getDescriptor().getFields());
				row.setField(4, transactionEnd);

			} else if (entryType.equals(CanalEntry.EntryType.TRANSACTIONEND.toString())) {
				Object transactionBegin = transactionBeginRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.TransactionBegin.getDescriptor(),
						CanalEntry.TransactionBegin.getDefaultInstance().toByteString()),
					CanalEntry.TransactionBegin.getDescriptor().getFields());
				row.setField(2, transactionBegin);

				Object rowChange = rowChangeRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.RowChange.getDescriptor(),
						CanalEntry.RowChange.getDefaultInstance().toByteString()),
					CanalEntry.RowChange.getDescriptor().getFields());
				row.setField(3, rowChange);

				Object transactionEnd = transactionEndRuntimeConverter.convert(
					DynamicMessage.parseFrom(
						CanalEntry.TransactionEnd.getDescriptor(),
						(ByteString) dynamicMessage.getField(storeValueField)),
					CanalEntry.TransactionEnd.getDescriptor().getFields());
				row.setField(4, transactionEnd);
			}

			// Assume all computed columns are Timestamp.
			// TODO: Remove computed columns definition from source.
			if (row.getArity() > 5) {
				for (int i = 5; i < row.getArity(); ++i) {
					row.setField(i, new Timestamp(System.currentTimeMillis()));
				}
			}

			return row;
		} catch (Throwable t) {
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

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final PbBinlogRowDeserializationSchema that = (PbBinlogRowDeserializationSchema) o;
		return Objects.equals(headerTypeInfo, that.headerTypeInfo) &&
			Objects.equals(entryTypeTypeInfo, that.entryTypeTypeInfo) &&
			Objects.equals(transactionBeginTypeInfo, that.transactionBeginTypeInfo) &&
			Objects.equals(rowChangeTypeInfo, that.rowChangeTypeInfo) &&
			Objects.equals(transactionEndTypeInfo, that.transactionEndTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(headerTypeInfo,
			entryTypeTypeInfo,
			transactionBeginTypeInfo,
			rowChangeTypeInfo,
			transactionEndTypeInfo);
	}

	/**
	 * Builder for {@link PbBinlogRowDeserializationSchema}.
	 */
	public static class Builder {
		private TypeInformation<Row> headerTypeInfo;
		private TypeInformation entryTypeTypeInfo;
		private TypeInformation<Row> transactionBeginTypeInfo;
		private TypeInformation<Row> rowChangeTypeInfo;
		private TypeInformation<Row> transactionEndTypeInfo;
		private final RowTypeInfo typeInfo;

		public static Builder newBuilder(RowTypeInfo typeInfo) {
			return new Builder(typeInfo);
		}

		public Builder(RowTypeInfo typeInfo) {
			this.typeInfo = typeInfo;
		}

		public Builder setHeaderType(TypeInformation<Row> headerTypeInfo) {
			this.headerTypeInfo = headerTypeInfo;
			return this;
		}

		public Builder setEntryTypeType(TypeInformation entryTypeType) {
			this.entryTypeTypeInfo = entryTypeType;
			return this;
		}

		public Builder setTransactionBeginTypeInfo(TypeInformation<Row> transactionBeginTypeInfo) {
			this.transactionBeginTypeInfo = transactionBeginTypeInfo;
			return this;
		}

		public Builder setRowChangeTypeInfo(TypeInformation<Row> rowChangeTypeInfo) {
			this.rowChangeTypeInfo = rowChangeTypeInfo;
			return this;
		}

		public Builder setTransactionEndTypeInfo(TypeInformation<Row> transactionEndTypeInfo) {
			this.transactionEndTypeInfo = transactionEndTypeInfo;
			return this;
		}

		public PbBinlogRowDeserializationSchema build() {
			return new PbBinlogRowDeserializationSchema(
				this.typeInfo,
				this.headerTypeInfo,
				this.entryTypeTypeInfo,
				this.transactionBeginTypeInfo,
				this.rowChangeTypeInfo,
				this.transactionEndTypeInfo);
		}
	}
}
