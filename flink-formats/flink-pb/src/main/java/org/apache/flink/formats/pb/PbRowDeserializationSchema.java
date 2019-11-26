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

	/** Type information describing the result type. */
	private final RowTypeInfo typeInfo;

	/** Type information describing the pb class. When there is no wrapper, it is the same as typeInfo. */
	private final RowTypeInfo pbTypeInfo;

	private final int columnCount;

	private final String pbDescriptorClass;

	private final DeserializationRuntimeConverter runtimeConverter;

	private Descriptors.Descriptor pbDescriptor = null;

	private final int skipBytes;

	private final boolean withWrapper;

	private PbRowDeserializationSchema(TypeInformation<Row> typeInfo, String pbDescriptorClass,
		int skipBytes) {
		this(typeInfo, pbDescriptorClass, skipBytes, false);
	}

	private PbRowDeserializationSchema(TypeInformation<Row> typeInfo, String pbDescriptorClass,
		int skipBytes, boolean withWrapper) {
		checkNotNull(typeInfo, "Type information");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.pbDescriptorClass = pbDescriptorClass;
		this.skipBytes = skipBytes;
		this.withWrapper = withWrapper;
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
		if (skipBytes > 0) {
			if (message.length < skipBytes) {
				throw new RuntimeException(String.format("The message length is %s, " +
					"which is less than the skip bytes %s", message.length, skipBytes));
			} else {
				message = Arrays.copyOfRange(message, skipBytes, message.length);
			}
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
			throw new IOException("Failed to deserialize PB object.", t);
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
		final PbRowDeserializationSchema that = (PbRowDeserializationSchema) o;
		return Objects.equals(typeInfo, that.typeInfo) &&
			Objects.equals(skipBytes, that.skipBytes) &&
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

		public PbRowDeserializationSchema build() {
			return new PbRowDeserializationSchema(this.typeInfo, this.pbDescriptorClass,
				this.skipBytes, this.withWrapper);
		}
	}
}
