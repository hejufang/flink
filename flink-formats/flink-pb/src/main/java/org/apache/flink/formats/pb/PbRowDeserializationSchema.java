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

	private final String pbDescriptorClass;

	private final DeserializationRuntimeConverter runtimeConverter;

	private Descriptors.Descriptor pbDescriptor = null;

	private PbRowDeserializationSchema(TypeInformation<Row> typeInfo, String pbDescriptorClass) {
		checkNotNull(typeInfo, "Type information");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.pbDescriptorClass = pbDescriptorClass;
		this.runtimeConverter = DeserializationRuntimeConverterFactory.createConverter(this.typeInfo);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		// lazy initial pbDescriptor
		if (pbDescriptor == null) {
			pbDescriptor = PbValidator.validateAndReturnDescriptor(pbDescriptorClass);
		}
		try {
			DynamicMessage dynamicMessage = DynamicMessage.parseFrom(pbDescriptor, message);
			return (Row) runtimeConverter.convert(dynamicMessage, pbDescriptor.getFields());
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

		public PbRowDeserializationSchema build() {
			return new PbRowDeserializationSchema(this.typeInfo, this.pbDescriptorClass);
		}
	}
}
