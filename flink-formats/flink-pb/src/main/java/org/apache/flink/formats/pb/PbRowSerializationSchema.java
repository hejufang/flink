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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.table.descriptors.PbValidator;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Serialization schema that serializes an object of Flink types into a protobuf bytes.
 *
 * <p>Serializes the input Flink object into a protobuf object and
 * converts it into <code>byte[]</code>.
 */
public class PbRowSerializationSchema implements SerializationSchema<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(PbRowSerializationSchema.class);

	/**
	 * Type information describing the input type.
	 */
	private final RowTypeInfo typeInfo;

	private final String pbDescriptorClass;

	private Descriptors.Descriptor pbDescriptor;

	/** Object mapper that is used to create output JSON objects. */
	private final ObjectMapper mapper = new ObjectMapper();

	/** Reusable object node. */
	private transient ObjectNode node;

	/** Reusable byte buffer. */
	private transient ByteBuffer byteBuffer;

	private final boolean sinkWithSizeHeader;

	private final boolean withWrapper;

	private final JsonRowSerializationSchema.SerializationRuntimeConverter jsonRuntimeConverter;

	public PbRowSerializationSchema(RowTypeInfo typeInfo, String pbDescriptorClass,
		boolean sinkWithSizeHeader) {
		this(typeInfo, pbDescriptorClass, sinkWithSizeHeader, false);
	}

	public PbRowSerializationSchema(RowTypeInfo typeInfo, String pbDescriptorClass,
		boolean sinkWithSizeHeader, boolean withWrapper) {
		this.typeInfo = typeInfo;
		this.pbDescriptorClass = pbDescriptorClass;
		this.jsonRuntimeConverter =
			new JsonRowSerializationSchema(this.typeInfo).createConverter(this.typeInfo);
		this.sinkWithSizeHeader = sinkWithSizeHeader;
		this.withWrapper = withWrapper;
	}

	@Override
	public byte[] serialize(Row element) {
		if (node == null) {
			node = mapper.createObjectNode();
		}
		if (pbDescriptor == null) {
			pbDescriptor = PbValidator.validateAndReturnDescriptor(pbDescriptorClass);
		}
		// 1. convert Row to  JsonNode.
		JsonNode jsonNode = jsonRuntimeConverter.convert(mapper, node, element);

		if (withWrapper) {
			jsonNode = jsonNode.get(PbConstant.FORMAT_PB_WRAPPER_NAME);
		}
		try {
			// 2. convert JsonNode to protobuf.
			Class<?> pbClass = Class.forName(pbDescriptorClass);
			Method buildMethod = pbClass.getMethod(PbConstant.PB_METHOD_NEW_BUILDER);
			Message.Builder builder = (Message.Builder) buildMethod.invoke(null);
			JsonFormat.parser().merge(jsonNode.toString(), builder);
			byte[] dataBytes = builder.build().toByteArray();
			if (sinkWithSizeHeader) {
				if (byteBuffer == null) {
					byteBuffer = ByteBuffer.allocate(Long.BYTES);
				}
				int size = dataBytes.length;
				byte[] sizeByte = byteBuffer.putLong(size).array();
				byte[] newBytes = new byte[dataBytes.length + sizeByte.length];
				System.arraycopy(sizeByte, 0, newBytes, 0, sizeByte.length);
				System.arraycopy(dataBytes, 0, newBytes, sizeByte.length, dataBytes.length);
				byteBuffer.clear();
				return newBytes;
			}
			return dataBytes;
		} catch (ClassNotFoundException e) {
			LOG.error("Failed to find class {}, please make sure this class is in class paths.",
				pbDescriptorClass, e);
			throw new RuntimeException(e);
		} catch (IllegalAccessException | NoSuchMethodException
			| InvocationTargetException | InvalidProtocolBufferException e) {
			LOG.error("Failed parse Row to protobuf. Row: {}", element, e);
			throw new RuntimeException(e);
		}
	}
}
