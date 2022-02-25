/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.pb.proto;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;

import java.util.Map;

/**
 * This class is migrated from io.confluent.kafka.schemaregistry.protobuf.dynamic#MessageDefinition, which
 * is in io.confluent:kafka-protobuf-provider.
 */
public class MessageDefinition {

	public static Builder newBuilder(String msgTypeName) {
		return new Builder(msgTypeName);
	}

	public String toString() {
		return mMsgType.toString();
	}

	DescriptorProto getMessageType() {
		return mMsgType;
	}

	private MessageDefinition(DescriptorProto msgType) {
		mMsgType = msgType;
	}

	private DescriptorProto mMsgType;

	/**
	 * Builder for {@link MessageDefinition}.
	 */
	public static class Builder {
		public Builder addField(String label, String type, String name, int num, String defaultVal) {
			return addField(label, type, name, num, defaultVal, null, null);
		}

		public Builder addField(
			String label,
			String type,
			String name,
			int num,
			String defaultVal,
			String jsonName,
			Boolean isPacked
		) {
			FieldDescriptorProto.Label protoLabel = sLabelMap.get(label);
			doAddField(protoLabel, type, name, num, defaultVal, jsonName, isPacked, null);
			return this;
		}

		public OneofBuilder addOneof(String oneofName) {
			mMsgTypeBuilder.addOneofDecl(OneofDescriptorProto.newBuilder().setName(oneofName).build());
			return new OneofBuilder(this, mOneofIndex++);
		}

		public Builder addMessageDefinition(MessageDefinition msgDef) {
			mMsgTypeBuilder.addNestedType(msgDef.getMessageType());
			return this;
		}

		public Builder addEnumDefinition(EnumDefinition enumDef) {
			mMsgTypeBuilder.addEnumType(enumDef.getEnumType());
			return this;
		}

		// Note: added
		public Builder addReservedName(String reservedName) {
			mMsgTypeBuilder.addReservedName(reservedName);
			return this;
		}

		// Note: added
		public Builder addReservedRange(int start, int end) {
			DescriptorProto.ReservedRange.Builder rangeBuilder =
				DescriptorProto.ReservedRange.newBuilder();
			rangeBuilder.setStart(start).setEnd(end);
			mMsgTypeBuilder.addReservedRange(rangeBuilder.build());
			return this;
		}

		// Note: added
		public Builder setMapEntry(boolean mapEntry) {
			DescriptorProtos.MessageOptions.Builder optionsBuilder =
				DescriptorProtos.MessageOptions.newBuilder();
			optionsBuilder.setMapEntry(mapEntry);
			mMsgTypeBuilder.mergeOptions(optionsBuilder.build());
			return this;
		}

		public MessageDefinition build() {
			return new MessageDefinition(mMsgTypeBuilder.build());
		}

		private Builder(String msgTypeName) {
			mMsgTypeBuilder = DescriptorProto.newBuilder();
			mMsgTypeBuilder.setName(msgTypeName);
		}

		private void doAddField(
			FieldDescriptorProto.Label label,
			String type,
			String name,
			int num,
			String defaultVal,
			String jsonName,
			Boolean isPacked,
			OneofBuilder oneofBuilder
		) {
			FieldDescriptorProto.Builder fieldBuilder = FieldDescriptorProto.newBuilder();
			// Note: changed
			if (label != null) {
				fieldBuilder.setLabel(label);
			}
			FieldDescriptorProto.Type primType = sTypeMap.get(type);
			if (primType != null) {
				fieldBuilder.setType(primType);
			} else {
				fieldBuilder.setTypeName(type);
			}
			fieldBuilder.setName(name).setNumber(num);
			if (defaultVal != null) {
				fieldBuilder.setDefaultValue(defaultVal);
			}
			if (oneofBuilder != null) {
				fieldBuilder.setOneofIndex(oneofBuilder.getIdx());
			}
			if (jsonName != null) {
				fieldBuilder.setJsonName(jsonName);
			}
			if (isPacked != null) {
				DescriptorProtos.FieldOptions.Builder optionsBuilder =
					DescriptorProtos.FieldOptions.newBuilder();
				optionsBuilder.setPacked(isPacked);
				fieldBuilder.mergeOptions(optionsBuilder.build());
			}
			mMsgTypeBuilder.addField(fieldBuilder.build());
		}

		private DescriptorProto.Builder mMsgTypeBuilder;
		private int mOneofIndex = 0;
	}

	/**
	 * MessageDefinition.OneofBuilder.
	 */
	public static class OneofBuilder {
		public OneofBuilder addField(String type, String name, int num, String defaultVal) {
			return addField(type, name, num, defaultVal, null);
		}

		public OneofBuilder addField(
			String type,
			String name,
			int num,
			String defaultVal,
			String jsonName
		) {
			mMsgBuilder.doAddField(
				FieldDescriptorProto.Label.LABEL_OPTIONAL,
				type,
				name,
				num,
				defaultVal,
				jsonName,
				null,
				this
			);
			return this;
		}

		public MessageDefinition.Builder msgDefBuilder() {
			return mMsgBuilder;
		}

		public int getIdx() {
			return mIdx;
		}

		private OneofBuilder(MessageDefinition.Builder msgBuilder, int oneofIdx) {
			mMsgBuilder = msgBuilder;
			mIdx = oneofIdx;
		}

		private MessageDefinition.Builder mMsgBuilder;
		private int mIdx;
	}

	//Note: modified, add final modifier.
	private static final Map<String, FieldDescriptorProto.Type> sTypeMap;
	//Note: modified, add final modifier.
	private static final Map<String, FieldDescriptorProto.Label> sLabelMap;

	static Map<String, FieldDescriptorProto.Type> getsTypeMap() {
		return sTypeMap;
	}

	static Map<String, FieldDescriptorProto.Label> getSLabelMap() {
		return sLabelMap;
	}

	static {
		//Note: modified, use immutable map.
		ImmutableMap.Builder<String, FieldDescriptorProto.Type> sTypeMapBuilder = ImmutableMap.builder();
		sTypeMapBuilder.put("double", FieldDescriptorProto.Type.TYPE_DOUBLE);
		sTypeMapBuilder.put("float", FieldDescriptorProto.Type.TYPE_FLOAT);
		sTypeMapBuilder.put("int32", FieldDescriptorProto.Type.TYPE_INT32);
		sTypeMapBuilder.put("int64", FieldDescriptorProto.Type.TYPE_INT64);
		sTypeMapBuilder.put("uint32", FieldDescriptorProto.Type.TYPE_UINT32);
		sTypeMapBuilder.put("uint64", FieldDescriptorProto.Type.TYPE_UINT64);
		sTypeMapBuilder.put("sint32", FieldDescriptorProto.Type.TYPE_SINT32);
		sTypeMapBuilder.put("sint64", FieldDescriptorProto.Type.TYPE_SINT64);
		sTypeMapBuilder.put("fixed32", FieldDescriptorProto.Type.TYPE_FIXED32);
		sTypeMapBuilder.put("fixed64", FieldDescriptorProto.Type.TYPE_FIXED64);
		sTypeMapBuilder.put("sfixed32", FieldDescriptorProto.Type.TYPE_SFIXED32);
		sTypeMapBuilder.put("sfixed64", FieldDescriptorProto.Type.TYPE_SFIXED64);
		sTypeMapBuilder.put("bool", FieldDescriptorProto.Type.TYPE_BOOL);
		sTypeMapBuilder.put("string", FieldDescriptorProto.Type.TYPE_STRING);
		sTypeMapBuilder.put("bytes", FieldDescriptorProto.Type.TYPE_BYTES);
		//sTypeMap.put("enum", FieldDescriptorProto.Type.TYPE_ENUM);
		//sTypeMap.put("message", FieldDescriptorProto.Type.TYPE_MESSAGE);
		//sTypeMap.put("group", FieldDescriptorProto.Type.TYPE_GROUP);
		sTypeMap = sTypeMapBuilder.build();

		//Note: modified, use immutable map.
		ImmutableMap.Builder<String, FieldDescriptorProto.Label> sLabelMapBuilder = ImmutableMap.builder();
		sLabelMapBuilder.put("optional", FieldDescriptorProto.Label.LABEL_OPTIONAL);
		sLabelMapBuilder.put("required", FieldDescriptorProto.Label.LABEL_REQUIRED);
		sLabelMapBuilder.put("repeated", FieldDescriptorProto.Label.LABEL_REPEATED);
		sLabelMap = sLabelMapBuilder.build();
	}
}
