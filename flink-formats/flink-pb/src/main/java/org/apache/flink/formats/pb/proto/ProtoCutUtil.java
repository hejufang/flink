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

package org.apache.flink.formats.pb.proto;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * util for cutting pb descriptor in runtime by runtime rowType, in order to parse field on demand.
 */
public class ProtoCutUtil {

	private static final Map<DescriptorProtos.FieldDescriptorProto.Type, String> typeAndTypeStringMap;
	private static final Map<DescriptorProtos.FieldDescriptorProto.Label, String> labelAndLabelStringMap;

	static {
		ImmutableMap.Builder<DescriptorProtos.FieldDescriptorProto.Type, String> typeAndTypeStringMapBuilder = ImmutableMap.builder();
		MessageDefinition.getsTypeMap().forEach((k, v) -> typeAndTypeStringMapBuilder.put(v, k));
		typeAndTypeStringMap = typeAndTypeStringMapBuilder.build();
		ImmutableMap.Builder<DescriptorProtos.FieldDescriptorProto.Label, String> labelAndLabelStringMapBuilder = ImmutableMap.builder();
		MessageDefinition.getSLabelMap().forEach((k, v) -> labelAndLabelStringMapBuilder.put(v, k));
		labelAndLabelStringMap = labelAndLabelStringMapBuilder.build();
	}

	public static Descriptors.Descriptor cutPbDescriptor(
			Descriptors.Descriptor descriptor,
			RowType runtimeRowType) {
		final IdentityHashMap<Descriptors.GenericDescriptor, Set<String>> map = new IdentityHashMap();
		descriptor.getFile().getMessageTypes().stream().forEach(dt -> map.put(dt, new HashSet<>()));
		ensureUsedFields(descriptor, runtimeRowType, map);
		return generateCutPbDescriptor(descriptor, map);
	}

	private static Descriptors.Descriptor generateCutPbDescriptor(
			Descriptors.Descriptor descriptor,
			Map<Descriptors.GenericDescriptor, Set<String>> descriptorAndUsedFieldSet) {
		DynamicSchema.Builder builder = DynamicSchema.newBuilder();
		Descriptors.FileDescriptor descriptorFile = descriptor.getFile();
		String syntax = descriptorFile.getSyntax().name().toLowerCase();
		builder.setSyntax(syntax);
		builder.setPackage(descriptorFile.getPackage());
		builder.setJavaPackage(descriptorFile.getOptions().getJavaPackage());
		builder.setJavaOuterClassname(descriptorFile.getOptions().getJavaOuterClassname());
		builder.setJavaMultipleFiles(descriptorFile.getOptions().getJavaMultipleFiles());
		descriptorFile.getEnumTypes().forEach(enumer -> builder.addEnumDefinition(toDynamicEnum(enumer)));
		descriptorFile.getMessageTypes().forEach(msgType -> toDynamicMessage(msgType, descriptorAndUsedFieldSet).ifPresent(builder::addMessageDefinition));
		try {
			return builder.build().getMessageDescriptor(descriptor.getName());
		} catch (Descriptors.DescriptorValidationException e) {
			throw new IllegalStateException(e);
		}

	}

	private static Optional<MessageDefinition> toDynamicMessage(
			Descriptors.Descriptor descriptor,
			Map<Descriptors.GenericDescriptor, Set<String>> descriptorAndUsedFieldSet) {
		MessageDefinition.Builder message = MessageDefinition.newBuilder(descriptor.getName());

		for (Descriptors.EnumDescriptor enumType : descriptor.getEnumTypes()) {
			message.addEnumDefinition(toDynamicEnum(enumType));
		}

		for (Descriptors.Descriptor messageType : descriptor.getNestedTypes()) {
			toDynamicMessage(messageType, descriptorAndUsedFieldSet).ifPresent(message::addMessageDefinition);
		}

		Set<String> usedFields = descriptorAndUsedFieldSet.get(descriptor);
		if (usedFields == null || usedFields.isEmpty()) {
			return Optional.empty();
		}
		Set<String> added = new HashSet<>();
		for (Descriptors.OneofDescriptor oneofDescriptor : descriptor.getOneofs()) {
			List<Descriptors.FieldDescriptor> fields = oneofDescriptor.getFields();
			if (fields.stream().noneMatch(f -> usedFields.contains(f.getName()))) {
				continue;
			}
			MessageDefinition.OneofBuilder oneofBuilder = message.addOneof(oneofDescriptor.getName());
			for (Descriptors.FieldDescriptor field : fields) {
				if (!usedFields.contains(field.getName())) {
					continue;
				}
				String defaultValue = String.valueOf(field.getDefaultValue());
				String jsonName = field.getJsonName();
				oneofBuilder.addField(
					getDynamicTypeString(field),
					field.getName(),
					field.getNumber(),
					defaultValue,
					jsonName
				);
				added.add(field.getName());
			}
		}

		for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
			if (!usedFields.contains(field.getName())) {
				continue;
			}
			if (added.contains(field.getName())) {
				continue;
			}

			final String label = getDynamicLabelString(field);
			final String fieldType = getDynamicTypeString(field);
			final String defaultValue;
			if (field.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
				defaultValue = null;
			} else if (field.toProto().getLabel().equals(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)) {
				defaultValue = null;
			} else {
				defaultValue = String.valueOf(field.getDefaultValue());
			}
			final String jsonName = field.getJsonName();
			final boolean isPacked = field.isPacked();
			message.addField(
				label,
				fieldType,
				field.getName(),
				field.getNumber(),
				defaultValue,
				jsonName,
				isPacked
			);

		}

		//handle reserved fields.
		DescriptorProtos.DescriptorProto proto = descriptor.toProto();
		proto.getReservedNameList().forEach(name -> message.addReservedName(name));
		proto.getReservedRangeList().forEach(range -> message.addReservedRange(range.getStart(), range.getEnd()));

		//set map entry.
		message.setMapEntry(descriptor.getOptions().getMapEntry());

		return Optional.of(message.build());
	}

	private static EnumDefinition toDynamicEnum(Descriptors.EnumDescriptor enumDescriptor) {
		boolean allowAlias = enumDescriptor.getOptions().getAllowAlias();
		EnumDefinition.Builder enumer = EnumDefinition.newBuilder(enumDescriptor.getName(), allowAlias);

		for (Descriptors.EnumValueDescriptor value : enumDescriptor.getValues()) {
			enumer.addValue(value.getName(), value.getNumber());
		}
		return enumer.build();
	}

	private static void ensureUsedFields(
			Descriptors.GenericDescriptor descriptor,
			LogicalType runtimeLogicalType,
			Map<Descriptors.GenericDescriptor, Set<String>> descriptorAndUsedFieldSet) {

		switch (runtimeLogicalType.getTypeRoot()) {
			case VARCHAR:
			case BIGINT:
			case BOOLEAN:
			case INTEGER:
			case DOUBLE:
			case FLOAT:
			case VARBINARY:
				break;
			case ARRAY:
				ensureUsedFieldsOnArrayType(
					(Descriptors.FieldDescriptor) descriptor,
					(ArrayType) runtimeLogicalType,
					descriptorAndUsedFieldSet);
				break;
			case MAP:
				ensureUsedFieldsOnMapType(
					(Descriptors.FieldDescriptor) descriptor,
					(MapType) runtimeLogicalType,
					descriptorAndUsedFieldSet);
				break;
			case ROW:
				Descriptors.Descriptor currDescriptor;
				if (descriptor instanceof Descriptors.FieldDescriptor) {
					currDescriptor = ((Descriptors.FieldDescriptor) descriptor).getMessageType();
				} else {
					currDescriptor = (Descriptors.Descriptor) descriptor;
				}
				ensureUsedFieldsOnRowType(
					currDescriptor,
					(RowType) runtimeLogicalType,
					descriptorAndUsedFieldSet);
				break;
			default:
				throw new UnsupportedOperationException("Unsupported type: " + runtimeLogicalType);
		}
	}

	private static void ensureUsedFieldsOnArrayType(
			Descriptors.FieldDescriptor fieldDescriptor,
			ArrayType runtimeLogicalType,
			Map<Descriptors.GenericDescriptor, Set<String>> descriptorAndUsedFieldSet) {

		if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
			Descriptors.Descriptor elementMessageType = fieldDescriptor.getMessageType();
			LogicalType elementType = runtimeLogicalType.getElementType();
			ensureUsedFields(elementMessageType, elementType, descriptorAndUsedFieldSet);
		}
	}

	private static void ensureUsedFieldsOnMapType(
			Descriptors.FieldDescriptor fieldDescriptor,
			MapType runtimeLogicalType,
			Map<Descriptors.GenericDescriptor, Set<String>> descriptorAndUsedFieldSet) {
		Descriptors.Descriptor descriptor = fieldDescriptor.getMessageType();
		if (!descriptorAndUsedFieldSet.containsKey(descriptor)) {
			descriptorAndUsedFieldSet.put(descriptor, new HashSet<>());
		}
		descriptorAndUsedFieldSet.get(descriptor).addAll(
			descriptor
				.getFields()
				.stream()
				.map(Descriptors.FieldDescriptor::getName)
				.collect(Collectors.toList()));
		Descriptors.FieldDescriptor keyDescriptor = descriptor.getFields().get(0);
		Descriptors.FieldDescriptor valueDescriptor = descriptor.getFields().get(1);
		ensureUsedFields(keyDescriptor, runtimeLogicalType.getKeyType(), descriptorAndUsedFieldSet);
		ensureUsedFields(valueDescriptor, runtimeLogicalType.getValueType(), descriptorAndUsedFieldSet);
	}

	private static void ensureUsedFieldsOnRowType(
			Descriptors.Descriptor descriptor,
			RowType runtimeLogicalType,
			Map<Descriptors.GenericDescriptor, Set<String>> descriptorAndUsedFieldSet) {

		Set<String> usedFieldNameSet = new HashSet(runtimeLogicalType.getFieldNames());
		if (!descriptorAndUsedFieldSet.containsKey(descriptor)) {
			descriptorAndUsedFieldSet.put(descriptor, new HashSet<>());
		}
		descriptorAndUsedFieldSet.get(descriptor).addAll(usedFieldNameSet);

		final List<Descriptors.FieldDescriptor> totalFieldDescriptors = new ArrayList<>();
		totalFieldDescriptors.addAll(descriptor.getFields());
		descriptor.getOneofs().forEach(oneOf -> totalFieldDescriptors.addAll(oneOf.getFields()));

		for (Descriptors.FieldDescriptor currFieldDescriptor : totalFieldDescriptors) {
			String fieldName = currFieldDescriptor.getName();
			if (usedFieldNameSet.contains(fieldName)) {
				LogicalType type = runtimeLogicalType.getTypeAt(runtimeLogicalType.getFieldIndex(fieldName));
				ensureUsedFields(currFieldDescriptor, type, descriptorAndUsedFieldSet);
			}
		}
	}

	private static String getDynamicTypeString(Descriptors.FieldDescriptor field) {
		DescriptorProtos.FieldDescriptorProto.Type fieldType = field.getType().toProto();
		if (fieldType.equals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)) {
			return field.getMessageType().getName();
		} else if (fieldType.equals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)) {
			return field.getEnumType().getName();
		} else if (field.equals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_GROUP)) {
			//todo support group.
			throw new IllegalArgumentException("currently we do not support [group] type in cutting pb.");
		} else {
			return typeAndTypeStringMap.get(fieldType);
		}
	}

	private static String getDynamicLabelString(Descriptors.FieldDescriptor field) {
		DescriptorProtos.FieldDescriptorProto.Label label = field.toProto().getLabel();
		return labelAndLabelStringMap.get(label);
	}

}
