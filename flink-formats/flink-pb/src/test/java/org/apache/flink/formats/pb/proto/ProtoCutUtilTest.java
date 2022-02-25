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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.formats.pb.PbSchemaTestUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * test for {@link ProtoCutUtil}.
 */
public class ProtoCutUtilTest {

	private static RowType baseRowType;
	private static Descriptors.Descriptor baseDescriptor;

	@BeforeClass
	public static void setUp() throws Exception {
		baseDescriptor = ProtoFileUtils.parseDescriptorFromProtoFile(PbSchemaTestUtil.getProtoFile());
		baseRowType = (RowType) PbFormatUtils.createDataType(baseDescriptor, false).getLogicalType();
	}

	@Test
	public void testDoNotCutDescriptorWhenRowTypeContainsAllFields() throws Exception {
		Tuple2<Descriptors.Descriptor, Descriptors.Descriptor> descriptorDescriptors = testCutUnusedFieldsAccordingToRuntimeRowDataBase(baseRowType);
		Descriptors.Descriptor oldDescriptor = descriptorDescriptors.f0;
		Descriptors.Descriptor newDescriptor = descriptorDescriptors.f1;
		checkDescriptorEquality(oldDescriptor, newDescriptor);
	}

	@Test
	public void testCutUnusedFields1() throws Exception {
		RowType projectedRowType = projectRowType(baseRowType, ImmutableList.of(1, 3, 5));
		testCutUnusedFieldsAccordingToRuntimeRowDataBase(projectedRowType);
	}

	@Test
	public void testCutUnusedFields2() throws Exception {
		RowType projectedRowType = projectRowType(baseRowType, ImmutableList.of(2, 4, 6));
		testCutUnusedFieldsAccordingToRuntimeRowDataBase(projectedRowType);
	}

	@Test
	public void testCutUnusedOneofFields() throws Exception {
		// no oneof used, expect no oneofDescriptor.
		RowType projectedRowType1 = projectRowType(baseRowType, ImmutableList.of(6));
		Descriptors.Descriptor descriptor1 = testCutUnusedFieldsAccordingToRuntimeRowDataBase(projectedRowType1).f1;
		assertTrue(descriptor1.getOneofs().isEmpty());

		//all oneof field used,expect all fields in oneofDescriptor.
		RowType projectedRowType2 = projectRowType(baseRowType, ImmutableList.of(0, 1));
		Descriptors.Descriptor descriptor2 = testCutUnusedFieldsAccordingToRuntimeRowDataBase(projectedRowType2).f1;
		assertEquals(2, descriptor2.getOneofs().get(0).getFieldCount());

		//part of oneof field used, expect partial fields in oneOfDescriptor.
		RowType projectedRowType3 = projectRowType(baseRowType, ImmutableList.of(1));
		Descriptors.Descriptor descriptor3 = testCutUnusedFieldsAccordingToRuntimeRowDataBase(projectedRowType3).f1;
		assertEquals(1, descriptor3.getOneofs().get(0).getFieldCount());
	}

	@Test
	public void testKeepAllFieldsInsideOfMapType() throws Exception {
		RowType projectedRowType = projectRowType(baseRowType, ImmutableList.of(9));
		Descriptors.Descriptor descriptor = testCutUnusedFieldsAccordingToRuntimeRowDataBase(projectedRowType).f1;
		assertEquals(projectedRowType.getFieldCount(), descriptor.getFields().size());
		Descriptors.FieldDescriptor mapDescriptor = descriptor.getFields().get(0).getMessageType().getFields().get(1);
		assertTrue(mapDescriptor.isMapField());
		assertEquals(2, mapDescriptor.getMessageType().getFields().size());
	}

	@Test
	public void testCutNestedUnusedFields() throws Exception {
		String fieldName = "innerMessage";
		int fieldIndex = baseRowType.getFieldIndex(fieldName);
		RowType rowTypeInside = (RowType) baseRowType.getTypeAt(fieldIndex);
		RowType newRowTypeInside = projectRowType(rowTypeInside, ImmutableList.of(0));
		RowType projectedRowOutside = projectRowType(baseRowType, ImmutableList.of(1, 5, fieldIndex));
		ArrayList<RowType.RowField> rowFields = new ArrayList<>(projectedRowOutside.getFields());
		rowFields.set(rowFields.size() - 1, new RowType.RowField(fieldName, newRowTypeInside));
		String[] names = rowFields.stream().map(RowType.RowField::getName).toArray(String[]::new);
		LogicalType[] logicalTypes = rowFields.stream().map(RowType.RowField::getType).toArray(LogicalType[]::new);
		RowType newRowTypeOutside = RowType.of(logicalTypes, names);
		Descriptors.Descriptor newDescriptor = testCutUnusedFieldsAccordingToRuntimeRowDataBase(newRowTypeOutside).f1;
		Descriptors.Descriptor insideDescriptor = newDescriptor.getFields().get(projectedRowOutside.getFieldCount() - 1)
			.getMessageType();
		assertEquals(newRowTypeInside.getFieldCount(), insideDescriptor.getFields().size());
	}

	private Tuple2<Descriptors.Descriptor, Descriptors.Descriptor> testCutUnusedFieldsAccordingToRuntimeRowDataBase(RowType runtimeRowType) throws Exception {
		ProtoFile protoFile = PbSchemaTestUtil.getProtoFile();
		Descriptors.Descriptor oldDescriptor = ProtoFileUtils.parseDescriptorFromProtoFile(protoFile);
		Descriptors.Descriptor newDescriptor = ProtoCutUtil.cutPbDescriptor(oldDescriptor, runtimeRowType);
		assertEquals(runtimeRowType.getFieldCount(), newDescriptor.getFields().size());
		DynamicMessage dynamicMessage = DynamicMessage.parseFrom(newDescriptor, PbSchemaTestUtil.generatePbBytes());
		Set<String> runtimeFieldNames = new HashSet<>(runtimeRowType.getFieldNames());

		// check whether runtime cut works or not
		Set<String> expectedFieldNames = newDescriptor
			.getFields()
			.stream()
			.filter(f -> runtimeFieldNames.contains(f.getName()))
			.map(field -> field.getName())
			.collect(Collectors.toSet());
		Set<String> actualFieldNames = dynamicMessage
			.getAllFields()
			.keySet()
			.stream()
			.map(Descriptors.FieldDescriptor::getName)
			.collect(Collectors.toSet());
		Map<Integer, UnknownFieldSet.Field> actualUnknownFields = dynamicMessage.getUnknownFields().asMap();

		assertTrue(actualFieldNames.stream().allMatch(index -> expectedFieldNames.contains(index)));
		if (!runtimeRowType.equals(baseRowType)) {
			//unknownFields should not empty if we project runtime rowType.
			assertFalse(actualUnknownFields.isEmpty());
		}

		return Tuple2.of(oldDescriptor, newDescriptor);
	}

	private RowType projectRowType(RowType baseRowType, List<Integer> projectedFields) {
		final List<RowType.RowField> fields = baseRowType.getFields();
		final List<LogicalType> fieldTypes = new ArrayList<>();
		final List<String> fieldNames = new ArrayList<>();
		for (int fieldIndex : projectedFields) {
			RowType.RowField rowField = fields.get(fieldIndex);
			fieldTypes.add(rowField.getType());
			fieldNames.add(rowField.getName());
		}
		final LogicalType[] types = fieldTypes.toArray(new LogicalType[fieldTypes.size()]);
		final String[] names = fieldNames.toArray(new String[fieldNames.size()]);
		return RowType.of(types, names);
	}

	private void checkDescriptorEquality(Descriptors.Descriptor oldDescriptor, Descriptors.Descriptor newDescriptor) {
		Descriptors.FileDescriptor oldFile = oldDescriptor.getFile();
		Descriptors.FileDescriptor newFile = newDescriptor.getFile();
		assertEquals(oldFile.getSyntax(), newFile.getSyntax());
		assertEquals(oldFile.getPackage(), newFile.getPackage());
		assertEquals(oldFile.getOptions().getJavaPackage(), newFile.getOptions().getJavaPackage());
		assertEquals(oldFile.getOptions().getJavaOuterClassname(), newFile.getOptions().getJavaOuterClassname());
		assertEquals(oldDescriptor.getName(), newDescriptor.getName());
		List<Descriptors.Descriptor> oldNestTypes = oldDescriptor.getNestedTypes();
		List<Descriptors.Descriptor> newNestTypes = newDescriptor.getNestedTypes();
		assertEquals(oldNestTypes.size(), newNestTypes.size());
		for (int i = 0; i < oldDescriptor.getNestedTypes().size(); i++) {
			checkDescriptorEquality(oldNestTypes.get(i), newNestTypes.get(i));
		}
		List<Descriptors.EnumDescriptor> oldEnumTypes = oldDescriptor.getEnumTypes();
		List<Descriptors.EnumDescriptor> newEnumTypes = newDescriptor.getEnumTypes();
		assertEquals(oldEnumTypes.size(), newEnumTypes.size());
		for (int i = 0; i < oldEnumTypes.size(); i++) {
			checkEnumDescriptorEquality(oldEnumTypes.get(i), newEnumTypes.get(i));
		}
		DescriptorProtos.MessageOptions oldOptions = oldDescriptor.getOptions();
		DescriptorProtos.MessageOptions newOptions = newDescriptor.getOptions();
		assertEquals(oldOptions.getMapEntry(), newOptions.getMapEntry());

		List<Descriptors.FieldDescriptor> oldFields = oldDescriptor.getFields();
		List<Descriptors.FieldDescriptor> newFields = newDescriptor.getFields();

		assertEquals(oldFields.size(), newFields.size());
		for (int i = 0; i < oldFields.size(); i++) {
			Descriptors.FieldDescriptor oldField = oldFields.get(i);
			Descriptors.FieldDescriptor newField = newFields.get(i);
			assertEquals(oldField.getName(), newField.getName());
			assertEquals(oldField.getType(), newField.getType());
			if (oldField.hasDefaultValue()) {
				assertEquals(oldField.getDefaultValue(), newField.getDefaultValue());
			}
			DescriptorProtos.FieldDescriptorProto.Label oldLabel = oldField.toProto().getLabel();
			DescriptorProtos.FieldDescriptorProto.Label newLabel = newField.toProto().getLabel();
			assertEquals(oldLabel, newLabel);
			if (oldField.toProto().getType().equals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)) {
				checkDescriptorEquality(oldField.getMessageType(), newField.getMessageType());
			}
			if (oldField.toProto().getType().equals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)) {
				checkEnumDescriptorEquality(oldField.getEnumType(), newField.getEnumType());
			}
			assertEquals(oldField.isMapField(), newField.isMapField());
			assertEquals(oldField.isPacked(), newField.isPacked());
		}
	}

	private void checkEnumDescriptorEquality(Descriptors.EnumDescriptor oldEnum, Descriptors.EnumDescriptor newEnum) {
		assertEquals(oldEnum.getName(), newEnum.getName());

		final DescriptorProtos.EnumOptions oldEnumOptions = oldEnum.getOptions();
		final DescriptorProtos.EnumOptions newEnumOptions = newEnum.getOptions();
		assertEquals(oldEnumOptions.hasAllowAlias(), newEnumOptions.hasAllowAlias());
		assertEquals(oldEnumOptions.hasDeprecated(), newEnumOptions.hasDeprecated());
		final List<Descriptors.EnumValueDescriptor> oldEnumValues = oldEnum.getValues();
		final List<Descriptors.EnumValueDescriptor> newEnumValues = newEnum.getValues();
		assertEquals(oldEnumValues.size(), newEnumValues.size());
		for (int i = 0; i < oldEnumValues.size(); i++) {
			final Descriptors.EnumValueDescriptor theOld = oldEnumValues.get(i);
			final Descriptors.EnumValueDescriptor theNew = newEnumValues.get(i);
			assertEquals(theOld.getName(), theNew.getName());
			assertEquals(theOld.getOptions().hasDeprecated(), theNew.getOptions().hasDeprecated());
			assertEquals(theOld.getOptions().getSerializedSize(), theNew.getOptions().getSerializedSize());
		}
	}
}
