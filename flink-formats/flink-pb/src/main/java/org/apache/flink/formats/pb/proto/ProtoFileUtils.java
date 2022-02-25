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

import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import kotlin.ranges.IntRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Util class that helps to convert content of protobuf file to {@link Descriptors.Descriptor}.
 * This class is migrated from io.confluent.kafka.schemaregistry.protobuf#ProtobufSchema, which
 * is in io.confluent:kafka-protobuf-provider.
 */
public class ProtoFileUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ProtoFileUtils.class);
	private static final Location DEFAULT_LOCATION = Location.get("");

	private static final String MAP_ENTRY_SUFFIX = "Entry";
	static final String KEY_FIELD = "key";
	static final String VALUE_FIELD = "value";

	public static Descriptors.Descriptor parseDescriptorFromProtoFile(ProtoFile protoFile) {
		ProtoFileElement protoFileElement =
			ProtoParser.Companion.parse(DEFAULT_LOCATION, protoFile.getContent());
		String entryClassName = protoFile.getEntryClassName();
		return toDynamicSchema(entryClassName, protoFileElement)
			.getMessageDescriptor(entryClassName);
	}

	private static DynamicSchema toDynamicSchema(String name, ProtoFileElement rootElem) {
		LOG.debug("toDynamicSchema: {}", rootElem.toSchema());
		DynamicSchema.Builder schema = DynamicSchema.newBuilder();
		try {
			com.squareup.wire.schema.ProtoFile.Syntax syntax = rootElem.getSyntax();
			if (syntax != null) {
				schema.setSyntax(syntax.toString());
			}
			if (rootElem.getPackageName() != null) {
				schema.setPackage(rootElem.getPackageName());
			}
			for (TypeElement typeElem : rootElem.getTypes()) {
				if (typeElem instanceof MessageElement) {
					MessageDefinition message = toDynamicMessage((MessageElement) typeElem);
					schema.addMessageDefinition(message);
				} else if (typeElem instanceof EnumElement) {
					EnumDefinition enumer = toDynamicEnum((EnumElement) typeElem);
					schema.addEnumDefinition(enumer);
				}
			}

			findOption("java_package", rootElem.getOptions())
				.map(o -> o.getValue().toString()).ifPresent(schema::setJavaPackage);
			findOption("java_outer_classname", rootElem.getOptions())
				.map(o -> o.getValue().toString()).ifPresent(schema::setJavaOuterClassname);
			findOption("java_multiple_files", rootElem.getOptions())
				.map(o -> Boolean.valueOf(o.getValue().toString())).ifPresent(schema::setJavaMultipleFiles);
			schema.setName(name);
			return schema.build();
		} catch (Descriptors.DescriptorValidationException e) {
			throw new IllegalStateException(e);
		}
	}

	private static MessageDefinition toDynamicMessage(MessageElement messageElem) {
		LOG.debug("message: {}", messageElem.getName());
		MessageDefinition.Builder message = MessageDefinition.newBuilder(messageElem.getName());
		for (TypeElement type : messageElem.getNestedTypes()) {
			if (type instanceof MessageElement) {
				message.addMessageDefinition(toDynamicMessage((MessageElement) type));
			} else if (type instanceof EnumElement) {
				message.addEnumDefinition(toDynamicEnum((EnumElement) type));
			}
		}
		Set<String> added = new HashSet<>();
		for (OneOfElement oneof : messageElem.getOneOfs()) {
			MessageDefinition.OneofBuilder oneofBuilder = message.addOneof(oneof.getName());
			for (FieldElement field : oneof.getFields()) {
				String defaultVal = field.getDefaultValue();
				String jsonName = findOption("json_name", field.getOptions())
					.map(o -> o.getValue().toString()).orElse(null);
				oneofBuilder.addField(
					field.getType(),
					field.getName(),
					field.getTag(),
					defaultVal,
					jsonName
				);
				added.add(field.getName());
			}
		}
		// Process fields after messages so that any newly created map entry messages are at the end
		for (FieldElement field : messageElem.getFields()) {
			if (added.contains(field.getName())) {
				continue;
			}
			Field.Label fieldLabel = field.getLabel();
			String label = fieldLabel != null ? fieldLabel.toString().toLowerCase() : null;
			String fieldType = field.getType();
			String defaultVal = field.getDefaultValue();
			String jsonName = findOption("json_name", field.getOptions())
				.map(o -> o.getValue().toString()).orElse(null);
			Boolean isPacked = findOption("packed", field.getOptions())
				.map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
			ProtoType protoType = ProtoType.get(fieldType);
			ProtoType keyType = protoType.getKeyType();
			ProtoType valueType = protoType.getValueType();
			// Map fields are only permitted in messages
			if (protoType.isMap() && keyType != null && valueType != null) {
				label = "repeated";
				fieldType = toMapEntry(field.getName());
				MessageDefinition.Builder mapMessage = MessageDefinition.newBuilder(fieldType);
				mapMessage.setMapEntry(true);
				mapMessage.addField(null, keyType.getSimpleName(), KEY_FIELD, 1, null);
				mapMessage.addField(null, valueType.getSimpleName(), VALUE_FIELD, 2, null);
				message.addMessageDefinition(mapMessage.build());
			}
			message.addField(
				label,
				fieldType,
				field.getName(),
				field.getTag(),
				defaultVal,
				jsonName,
				isPacked
			);
		}
		for (ReservedElement reserved : messageElem.getReserveds()) {
			for (Object elem : reserved.getValues()) {
				if (elem instanceof String) {
					message.addReservedName((String) elem);
				} else if (elem instanceof Integer) {
					int tag = (Integer) elem;
					message.addReservedRange(tag, tag);
				} else if (elem instanceof IntRange) {
					IntRange range = (IntRange) elem;
					message.addReservedRange(range.getStart(), range.getEndInclusive());
				} else {
					throw new IllegalStateException("Unsupported reserved type: " + elem.getClass()
						.getName());
				}
			}
		}
		findOption("map_entry", messageElem.getOptions())
			.map(o -> Boolean.valueOf(o.getValue().toString())).ifPresent(message::setMapEntry);
		return message.build();
	}

	private static Optional<OptionElement> findOption(String name, List<OptionElement> options) {
		return options.stream().filter(o -> o.getName().equals(name)).findFirst();
	}

	private static EnumDefinition toDynamicEnum(EnumElement enumElem) {
		Boolean allowAlias = findOption("allow_alias", enumElem.getOptions())
			.map(o -> Boolean.valueOf(o.getValue().toString())).orElse(null);
		EnumDefinition.Builder enumer = EnumDefinition.newBuilder(enumElem.getName(), allowAlias);
		for (EnumConstantElement constant : enumElem.getConstants()) {
			enumer.addValue(constant.getName(), constant.getTag());
		}
		return enumer.build();
	}

	public static String toMapEntry(String s) {
		if (s.contains("_")) {
			s = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, s);
		}
		return s + MAP_ENTRY_SUFFIX;
	}

}
