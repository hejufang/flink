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
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.PbValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Table format factory for providing configured instances of PB-to-row {@link DeserializationSchema}.
 */
public class PbRowFormatFactory extends TableFormatFactoryBase<Row>
	implements DeserializationSchemaFactory<Row> {

	public PbRowFormatFactory() {
		super(PbConstant.FORMAT_TYPE_VALUE, 1, true);
	}

	@Override
	protected List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(PbConstant.FORMAT_PB_CLASS);
		properties.add(PbConstant.FORMAT_PB_SKIP_BYTES);
		return properties;
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		String pbDescriptorClass = getDescriptorClass(descriptorProperties);
		RowTypeInfo typeInfo = (RowTypeInfo) deriveSchema(properties).toRowType();
		Optional<Integer> skipBytes =
			descriptorProperties.getOptionalInt(PbConstant.FORMAT_PB_SKIP_BYTES);

		PbRowDeserializationSchema.Builder schemaBuilder = PbRowDeserializationSchema.Builder.newBuilder()
			.setPbDescriptorClass(pbDescriptorClass)
			.setTypeInfo(typeInfo);

		skipBytes.ifPresent(schemaBuilder::setSkipBytes);
		return schemaBuilder.build();
	}

	public TypeInformation<Row> getRowTypeInformation(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		Descriptors.Descriptor pbDescriptor = createDescriptor(descriptorProperties);
		return PbRowTypeInformation.generateRow(pbDescriptor, getTimestampSchemaIndex(descriptorProperties));
	}

	private Optional<Integer> getTimestampSchemaIndex(DescriptorProperties descriptorProperties) {
		String reg = "schema.(\\d+).rowtime.*";
		for (String propertiesKey : descriptorProperties.asMap().keySet()) {
			Matcher matcher = Pattern.compile(reg).matcher(propertiesKey);
			if (matcher.find()) {
				return Optional.of(Integer.parseInt(matcher.group(1)));
			}
		}
		return Optional.empty();
	}

	private String getDescriptorClass(DescriptorProperties descriptorProperties) {
		return descriptorProperties.getString(PbConstant.FORMAT_PB_CLASS);
	}

	private Descriptors.Descriptor createDescriptor(DescriptorProperties descriptorProperties) {
		String pbClassName = getDescriptorClass(descriptorProperties);
		return PbValidator.validateAndReturnDescriptor(pbClassName);
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new PbValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
