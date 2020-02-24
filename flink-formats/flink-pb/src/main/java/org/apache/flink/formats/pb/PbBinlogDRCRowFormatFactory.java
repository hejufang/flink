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

import com.bytedance.dbus.DRCEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Factory to get drc binlog row type information and deserialize schema.
 */
public class PbBinlogDRCRowFormatFactory extends TableFormatFactoryBase<Row>
	implements DeserializationSchemaFactory<Row> {

	public PbBinlogDRCRowFormatFactory() {
		super(PbConstant.FORMAT_BINLOG_DRC_TYPE_VALUE, 1, true);
	}

	public static TypeInformation<Row> getBinlogRowTypeInformation() {
		TypeInformation[] types = getBinlogRowTypeInformationsArray();
		String[] names = getBinlogRowNamesArray();
		return new RowTypeInfo(types, names);
	}

	private static TypeInformation<Row>[] getBinlogRowTypeInformationsArray() {
		TypeInformation[] types = new TypeInformation[2];
		types[0] = PbRowTypeInformation.generateRow(DRCEntry.EntryHeader.getDescriptor());
		types[1] = PbRowTypeInformation.generateRow(DRCEntry.EntryBody.getDescriptor());
		return types;
	}

	private static String[] getBinlogRowNamesArray() {
		String[] names = new String[2];
		names[0] = PbConstant.FORMAT_BINLOG_DRC_TYPE_HEADER;
		names[1] = PbConstant.FORMAT_BINLOG_DRC_TYPE_BODY;
		return names;
	}

	@Override
	protected List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(PbConstant.FORMAT_IGNORE_PARSE_ERRORS);
		return properties;
	}

	public TypeInformation<Row> getBinlogRowTypeInformation(Map<String, String> properties) {
		getValidatedProperties(properties);

		return getBinlogRowTypeInformation();
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		TypeInformation[] types = getBinlogRowTypeInformationsArray();

		RowTypeInfo typeInfo = (RowTypeInfo) deriveSchema(properties).toRowType();

		PbBinlogDRCRowDeserializationSchema.Builder schemaBuilder =
			PbBinlogDRCRowDeserializationSchema.builder(typeInfo)
				.setHeaderType(types[0])
				.setBodyTypeInfo(types[1]);
		descriptorProperties.getOptionalBoolean(PbConstant.FORMAT_IGNORE_PARSE_ERRORS)
			.ifPresent(schemaBuilder::setIgnoreParseErrors);

		return schemaBuilder.build();
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new PbValidator().validate(descriptorProperties);
		return descriptorProperties;
	}
}
