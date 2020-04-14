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

package org.apache.flink.format.binlog;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.pb.PbRowTypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descripters.BinlogValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import com.bytedance.binlog.DRCEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descripters.BinlogValidator.BINLOG_BODY;
import static org.apache.flink.table.descripters.BinlogValidator.BINLOG_HEADER;
import static org.apache.flink.table.descripters.BinlogValidator.FORMAT_IGNORE_PARSE_ERRORS;
import static org.apache.flink.table.descripters.BinlogValidator.FORMAT_TARGET_TABLE;
import static org.apache.flink.table.descripters.BinlogValidator.FORMAT_TYPE_VALUE;

/**
 * Binlog row format factory.
 */
public class BinlogRowFormatFactory extends TableFormatFactoryBase<Row>
		implements DeserializationSchemaFactory<Row> {
	public BinlogRowFormatFactory() {
		super(FORMAT_TYPE_VALUE, 1, true);
	}

	@Override
	protected List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(FORMAT_IGNORE_PARSE_ERRORS);
		properties.add(FORMAT_TARGET_TABLE);
		return properties;
	}

	/**
	 * Parse the column from properties and append the extra header & body columns.
	 *
	 * @return TypeInformation of the binlog table row.
	 */
	public static RowTypeInfo getBinlogRowTypeInformation(
			List<String> userDefindedColumns,
			List<TypeInformation> userDefindedTypes) {

		TypeInformation<Row> headerTypeInfo =
			PbRowTypeInformation.generateRow(DRCEntry.EntryHeader.getDescriptor());
		TypeInformation<Row> bodyTypeInfo =
			PbRowTypeInformation.generateRow(DRCEntry.EntryBody.getDescriptor());
		int userDefindedColumnNum = userDefindedColumns.size();
		int newArity = userDefindedColumnNum + 2;
		TypeInformation[] types = new TypeInformation[newArity];
		String[] fieldNames = new String[newArity];

		// add header
		types[0] = headerTypeInfo;
		fieldNames[0] = BINLOG_HEADER;

		// add body
		types[1] = bodyTypeInfo;
		fieldNames[1] = BINLOG_BODY;

		// add row data
		for (int i = 0; i < userDefindedColumnNum; i++) {
			types[i + 2] = userDefindedTypes.get(i);
			fieldNames[i + 2] = userDefindedColumns.get(i);
		}

		return new RowTypeInfo(types, fieldNames);
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = deriveSchema(properties);
		BinlogRowDeserializationSchema.Builder builder =
			BinlogRowDeserializationSchema.builder((RowTypeInfo) tableSchema.toRowType());
		getRowtimeColumn(descriptorProperties, tableSchema).ifPresent(builder::setRowtimeColumn);
		getProctimeColumn(descriptorProperties, tableSchema).ifPresent(builder::setProctimeColumn);
		descriptorProperties.getOptionalBoolean(FORMAT_IGNORE_PARSE_ERRORS)
			.ifPresent(builder::setIgnoreParseErrors);
		descriptorProperties.getOptionalString(FORMAT_TARGET_TABLE)
			.ifPresent(builder::setTargetTable);
		return builder.build();
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(properties);

		// validate
		new BinlogValidator().validate(descriptorProperties);
		return descriptorProperties;
	}
}
