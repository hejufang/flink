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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.PbValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Factory to get binlog row type information and deserialize schema.
 */
public class PbBinlogRowFormatFactory extends TableFormatFactoryBase<Row>
	implements DeserializationSchemaFactory<Row> {

	public PbBinlogRowFormatFactory() {
		super(PbConstant.FORMAT_BINLOG_TYPE_VALUE, 1, true);
	}

	@Override
	protected List<String> supportedFormatProperties() {
		return Collections.emptyList();
	}

	public TypeInformation<Row> getBinlogRowTypeInformation(Map<String, String> properties) {
		getValidatedProperties(properties);

		return getBinlogRowTypeInformation();
	}

	public static TypeInformation<Row> getBinlogRowTypeInformation() {
		TypeInformation[] types = getBinlogRowTypeInformationsArray();
		String[] names = getBinlogRowNamesArray();
		return new RowTypeInfo(types, names);
	}

	private static TypeInformation[] getBinlogRowTypeInformationsArray() {
		TypeInformation[] types = new TypeInformation[5];
		types[0] = PbRowTypeInformation.generateRow(CanalEntry.Header.getDescriptor());
		types[1] = Types.STRING;
		types[2] = PbRowTypeInformation.generateRow(CanalEntry.TransactionBegin.getDescriptor());
		types[3] = PbRowTypeInformation.generateRow(CanalEntry.RowChange.getDescriptor());
		types[4] = PbRowTypeInformation.generateRow(CanalEntry.TransactionEnd.getDescriptor());
		return types;
	}

	private static String[] getBinlogRowNamesArray() {
		String[] names = new String[5];
		names[0] = PbConstant.FORMAT_BINLOG_TYPE_HEADER;
		names[1] = PbConstant.FORMAT_BINLOG_TYPE_ENTRY_TYPE;
		names[2] = PbConstant.FORMAT_BINLOG_TYPE_TRANSACTION_BEGIN;
		names[3] = PbConstant.FORMAT_BINLOG_TYPE_ROW_CHANGE;
		names[4] = PbConstant.FORMAT_BINLOG_TYPE_TRANSACTION_END;
		return names;
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
		getValidatedProperties(properties);

		TypeInformation[] types = getBinlogRowTypeInformationsArray();

		PbBinlogRowDeserializationSchema.Builder schemaBuilder = PbBinlogRowDeserializationSchema.Builder.newBuilder()
			.setHeaderType(types[0])
			.setEntryTypeType(types[1])
			.setTransactionBeginTypeInfo(types[2])
			.setRowChangeTypeInfo(types[3])
			.setTransactionEndTypeInfo(types[4]);

		return schemaBuilder.build();
	}

	private void getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new PbValidator().validate(descriptorProperties);
	}
}
