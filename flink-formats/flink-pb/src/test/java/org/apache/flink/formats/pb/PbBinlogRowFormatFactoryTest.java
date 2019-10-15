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
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PbBinlogRowFormatFactory}.
 */
public class PbBinlogRowFormatFactoryTest {
	@Test
	public void testDeserializationSchema() {
		final Map<String, String> properties = new HashMap<>();
		properties.put("format.type", "pb_binlog");
		properties.put("schema.0.name", "header");
		properties.put("schema.0.type", "ROW<version INT, logfileName VARCHAR, logfileOffset BIGINT, serverId BIGINT, serverenCode VARCHAR, executeTime BIGINT, sourceType VARCHAR, schemaName VARCHAR, tableName VARCHAR, eventLength BIGINT, eventType VARCHAR, props OBJECT_ARRAY<ROW<key VARCHAR, value VARCHAR>>>");
		properties.put("schema.1.name", "entryType");
		properties.put("schema.1.type", "VARCHAR");
		properties.put("schema.2.name", "TransactionBegin");
		properties.put("schema.2.type", "ROW<executeTime BIGINT, transactionId VARCHAR, props OBJECT_ARRAY<ROW<key VARCHAR, value VARCHAR>>, threadId BIGINT>");
		properties.put("schema.3.name", "RowChange");
		properties.put("schema.3.type", "ROW<tableId BIGINT, eventType VARCHAR, isDdl BOOLEAN, sql VARCHAR, rowDatas OBJECT_ARRAY<ROW<beforeColumns OBJECT_ARRAY<ROW<index INT, sqlType INT, name VARCHAR, isKey BOOLEAN, updated BOOLEAN, isNull BOOLEAN, props OBJECT_ARRAY<ROW<key VARCHAR, value VARCHAR>>, value VARCHAR, length INT, mysqlType VARCHAR>>, afterColumns OBJECT_ARRAY<ROW<index INT, sqlType INT, name VARCHAR, isKey BOOLEAN, updated BOOLEAN, isNull BOOLEAN, props OBJECT_ARRAY<ROW<key VARCHAR, value VARCHAR>>, value VARCHAR, length INT, mysqlType VARCHAR>>, props OBJECT_ARRAY<ROW<key VARCHAR, value VARCHAR>>>>, props OBJECT_ARRAY<ROW<key VARCHAR, value VARCHAR>>, ddlSchemaName VARCHAR>");
		properties.put("schema.4.name", "TransactionEnd");
		properties.put("schema.4.type", "ROW<executeTime BIGINT, transactionId VARCHAR, props OBJECT_ARRAY<ROW<key VARCHAR, value VARCHAR>>>");

		final DeserializationSchema<?> deserializationSchema = TableFactoryService
			.find(DeserializationSchemaFactory.class, properties)
			.createDeserializationSchema(properties);

		TypeInformation[] types = ((RowTypeInfo) PbBinlogRowFormatFactory.getBinlogRowTypeInformation()).getFieldTypes();
		RowTypeInfo rowTypeInfo = (RowTypeInfo) PbBinlogRowFormatFactory.deriveSchema(properties).toRowType();

		final PbBinlogRowDeserializationSchema expectedSchema = PbBinlogRowDeserializationSchema.Builder.newBuilder(rowTypeInfo)
			.setHeaderType(types[0])
			.setEntryTypeType(types[1])
			.setTransactionBeginTypeInfo(types[2])
			.setRowChangeTypeInfo(types[3])
			.setTransactionEndTypeInfo(types[4])
			.build();

		assertEquals(expectedSchema, deserializationSchema);
	}
}
