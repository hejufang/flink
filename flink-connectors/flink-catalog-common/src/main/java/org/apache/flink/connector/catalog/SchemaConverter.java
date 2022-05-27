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

package org.apache.flink.connector.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import com.bytedance.schema.registry.common.table.ByteSchemaElementType;
import com.bytedance.schema.registry.common.table.ByteSchemaField;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Kafka schema converter, which helps converting ByteSchemaTable to flink TableSchema.
 */
public class SchemaConverter {

	private static final Pattern TYPE_PRECISION_PATTERN = Pattern.compile("^([A-Z]+)\\((\\d+)\\)$");
	private static final Pattern TYPE_PRECISION_SCALE_PATTERN = Pattern.compile("^([A-Z]+)\\((\\d+),\\s*(\\d+)\\)$");

	public static TableSchema convertToTableSchema(List<ByteSchemaField> byteSchemaFields) {
		TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
		byteSchemaFields.stream()
			.map(SchemaConverter::convertToTableColumn)
			.forEach(tableColumn -> tableSchemaBuilder.field(tableColumn.getName(), tableColumn.getType()));
		return tableSchemaBuilder.build();
	}

	private static TableColumn convertToTableColumn(ByteSchemaField byteSchemaField) {
		String fieldName = byteSchemaField.getName();
		DataType fieldType = parseDataType(byteSchemaField);
		return TableColumn.of(fieldName, fieldType);
	}

	private static DataType parseDataType(ByteSchemaField byteSchemaField) {
		String typeName = byteSchemaField.getType();
		Matcher matcher = TYPE_PRECISION_SCALE_PATTERN.matcher(typeName);
		if (matcher.matches()) {
			String typeRoot = matcher.group(1);
			int precision = Integer.parseInt(matcher.group(2));
			int scale = Integer.parseInt(matcher.group(3));
			switch (typeRoot) {
				case "DECIMAL":
				case "NUMERIC":
					return DataTypes.DECIMAL(precision, scale);
			}
		}
		matcher = TYPE_PRECISION_PATTERN.matcher(typeName);
		if (matcher.matches()) {
			String typeRoot = matcher.group(1);
			int precision = Integer.parseInt(matcher.group(2));
			switch (typeRoot) {
				case "CHAR":
					return DataTypes.CHAR(precision);
				case "VARCHAR":
					return DataTypes.VARCHAR(precision);
				case "BINARY":
					return DataTypes.BINARY(precision);
				case "VARBINARY":
					return DataTypes.VARBINARY(precision);
				case "DECIMAL":
				case "NUMERIC":
					return DataTypes.DECIMAL(precision, 0);
				case "TIME":
					return DataTypes.TIME(precision);
				case "TIMESTAMP":
				case "TIMESTAMP WITHOUT TIME ZONE":
					return DataTypes.TIMESTAMP(precision);
			}
		}
		switch (typeName) {
			case "CHAR":
			case "VARCHAR":
			case "STRING":
				return DataTypes.STRING();
			case "BINARY":
				return DataTypes.BINARY(1);
			case "VARBINARY":
			case "BYTES":
				return DataTypes.BYTES();
			case "DECIMAL":
			case "NUMERIC":
				return DataTypes.DECIMAL(10, 0);
			case "TINYINT":
				return DataTypes.TINYINT();
			case "SMALLINT":
				return DataTypes.SMALLINT();
			case "INT":
			case "INTEGER":
				return DataTypes.INT();
			case "BIGINT":
				return DataTypes.BIGINT();
			case "FLOAT":
				return DataTypes.FLOAT();
			case "DOUBLE":
				return DataTypes.DOUBLE();
			case "DATE":
				return DataTypes.DATE();
			case "TIME":
				return DataTypes.TIME();
			case "TIMESTAMP":
			case "TIMESTAMP WITHOUT TIME ZONE":
				return DataTypes.TIMESTAMP();
			case "BOOLEAN":
				return DataTypes.BOOLEAN();
			case "ROW":
				return parseRowType(byteSchemaField);
			case "MAP":
				return parseMapType(byteSchemaField);
			case "ARRAY":
				return parseArrayType(byteSchemaField);
			default:
				throw new IllegalStateException("Unsupported type: " + typeName);
		}
	}

	private static DataType parseRowType(ByteSchemaField byteSchemaField) {
		List<ByteSchemaField> innerByteSchemaFields = byteSchemaField.getFields();

		List<DataTypes.Field> fields = new ArrayList<>();
		for (ByteSchemaField innerByteSchemaField : innerByteSchemaFields) {
			String innerFieldName = innerByteSchemaField.getName();
			String innerFieldDesc = innerByteSchemaField.getDescription();
			innerFieldDesc = innerFieldDesc == null ? "" : innerFieldDesc;
			DataType innerFieldType = parseDataType(innerByteSchemaField);
			fields.add(DataTypes.FIELD(innerFieldName, innerFieldType, innerFieldDesc));
		}
		return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
	}

	private static DataType parseMapType(ByteSchemaField byteSchemaField) {
		ByteSchemaElementType byteSchemaKeyType = byteSchemaField.getMapKeyType();
		ByteSchemaElementType byteSchemaValueType = byteSchemaField.getMapValueType();
		DataType keyType = parseDataType(wrapElementTypeIntoSchemaField(byteSchemaKeyType));
		DataType valueType = parseDataType(wrapElementTypeIntoSchemaField(byteSchemaValueType));
		return DataTypes.MAP(keyType, valueType);
	}

	private static DataType parseArrayType(ByteSchemaField byteSchemaField) {
		ByteSchemaElementType byteSchemaElementType = byteSchemaField.getArrayElementType();
		DataType elementType = parseDataType(wrapElementTypeIntoSchemaField(byteSchemaElementType));
		return DataTypes.ARRAY(elementType);
	}

	private static ByteSchemaField wrapElementTypeIntoSchemaField(ByteSchemaElementType elementType) {
		return new ByteSchemaField()
			.setType(elementType.getType())
			.setFields(elementType.getFields())
			.setArrayElementType(elementType.getArrayElementType())
			.setMapKeyType(elementType.getMapKeyType())
			.setMapValueType(elementType.getMapValueType())
			.setExtraMeta(elementType.getExtraMeta());
	}
}
