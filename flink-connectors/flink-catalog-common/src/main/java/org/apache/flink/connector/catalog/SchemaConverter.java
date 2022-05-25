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
import com.bytedance.schema.registry.common.util.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Kafka schema converter, which helps converting ByteSchemaTable to flink TableSchema.
 */
public class SchemaConverter {

	private static final Pattern TYPE_PRECISION_PATTERN = Pattern.compile("^([A-Z]+)\\((\\d+)\\)$");
	private static final Pattern TYPE_PRECISION_SCALE_PATTERN = Pattern.compile("^([A-Z]+)\\((\\d+),\\s*(\\d+)\\)$");

	public static TableSchema convertToTableSchema(
			List<ByteSchemaField> byteSchemaFields,
			Map<String, Object> extraContent) {
		TableSchema.Builder tableSchemaBuilder = TableSchema.builder();

		Optional<String[]> optionalPks = Constants.getPrimaryKeys(extraContent);
		Set<String> pks = new HashSet<>();
		Constants.getPrimaryKeys(extraContent).ifPresent(strings -> pks.addAll(Arrays.asList(strings)));
		optionalPks.ifPresent(tableSchemaBuilder::primaryKey);

		for (ByteSchemaField field : byteSchemaFields) {
			boolean isPrimaryKey = pks.contains(field.getName());
			TableColumn column = convertToTableColumn(field, isPrimaryKey);
			tableSchemaBuilder.field(column.getName(), column.getType());
		}
		return tableSchemaBuilder.build();
	}

	private static TableColumn convertToTableColumn(ByteSchemaField byteSchemaField, boolean isPrimaryKey) {
		String fieldName = byteSchemaField.getName();
		DataType fieldType = parseDataType(byteSchemaField, isPrimaryKey);
		return TableColumn.of(fieldName, fieldType);
	}

	private static DataType parseDataType(ByteSchemaField byteSchemaField, boolean isPrimaryKey) {
		DataType dataType = null;
		String typeName = byteSchemaField.getType();
		Matcher matcher1 = TYPE_PRECISION_SCALE_PATTERN.matcher(typeName);
		Matcher matcher2 = TYPE_PRECISION_PATTERN.matcher(typeName);
		if (matcher1.matches()) {   // data type with precision and scale
			String typeRoot = matcher1.group(1);
			int precision = Integer.parseInt(matcher1.group(2));
			int scale = Integer.parseInt(matcher1.group(3));
			switch (typeRoot) {
				case "DECIMAL":
				case "NUMERIC":
					dataType = DataTypes.DECIMAL(precision, scale);
					break;
				default:
					throw new IllegalStateException("Unsupported type: " + typeName);
			}
		} else if (matcher2.matches()) {   // data type with precision
			String typeRoot = matcher2.group(1);
			int precision = Integer.parseInt(matcher2.group(2));
			switch (typeRoot) {
				case "CHAR":
					dataType = DataTypes.CHAR(precision);
					break;
				case "VARCHAR":
					dataType = DataTypes.VARCHAR(precision);
					break;
				case "BINARY":
					dataType = DataTypes.BINARY(precision);
					break;
				case "VARBINARY":
					dataType = DataTypes.VARBINARY(precision);
					break;
				case "DECIMAL":
				case "NUMERIC":
					dataType = DataTypes.DECIMAL(precision, 0);
					break;
				case "TIME":
					dataType = DataTypes.TIME(precision);
					break;
				case "TIMESTAMP":
				case "TIMESTAMP WITHOUT TIME ZONE":
					dataType = DataTypes.TIMESTAMP(precision);
					break;
				default:
					throw new IllegalStateException("Unsupported type: " + typeName);
			}
		} else {   // data type without precision or scale
			switch (typeName) {
				case "CHAR":
				case "VARCHAR":
				case "STRING":
					dataType = DataTypes.STRING();
					break;
				case "BINARY":
					dataType = DataTypes.BINARY(1);
					break;
				case "VARBINARY":
				case "BYTES":
					dataType = DataTypes.BYTES();
					break;
				case "DECIMAL":
				case "NUMERIC":
					dataType = DataTypes.DECIMAL(10, 0);
					break;
				case "TINYINT":
					dataType = DataTypes.TINYINT();
					break;
				case "SMALLINT":
					dataType = DataTypes.SMALLINT();
					break;
				case "INT":
				case "INTEGER":
					dataType = DataTypes.INT();
					break;
				case "BIGINT":
					dataType = DataTypes.BIGINT();
					break;
				case "FLOAT":
					dataType = DataTypes.FLOAT();
					break;
				case "DOUBLE":
					dataType = DataTypes.DOUBLE();
					break;
				case "DATE":
					dataType = DataTypes.DATE();
					break;
				case "TIME":
					dataType = DataTypes.TIME();
					break;
				case "TIMESTAMP":
				case "TIMESTAMP WITHOUT TIME ZONE":
					dataType = DataTypes.TIMESTAMP();
					break;
				case "BOOLEAN":
					dataType = DataTypes.BOOLEAN();
					break;
				case "ROW":
					dataType = parseRowType(byteSchemaField);
					break;
				case "MAP":
					dataType = parseMapType(byteSchemaField);
					break;
				case "ARRAY":
					dataType = parseArrayType(byteSchemaField);
					break;
				default:
					throw new IllegalStateException("Unsupported type: " + typeName);
			}
		}
		if (isPrimaryKey) {
			dataType = dataType.notNull();
		}
		return dataType;
	}

	private static DataType parseRowType(ByteSchemaField byteSchemaField) {
		List<ByteSchemaField> innerByteSchemaFields = byteSchemaField.getFields();

		List<DataTypes.Field> fields = new ArrayList<>();
		for (ByteSchemaField innerByteSchemaField : innerByteSchemaFields) {
			String innerFieldName = innerByteSchemaField.getName();
			String innerFieldDesc = innerByteSchemaField.getDescription();
			innerFieldDesc = innerFieldDesc == null ? "" : innerFieldDesc;
			DataType innerFieldType = parseDataType(innerByteSchemaField, false);
			fields.add(DataTypes.FIELD(innerFieldName, innerFieldType, innerFieldDesc));
		}
		return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
	}

	private static DataType parseMapType(ByteSchemaField byteSchemaField) {
		ByteSchemaElementType byteSchemaKeyType = byteSchemaField.getMapKeyType();
		ByteSchemaElementType byteSchemaValueType = byteSchemaField.getMapValueType();
		DataType keyType = parseDataType(wrapElementTypeIntoSchemaField(byteSchemaKeyType), false);
		DataType valueType = parseDataType(wrapElementTypeIntoSchemaField(byteSchemaValueType), false);
		return DataTypes.MAP(keyType, valueType);
	}

	private static DataType parseArrayType(ByteSchemaField byteSchemaField) {
		ByteSchemaElementType byteSchemaElementType = byteSchemaField.getArrayElementType();
		DataType elementType = parseDataType(wrapElementTypeIntoSchemaField(byteSchemaElementType), false);
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
