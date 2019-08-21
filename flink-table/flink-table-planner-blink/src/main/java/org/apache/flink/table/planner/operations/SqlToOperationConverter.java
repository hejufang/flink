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

package org.apache.flink.table.planner.operations;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.pb.PbConstant;
import org.apache.flink.formats.pb.PbRowFormatFactory;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.operations.ddl.DropTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Mix-in tool class for {@code SqlNode} that allows DDL commands to be
 * converted to {@link Operation}.
 *
 * <p>For every kind of {@link SqlNode}, there needs to have a corresponding
 * #convert(type) method, the 'type' argument should be the subclass
 * of the supported {@link SqlNode}.
 *
 * <p>Every #convert() should return a {@link Operation} which can be used in
 * {@link org.apache.flink.table.delegation.Planner}.
 */
public class SqlToOperationConverter {
	private FlinkPlannerImpl flinkPlanner;

	//~ Constructors -----------------------------------------------------------

	private SqlToOperationConverter(FlinkPlannerImpl flinkPlanner) {
		this.flinkPlanner = flinkPlanner;
	}

	/**
	 * This is the main entrance for executing all kinds of DDL/DML {@code SqlNode}s, different
	 * SqlNode will have it's implementation in the #convert(type) method whose 'type' argument
	 * is subclass of {@code SqlNode}.
	 *
	 * @param flinkPlanner     FlinkPlannerImpl to convertCreateTable sql node to rel node
	 * @param sqlNode          SqlNode to execute on
	 */
	public static Operation convert(FlinkPlannerImpl flinkPlanner, SqlNode sqlNode) {
		// validate the query
		final SqlNode validated = flinkPlanner.validate(sqlNode);
		SqlToOperationConverter converter = new SqlToOperationConverter(flinkPlanner);
		if (validated instanceof SqlCreateTable) {
			return converter.convertCreateTable((SqlCreateTable) validated);
		} if (validated instanceof SqlCreateView) {
			return converter.convertCreateView((SqlCreateView) validated);
		} if (validated instanceof SqlDropTable) {
			return converter.convertDropTable((SqlDropTable) validated);
		} else if (validated instanceof SqlCreateFunction) {
			return converter.convertCreateFunction((SqlCreateFunction) validated);
		} else if (validated instanceof RichSqlInsert) {
			return converter.convertSqlInsert((RichSqlInsert) validated);
		} else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
			return converter.convertSqlQuery(validated);
		} else {
			throw new TableException("Unsupported node type "
				+ validated.getClass().getSimpleName());
		}
	}

	/**
	 * Convert the {@link SqlCreateTable} node.
	 */
	private Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
		// primary key and unique keys are not supported
		if ((sqlCreateTable.getPrimaryKeyList() != null
				&& sqlCreateTable.getPrimaryKeyList().size() > 0)
			|| (sqlCreateTable.getUniqueKeysList() != null
				&& sqlCreateTable.getUniqueKeysList().size() > 0)) {
			throw new SqlConversionException("Primary key and unique key are not supported yet.");
		}

		// set with properties
		SqlNodeList propertyList = sqlCreateTable.getPropertyList();
		Map<String, String> properties = new HashMap<>();
		if (propertyList != null) {
			propertyList.getList().forEach(p ->
				properties.put(((SqlTableOption) p).getKeyString().toLowerCase(),
					((SqlTableOption) p).getValueString()));
		}

		TableSchema tableSchema = createTableSchema(sqlCreateTable,
			new FlinkTypeFactory(new FlinkTypeSystem())); // need to make type factory singleton ?
		String tableComment = "";
		if (sqlCreateTable.getComment() != null) {
			tableComment = sqlCreateTable.getComment().getNlsString().getValue();
		}
		// set partition key
		List<String> partitionKeys = new ArrayList<>();
		SqlNodeList partitionKey = sqlCreateTable.getPartitionKeyList();
		if (partitionKey != null) {
			partitionKeys = partitionKey
				.getList()
				.stream()
				.map(p -> ((SqlIdentifier) p).getSimple())
				.collect(Collectors.toList());
		}
		//set watermark (rowtime)
		Map<String, Rowtime> rowtimes = new HashMap<>();
		SqlWatermark watermark = sqlCreateTable.getWatermark();
		if (watermark != null) {
			Rowtime rowtime = new Rowtime();
			String rowtimeName = watermark.getColumnName().getSimple();
			rowtime.timestampsFromField(rowtimeName);
			try {
				rowtime.watermarksPeriodicBounded(watermark.getWatermarkOffset());
			} catch (Exception e) {
				throw new SqlConversionException(e.getMessage());
			}
			rowtimes.put(rowtimeName, rowtime);

			// change the rowtime field type to timestamp
			TableSchema.Builder schemaWithRowtime = TableSchema.builder();
			for (String fieldName : tableSchema.getFieldNames()) {
				if (fieldName.equalsIgnoreCase(rowtimeName)) {
					schemaWithRowtime.field(fieldName, LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
						new TimestampType(true, TimestampKind.ROWTIME, 3)));
				} else {
					schemaWithRowtime.field(fieldName, tableSchema.getFieldDataType(fieldName).get());
				}
			}
			tableSchema = schemaWithRowtime.build();
		}
		CatalogTable catalogTable = new CatalogTableImpl(tableSchema,
			partitionKeys,
			properties,
			rowtimes,
			tableComment);
		return new CreateTableOperation(sqlCreateTable.fullTableName(), catalogTable,
			sqlCreateTable.isIfNotExists());
	}

	/** Convert DROP TABLE statement. */
	private Operation convertDropTable(SqlDropTable sqlDropTable) {
		return new DropTableOperation(sqlDropTable.fullTableName(), sqlDropTable.getIfExists());
	}

	/**
	 * Convert the {@link SqlCreateView} node.
	 */
	private Operation convertCreateView(SqlCreateView sqlCreateView) {
		flinkPlanner.validate(sqlCreateView.getQuery());
		String comment = (sqlCreateView.getComment() == null) ? "" : sqlCreateView.getComment().toString();
		CatalogView catalogView = new QueryOperationCatalogView(toQueryOperation(flinkPlanner, sqlCreateView.getQuery()), comment);
		return new CreateViewOperation(sqlCreateView.fullViewName(), catalogView, sqlCreateView.isIfNotExists());
	}

	/** Convert CREATE FUNCTION statement. */
	private Operation convertCreateFunction(SqlCreateFunction sqlCreateFunction) {
		return new CreateFunctionOperation(
			sqlCreateFunction.getFunctionName().getSimple(),
			sqlCreateFunction.getClassName(),
			false);
	}

	/** Convert insert into statement. */
	private Operation convertSqlInsert(RichSqlInsert insert) {
		// get name of sink table
		List<String> targetTablePath = ((SqlIdentifier) insert.getTargetTable()).names;
		return new CatalogSinkModifyOperation(
			targetTablePath,
			(PlannerQueryOperation) SqlToOperationConverter.convert(flinkPlanner,
				insert.getSource()),
			insert.getStaticPartitionKVs());
	}

	/** Fallback method for sql query. */
	private Operation convertSqlQuery(SqlNode node) {
		return toQueryOperation(flinkPlanner, node);
	}

	//~ Tools ------------------------------------------------------------------

	/**
	 * Create a table schema from {@link SqlCreateTable}. This schema contains computed column
	 * fields, say, we have a create table DDL statement:
	 * <blockquote><pre>
	 *   create table t(
	 *     a int,
	 *     b varchar,
	 *     c as to_timestamp(b))
	 *   with (
	 *     'connector' = 'csv',
	 *     'k1' = 'v1')
	 * </pre></blockquote>
	 *
	 * <p>The returned table schema contains columns (a:int, b:varchar, c:timestamp).
	 *
	 * @param sqlCreateTable sql create table node.
	 * @param factory        FlinkTypeFactory instance.
	 * @return TableSchema
	 */
	private TableSchema createTableSchema(SqlCreateTable sqlCreateTable,
			FlinkTypeFactory factory) {
		TableSchema physicalSchema = null;
		TableSchema.Builder builder = new TableSchema.Builder();
		Map<String, String> propertiesMap = propertiesToMap(sqlCreateTable.getPropertyList());

		if (sqlCreateTable.getColumnList().size() == 0 &&
			propertiesMap.getOrDefault(FormatDescriptorValidator.FORMAT_TYPE, "").equalsIgnoreCase(PbConstant.FORMAT_TYPE_VALUE)) {
			PbRowFormatFactory pbFactory = new PbRowFormatFactory();
			RowTypeInfo rowTypeInfo = (RowTypeInfo) pbFactory.getRowTypeInformation(
				propertiesToMap(sqlCreateTable.getPropertyList()));
			for (int index = 0; index < rowTypeInfo.getArity(); index++) {
				builder = builder.field(rowTypeInfo.getFieldNames()[index], rowTypeInfo.getTypeAt(index));
			}
			physicalSchema = builder.build();
		} else {
			// setup table columns
			SqlNodeList columnList = sqlCreateTable.getColumnList();
			// collect the physical table schema first.
			final List<SqlNode> physicalColumns = columnList.getList().stream()
				.filter(n -> n instanceof SqlTableColumn).collect(Collectors.toList());
			for (SqlNode node : physicalColumns) {
				SqlTableColumn column = (SqlTableColumn) node;
				final RelDataType relType = column.getType().deriveType(factory,
					column.getType().getNullable());
				builder.field(column.getName().getSimple(),
					LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
						FlinkTypeFactory.toLogicalType(relType)));
				physicalSchema = builder.build();
			}
			assert physicalSchema != null;
			if (sqlCreateTable.containsComputedColumn()) {
				throw new SqlConversionException("Computed columns for DDL is not supported yet!");
			}
		}

		return physicalSchema;
	}

	private Map<String, String> propertiesToMap(SqlNodeList propertiesList) {
		Map<String, String> properties = new HashMap<>();
		for (SqlNode node : propertiesList) {
			SqlTableOption property = (SqlTableOption) node;
			properties.put(property.getKeyString(), property.getValueString());
		}
		return properties;
	}

	private PlannerQueryOperation toQueryOperation(FlinkPlannerImpl planner, SqlNode validated) {
		// transform to a relational tree
		RelRoot relational = planner.rel(validated);
		return new PlannerQueryOperation(relational.project());
	}
}
