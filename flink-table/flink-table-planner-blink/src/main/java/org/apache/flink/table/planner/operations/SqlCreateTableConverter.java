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

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.connector.format.TableSchemaInferrable;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/**
 * Helper class for converting {@link SqlCreateTable} to {@link CreateTableOperation}.
 */
class SqlCreateTableConverter {

	private final MergeTableLikeUtil mergeTableLikeUtil;
	private final CatalogManager catalogManager;
	private final Consumer<SqlTableConstraint> validateTableConstraint;

	SqlCreateTableConverter(
			FlinkCalciteSqlValidator sqlValidator,
			CatalogManager catalogManager,
			Function<SqlNode, String> escapeExpression,
			Consumer<SqlTableConstraint> validateTableConstraint) {
		this.mergeTableLikeUtil = new MergeTableLikeUtil(
			sqlValidator,
			escapeExpression);
		this.catalogManager = catalogManager;
		this.validateTableConstraint = validateTableConstraint;
	}

	/**
	 * Convert the {@link SqlCreateTable} node.
	 */
	Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
		sqlCreateTable.getTableConstraints().forEach(validateTableConstraint);
		CatalogTable catalogTable = createCatalogTable(sqlCreateTable);

		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlCreateTable.fullTableName());
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

		return new CreateTableOperation(
			identifier,
			catalogTable,
			sqlCreateTable.isIfNotExists(),
			sqlCreateTable.isTemporary());
	}

	private CatalogTable createCatalogTable(SqlCreateTable sqlCreateTable) {

		TableSchema sourceTableSchema;
		final List<String> sourcePartitionKeys;
		final List<SqlTableLike.SqlTableLikeOption> likeOptions;
		final Map<String, String> sourceProperties;
		if (sqlCreateTable.getTableLike().isPresent()) {
			SqlTableLike sqlTableLike = sqlCreateTable.getTableLike().get();
			CatalogTable table = lookupLikeSourceTable(sqlTableLike);
			sourceTableSchema = table.getSchema();
			sourcePartitionKeys = table.getPartitionKeys();
			likeOptions = sqlTableLike.getOptions();
			sourceProperties = table.getProperties();
		} else {
			sourceTableSchema = TableSchema.builder().build();
			sourcePartitionKeys = Collections.emptyList();
			likeOptions = Collections.emptyList();
			sourceProperties = Collections.emptyMap();
		}
		Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies =
			mergeTableLikeUtil.computeMergingStrategies(likeOptions);

		Map<String, String> mergedOptions = mergeOptions(sqlCreateTable, sourceProperties, mergingStrategies);

		Optional<TableSchema> inferredTableSchema = inferTableSchema(mergedOptions);

		Optional<SqlTableConstraint> primaryKey = sqlCreateTable.getFullConstraints()
			.stream()
			.filter(SqlTableConstraint::isPrimaryKey)
			.findAny();

		boolean removeNonInferredPhysicalColumns = false;
		if (inferredTableSchema.isPresent()) {
			removeNonInferredPhysicalColumns = true;
			String connector = mergedOptions.get(CONNECTOR.key());
			String format = mergedOptions.get(FORMAT.key());
			if (("abase".equals(connector) || "redis".equals(connector)) && primaryKey.isPresent()) {
				// This is a special case which the primary key column should be remained and won't be
				// involved in de/serialize schema.
				// todo: remove this trick logic.
				removeNonInferredPhysicalColumns = false;
			} else if ("binlog".equals(format) || "pb_binlog_drc".equals(format)) {
				// We should keep all physical columns in source tables
				// when format is 'binlog' or 'pb_binlog_drc'.
				removeNonInferredPhysicalColumns = false;
			}
		}

		TableSchema mergedSchema = mergeTableLikeUtil.mergeTables(
			mergingStrategies,
			sourceTableSchema,
			sqlCreateTable.getColumnList().getList(),
			sqlCreateTable.getWatermark().map(Collections::singletonList).orElseGet(Collections::emptyList),
			primaryKey.orElse(null),
			inferredTableSchema.orElse(null),
			removeNonInferredPhysicalColumns
		);

		List<String> partitionKeys = mergePartitions(
			sourcePartitionKeys,
			sqlCreateTable.getPartitionKeyList(),
			mergingStrategies
		);

		verifyPartitioningColumnsExist(mergedSchema, partitionKeys);

		String tableComment = sqlCreateTable.getComment()
			.map(comment -> comment.getNlsString().getValue())
			.orElse(null);

		return new CatalogTableImpl(mergedSchema,
			partitionKeys,
			mergedOptions,
			tableComment);
	}

	private Optional<TableSchema> inferTableSchema(Map<String, String> options) {
		String format = options.get(FORMAT.key());
		DeserializationFormatFactory deserializationFormatFactory =
			FactoryUtil.discoverOptionalFactory(
				Thread.currentThread().getContextClassLoader(),
				DeserializationFormatFactory.class,
				format).orElse(null);

		if ((deserializationFormatFactory instanceof TableSchemaInferrable)) {
			TableSchemaInferrable tableSchemaInferrable =
				(TableSchemaInferrable) deserializationFormatFactory;
			return tableSchemaInferrable.getOptionalTableSchema(options);
		}

		SerializationFormatFactory serializationFormatFactory =
			FactoryUtil.discoverOptionalFactory(
				Thread.currentThread().getContextClassLoader(),
				SerializationFormatFactory.class,
				format).orElse(null);

		if ((serializationFormatFactory instanceof TableSchemaInferrable)) {
			TableSchemaInferrable tableSchemaInferrable =
				(TableSchemaInferrable) serializationFormatFactory;
			return tableSchemaInferrable.getOptionalTableSchema(options);
		}

		FileSystemFormatFactory fileSystemFormatFactory =
			FactoryUtil.discoverOptionalFactory(
				Thread.currentThread().getContextClassLoader(),
				FileSystemFormatFactory.class,
				format).orElse(null);

		if ((fileSystemFormatFactory instanceof TableSchemaInferrable)) {
			TableSchemaInferrable tableSchemaInferrable =
				(TableSchemaInferrable) fileSystemFormatFactory;
			return tableSchemaInferrable.getOptionalTableSchema(options);
		}

		// for rpc factory.
		if (options.get(CONNECTOR.key()).equals("rpc")) {
			DynamicTableSourceFactory dynamicTableSourceFactory =
				FactoryUtil.discoverOptionalFactory(
					Thread.currentThread().getContextClassLoader(),
					DynamicTableSourceFactory.class,
					"rpc").orElse(null);
			if ((dynamicTableSourceFactory instanceof TableSchemaInferrable)) {
				TableSchemaInferrable tableSchemaInferrable =
					(TableSchemaInferrable) dynamicTableSourceFactory;
				return tableSchemaInferrable.getOptionalTableSchema(options);
			}
		}

		return Optional.empty();
	}

	private CatalogTable lookupLikeSourceTable(SqlTableLike sqlTableLike) {
		UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sqlTableLike.fullTableName());
		ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
		CatalogManager.TableLookupResult lookupResult = catalogManager.getTable(identifier)
			.orElseThrow(() -> new ValidationException(String.format(
				"Source table '%s' of the LIKE clause not found in the catalog, at %s",
				identifier,
				sqlTableLike.getSourceTable().getParserPosition())));
		if (!(lookupResult.getTable() instanceof CatalogTable)) {
			throw new ValidationException(String.format(
				"Source table '%s' of the LIKE clause can not be a VIEW, at %s",
				identifier,
				sqlTableLike.getSourceTable().getParserPosition()));
		}
		return (CatalogTable) lookupResult.getTable();
	}

	private void verifyPartitioningColumnsExist(TableSchema mergedSchema, List<String> partitionKeys) {
		for (String partitionKey : partitionKeys) {
			if (!mergedSchema.getTableColumn(partitionKey).isPresent()) {
				throw new ValidationException(
					String.format(
						"Partition column '%s' not defined in the table schema. Available columns: [%s]",
						partitionKey,
						Arrays.stream(mergedSchema.getFieldNames()).collect(Collectors.joining("', '", "'", "'"))
					));
			}
		}
	}

	private List<String> mergePartitions(
		List<String> sourcePartitionKeys,
		SqlNodeList derivedPartitionKeys,
		Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies) {
		// set partition key
		return mergeTableLikeUtil.mergePartitions(
			mergingStrategies.get(SqlTableLike.FeatureOption.PARTITIONS),
			sourcePartitionKeys,
			derivedPartitionKeys
				.getList()
				.stream()
				.map(p -> ((SqlIdentifier) p).getSimple())
				.collect(Collectors.toList()));
	}

	private Map<String, String> mergeOptions(
		SqlCreateTable sqlCreateTable,
		Map<String, String> sourceProperties,
		Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies) {
		// set with properties
		Map<String, String> properties = new HashMap<>();
		sqlCreateTable.getPropertyList().getList().forEach(p ->
			properties.put(((SqlTableOption) p).getKeyString(), ((SqlTableOption) p).getValueString()));
		return mergeTableLikeUtil.mergeOptions(
			mergingStrategies.get(SqlTableLike.FeatureOption.OPTIONS),
			sourceProperties,
			properties
		);
	}
}
