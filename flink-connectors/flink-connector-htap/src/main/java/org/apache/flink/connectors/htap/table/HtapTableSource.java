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

package org.apache.flink.connectors.htap.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.htap.batch.HtapRowInputFormat;
import org.apache.flink.connectors.htap.connector.HtapAggregateInfo;
import org.apache.flink.connectors.htap.connector.HtapFilterInfo;
import org.apache.flink.connectors.htap.connector.HtapTableInfo;
import org.apache.flink.connectors.htap.connector.reader.HtapReaderConfig;
import org.apache.flink.connectors.htap.table.utils.HtapAggregateUtils;
import org.apache.flink.connectors.htap.table.utils.HtapAggregateUtils.FlinkAggregateFunction;
import org.apache.flink.connectors.htap.table.utils.HtapMetaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.sources.AggregatableTableSource;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.LimitableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.bytedance.bytehtap.Commons.AggregateType;
import com.bytedance.htap.client.HtapMetaClient;
import com.bytedance.htap.meta.HtapTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

import static org.apache.flink.connectors.htap.table.utils.HtapTableUtils.toHtapFilterInfo;

/**
 * HtapTableSource.
 */
public class HtapTableSource implements StreamTableSource<Row>, LimitableTableSource<Row>,
		ProjectableTableSource<Row>,
		FilterableTableSource<Row>,
		AggregatableTableSource<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(HtapTableSource.class);

	private final HtapReaderConfig readerConfig;
	private final HtapTableInfo tableInfo;
	private final TableSchema flinkSchema;
	private final String[] projectedFields;
	private final String[] groupByFields;
	private final List<HtapAggregateInfo> aggregates;
	private final List<FlinkAggregateFunction> aggregateFunctions;
	private final DataType outputDataType;
	private final List<HtapFilterInfo> predicates;
	private boolean isFilterPushedDown = false;
	private boolean isLimitPushedDown = false;
	private boolean isAggregatePushedDown = false;

	public HtapTableSource(
			HtapReaderConfig readerConfig,
			HtapTableInfo tableInfo,
			TableSchema flinkSchema,
			List<HtapFilterInfo> predicates,
			String[] projectedFields,
			String[] groupByFields,
			List<HtapAggregateInfo> aggregates,
			List<FlinkAggregateFunction> aggregateFunctions,
			DataType outputDataType,
			boolean isLimitPushedDown) {
		this.readerConfig = readerConfig;
		this.tableInfo = tableInfo;
		this.flinkSchema = flinkSchema;
		this.predicates = predicates;
		this.projectedFields = projectedFields;
		this.groupByFields = groupByFields;
		this.aggregates = aggregates;
		this.aggregateFunctions = aggregateFunctions;
		this.outputDataType = outputDataType;
		if (predicates != null && predicates.size() != 0) {
			this.isFilterPushedDown = true;
		}
		if (aggregates != null && aggregates.size() != 0) {
			this.isAggregatePushedDown = true;
		}
		this.isLimitPushedDown = isLimitPushedDown;
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		// TM should not access meta service, so we get metadata here and propagate it out.
		HtapMetaClient metaClient = HtapMetaUtils.getMetaClient(
			readerConfig.getMetaHosts(), readerConfig.getMetaPort());
		HtapTable table = readerConfig.getCheckPointLSN() == -1L ?
			metaClient.getTable(tableInfo.getName()) :
			metaClient.getTable(tableInfo.getName(), readerConfig.getCheckPointLSN());
		HtapRowInputFormat inputFormat = new HtapRowInputFormat(readerConfig, table,
			predicates == null ? Collections.emptyList() : predicates,
			projectedFields == null ? Collections.emptyList() : Lists.newArrayList(projectedFields),
			aggregates == null ? Collections.emptyList() : aggregates,
			groupByFields == null ? Collections.emptyList() : Lists.newArrayList(groupByFields),
			aggregateFunctions == null ? Collections.emptyList() : aggregateFunctions,
			outputDataType);
		return env.createInput(inputFormat,
			(TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType()))
			.name(explainSource());
	}

	@Override
	public TableSchema getTableSchema() {
		return flinkSchema;
	}

	@Override
	public DataType getProducedDataType() {
		if (outputDataType != null) {
			// outputDataType is not null, which means aggregate pushdown works.
			// outputDataType represents the output dataType of LocalAggregate operation.
			return outputDataType;
		} else if (projectedFields != null) {
			// projection works
			DataTypes.Field[] fields = new DataTypes.Field[projectedFields.length];
			for (int i = 0; i < fields.length; i++) {
				String fieldName = projectedFields[i];
				fields[i] = DataTypes.FIELD(
					fieldName,
					flinkSchema
						.getTableColumn(fieldName)
						.get()
						.getType()
				);
			}
			return DataTypes.ROW(fields);
		} else {
			return flinkSchema.toRowDataType();
		}
	}

	@Override
	public boolean isLimitPushedDown() {
		return this.isLimitPushedDown;
	}

	@Override
	public TableSource<Row> applyLimit(long l) {
		LOG.info("HtapTableSource[{}] apply limit: {}", tableInfo.getName(), l);
		readerConfig.setRowLimit((int) l);
		return new HtapTableSource(readerConfig, tableInfo, flinkSchema, predicates,
			projectedFields, groupByFields, aggregates, aggregateFunctions, outputDataType, true);
	}

	@Override
	public TableSource<Row> projectFields(int[] ints) {
		String[] fieldNames = getColumnNamesByIndexList(ints);
		return new HtapTableSource(readerConfig, tableInfo, flinkSchema, predicates, fieldNames,
			groupByFields, aggregates, aggregateFunctions, outputDataType, isLimitPushedDown);
	}

	@Override
	public boolean isFilterPushedDown() {
		return this.isFilterPushedDown;
	}

	@Override
	public TableSource<Row> applyPredicate(List<Expression> predicates) {
		List<HtapFilterInfo> htapPredicates = new ArrayList<>();
		ListIterator<Expression> predicatesIter = predicates.listIterator();
		while (predicatesIter.hasNext()) {
			Expression predicate = predicatesIter.next();
			Optional<HtapFilterInfo> htapPred = toHtapFilterInfo(predicate);
			if (htapPred != null && htapPred.isPresent()) {
				LOG.debug("Predicate [{}] converted into HtapFilterInfo and pushed into " +
					"HtapTable [{}].", predicate, tableInfo.getName());
					htapPredicates.add(htapPred.get());
				predicatesIter.remove();
			} else {
				LOG.debug("Predicate [{}] could not be pushed into HtapFilterInfo for HtapTable [{}].",
					predicate, tableInfo.getName());
			}
		}
		LOG.info("applied predicates: flink predicates: [{}], pushed predicates: [{}]",
			predicates, htapPredicates);
		return new HtapTableSource(readerConfig, tableInfo, flinkSchema, htapPredicates,
			projectedFields, groupByFields, aggregates, aggregateFunctions, outputDataType,
			isLimitPushedDown);
	}

	@Override
	public TableSource<Row> applyAggregates(
			List<FunctionDefinition> aggregateFunctions,
			List<int[]> aggregateFields, int[] groupSet,
			DataType aggOutputDataType) {
		List<FlinkAggregateFunction> validAggFunctions = new ArrayList<>(aggregateFunctions.size());
		// groupBy columns
		String[] groupByFields = getColumnNamesByIndexList(groupSet);
		// aggregates
		List<HtapAggregateInfo> aggregates = new ArrayList<>(aggregateFunctions.size());
		ListIterator<FunctionDefinition> iterator = aggregateFunctions.listIterator();
		int cur = 0;
		RowType rowType = (RowType) flinkSchema.toRowDataType().getLogicalType();
		while (iterator.hasNext() && cur < aggregateFields.size()) {
			FunctionDefinition aggFunction = iterator.next();
			int[] aggFields = aggregateFields.get(cur++);
			if (HtapAggregateUtils.supportAggregatePushDown(aggFunction, aggFields, rowType)) {
				String[] aggColumns = getColumnNamesByIndexList(aggFields);
				AggregateType aggregateType = HtapAggregateUtils.toAggregateType(aggFunction);
				aggregates.add(new HtapAggregateInfo(aggColumns, aggregateType));
				validAggFunctions.add(HtapAggregateUtils.toFlinkAggregateFunction(aggFunction));
				iterator.remove();
			}
		}
		return new HtapTableSource(readerConfig, tableInfo, flinkSchema, predicates,
			projectedFields, groupByFields, aggregates, validAggFunctions, aggOutputDataType,
			isLimitPushedDown);
	}

	@Override
	public boolean isAggregatePushedDown() {
		return isAggregatePushedDown;
	}

	@Override
	public String explainSource() {
		return "HtapTableSource[schema=" + Arrays.toString(getTableSchema().getFieldNames()) +
			", filter=" + predicateString() +
			", isFilterPushedDown=" + isFilterPushedDown +
			", limit=" + readerConfig.getRowLimit() +
			", isLimitPushedDown=" + isLimitPushedDown +
			", isAggregatePushedDown=" + isAggregatePushedDown +
			(groupByFields != null ? ", groupByFields=" + Arrays.toString(groupByFields) : "") +
			(aggregates != null ? ", aggregates=" + aggregates : "") +
			(aggregateFunctions != null ? ", aggregateFunctions=" + aggregateFunctions : "") +
			(outputDataType != null ? ", outputDataType=" + outputDataType : "") +
			(projectedFields != null ? ", projectFields=" + Arrays.toString(projectedFields) + "]" : "]");

	}

	private String predicateString() {
		if (predicates == null || predicates.size() == 0) {
			return "No predicates push down";
		} else {
			return "AND(" + predicates + ")";
		}
	}

	/**
	 * Convert column index to column names bases on {@link RowType}.
	 */
	private String[] getColumnNamesByIndexList(int[] indexes, RowType rowType) {
		String[] fieldNames = new String[indexes.length];
		List<String> prevFieldNames = rowType.getFieldNames();
		for (int i = 0; i < indexes.length; i++) {
			fieldNames[i] = prevFieldNames.get(indexes[i]);
		}
		return fieldNames;
	}

	/**
	 * Convert column index to column names bases on columnNames array.
	 */
	private String[] getColumnNamesByIndexList(int[] indexes, String[] columnNames) {
		String[] fieldNames = new String[indexes.length];
		for (int i = 0; i < indexes.length; i++) {
			fieldNames[i] = columnNames[indexes[i]];
		}
		return fieldNames;
	}

	/**
	 * Convert column index to column names.
	 */
	private String[] getColumnNamesByIndexList(int[] indexes) {
		if (projectedFields == null) {
			RowType producedDataType = (RowType) flinkSchema.toRowDataType().getLogicalType();
			return getColumnNamesByIndexList(indexes, producedDataType);
		} else {
			return getColumnNamesByIndexList(indexes, projectedFields);
		}
	}
}
