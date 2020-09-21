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
import org.apache.flink.connectors.htap.connector.HtapFilterInfo;
import org.apache.flink.connectors.htap.connector.HtapTableInfo;
import org.apache.flink.connectors.htap.connector.reader.HtapReaderConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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
		FilterableTableSource<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(HtapTableSource.class);

	private final HtapReaderConfig readerConfig;
	private final HtapTableInfo tableInfo;
	private final TableSchema flinkSchema;
	private final String[] projectedFields;

	@Nullable
	private final List<HtapFilterInfo> predicates;
	private boolean isFilterPushedDown = false;
	private boolean isLimitPushedDown = false;

	public HtapTableSource(
			HtapReaderConfig readerConfig,
			HtapTableInfo tableInfo,
			TableSchema flinkSchema,
			List<HtapFilterInfo> predicates,
			String[] projectedFields,
			boolean isLimitPushedDown) {
		this.readerConfig = readerConfig;
		this.tableInfo = tableInfo;
		this.flinkSchema = flinkSchema;
		this.predicates = predicates;
		this.projectedFields = projectedFields;
		// TODO: predicated & limit push down is under development in Htap store,
		//  we push down the expression and recompute in flink side now.
		// if (predicates != null && predicates.size() != 0) {
			// this.isFilterPushedDown = true;
		// }
		// this.isLimitPushedDown = isLimitPushedDown;
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		HtapRowInputFormat inputFormat = new HtapRowInputFormat(readerConfig, tableInfo,
			predicates == null ? Collections.emptyList() : predicates,
			projectedFields == null ? null : Lists.newArrayList(projectedFields));
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
		if (projectedFields == null) {
			return flinkSchema.toRowDataType();
		} else {
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
		}
	}

	@Override
	public boolean isLimitPushedDown() {
		return this.isLimitPushedDown;
	}

	@Override
	public TableSource<Row> applyLimit(long l) {
		readerConfig.setRowLimit((int) l);
		return new HtapTableSource(readerConfig, tableInfo, flinkSchema, predicates, projectedFields,
			true);
	}

	@Override
	public TableSource<Row> projectFields(int[] ints) {
		String[] fieldNames = new String[ints.length];
		RowType producedDataType = (RowType) getProducedDataType().getLogicalType();
		List<String> prevFieldNames = producedDataType.getFieldNames();
		for (int i = 0; i < ints.length; i++) {
			fieldNames[i] = prevFieldNames.get(ints[i]);
		}
		return new HtapTableSource(readerConfig, tableInfo, flinkSchema, predicates, fieldNames,
			isLimitPushedDown);
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
				// TODO: predicated push down is under development in Htap store, should keep
				//  expressions in flink side
				// predicatesIter.remove();
			} else {
				LOG.debug("Predicate [{}] could not be pushed into HtapFilterInfo for HtapTable [{}].",
					predicate, tableInfo.getName());
			}
		}
		LOG.info("applied predicates: flink predicates: [{}], pushed predicates: [{}]",
			predicates, htapPredicates);
		return new HtapTableSource(readerConfig, tableInfo, flinkSchema, htapPredicates, projectedFields,
			isLimitPushedDown);
	}

	@Override
	public String explainSource() {
		return "HtapTableSource[schema=" + Arrays.toString(getTableSchema().getFieldNames()) +
			", filter=" + predicateString() + ", isFilterPushedDown=" + isFilterPushedDown +
			", limit=" + readerConfig.getRowLimit() +
			", isLimitPushedDown=" + isLimitPushedDown +
			(projectedFields != null ? ", projectFields=" + Arrays.toString(projectedFields) + "]" : "]");
	}

	private String predicateString() {
		if (predicates == null || predicates.size() == 0) {
			return "No predicates push down";
		} else {
			return "AND(" + predicates + ")";
		}
	}
}
