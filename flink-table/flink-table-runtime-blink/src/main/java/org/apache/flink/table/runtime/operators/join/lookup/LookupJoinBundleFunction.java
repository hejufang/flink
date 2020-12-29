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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.MiniBatchTableFunction;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.bundle.ListBundleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Similar to {@link LookupJoinRunner}, {@link LookupJoinBundleFunction} process join lookup logic with codegen
 * {@code generatedKeyConverter} and {@code generatedCollector}. The difference is that it runs in a batch fashion.
 */
public class LookupJoinBundleFunction implements ListBundleFunction<RowData, RowData> {
	private static final long serialVersionUID = 1L;

	//map a complete row to object array of keys
	private final GeneratedFunction<MapFunction<RowData, Object[]>> generatedKeyConverter;
	//collect joined row result, combine input row and joined result row together
	private final GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector;
	private final RowType inputRowType;
	private transient MapFunction<RowData, Object[]> keyConverter;
	protected transient TableFunctionCollector<RowData> collector;
	private final MiniBatchTableFunction<RowData> tableFunction;
	private final boolean isLeftOuterJoin;
	private final int tableFieldsCount;

	private transient GenericRowData nullRow;
	private transient JoinedRowData outRow;
	private transient Set<Integer> nullIndices;

	private boolean isObjectUse = false;
	//in object reuse mode, we need to copy the row data when adding it to bundle
	private transient RowDataSerializer inputRowDataSerializer;

	public LookupJoinBundleFunction(
			GeneratedFunction<MapFunction<RowData, Object[]>> generatedKeyConverter,
			GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector,
			MiniBatchTableFunction<RowData> tableFunction,
			RowType inputRowType,
			boolean isLeftOuterJoin,
			int tableFieldsCount) {
		this.generatedKeyConverter = generatedKeyConverter;
		this.generatedCollector = generatedCollector;
		this.inputRowType = inputRowType;
		this.isLeftOuterJoin = isLeftOuterJoin;
		this.tableFieldsCount = tableFieldsCount;
		this.tableFunction = tableFunction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		ExecutionConfig executionConfig = ctx.getRuntimeContext().getExecutionConfig();

		this.keyConverter = generatedKeyConverter.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
		this.collector = generatedCollector.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());

		FunctionUtils.setFunctionRuntimeContext(collector, ctx.getRuntimeContext());
		FunctionUtils.openFunction(collector, new Configuration());

		FunctionUtils.setFunctionRuntimeContext(keyConverter, ctx.getRuntimeContext());
		FunctionUtils.openFunction(keyConverter, new Configuration());

		tableFunction.open(new FunctionContext(ctx.getRuntimeContext()));

		this.nullRow = new GenericRowData(tableFieldsCount);
		this.outRow = new JoinedRowData();

		this.isObjectUse = executionConfig.isObjectReuseEnabled();
		LogicalType[] inputTableRowChildTypes = inputRowType.getChildren().toArray(new LogicalType[0]);
		this.inputRowDataSerializer = new RowDataSerializer(executionConfig, inputTableRowChildTypes);

		this.nullIndices = new HashSet<>();
	}

	@Override
	public RowData addInput(RowData value) {
		if (isObjectUse) {
			return inputRowDataSerializer.copy(value);
		} else {
			return value;
		}
	}

	public Collector<RowData> getFetcherCollector() {
		return collector;
	}

	@Override
	public void finishBundle(List<RowData> buffer, Collector<RowData> out) throws Exception {
		nullIndices.clear();
		collector.setCollector(out);
		List<Object[]> convertedKeysList = new ArrayList<>();
		for (int i = 0; i < buffer.size(); i++) {
			RowData bufferRow = buffer.get(i);
			Object[] keys = keyConverter.map(bufferRow);
			if (keys == null) {
				// one of the key fields is null
				nullIndices.add(i);
			} else {
				// all key fields are not null
				convertedKeysList.add(keys);
			}
		}
		List<Collection<RowData>> resultRowsList = tableFunction.eval(convertedKeysList);
		if (convertedKeysList.size() != resultRowsList.size()) {
			throw new FlinkRuntimeException("The list size returned by batched eval() in TableFunction is not match " +
				"with input buffer list. This is a connector bug.");
		}
		int validPosition = 0;
		for (int i = 0; i < buffer.size(); i++) {
			RowData inputRowData = buffer.get(i);
			if (nullIndices.contains(i)) {
				//null keys
				if (isLeftOuterJoin) {
					outRow.replace(inputRowData, nullRow);
					outRow.setRowKind(inputRowData.getRowKind());
					out.collect(outRow);
				}
			} else {
				//not null keys
				Collection<RowData> resultRows = resultRowsList.get(validPosition++);
				if (isLeftOuterJoin && (resultRows == null || resultRows.isEmpty())) {
					outRow.replace(inputRowData, nullRow);
					outRow.setRowKind(inputRowData.getRowKind());
					out.collect(outRow);
				} else if (resultRows != null) {
					for (RowData resultRow : resultRows) {
						collector.setInput(inputRowData);
						getFetcherCollector().collect(resultRow);
					}
				}
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (keyConverter != null) {
			FunctionUtils.closeFunction(keyConverter);
		}
		if (collector != null) {
			FunctionUtils.closeFunction(collector);
		}
		if (tableFunction != null) {
			tableFunction.close();
		}
	}
}
