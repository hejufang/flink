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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.StatefulFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateRegistry;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The join runner to lookup the dimension table with retry.
 */
public class LookupJoinWithRetryRunner extends LookupJoinRunner
		implements StatefulFunction {
	private static final long serialVersionUID = 1L;
	private static final long NO_CACHE_STATE = Long.MAX_VALUE;

	private final RowDataTypeInfo leftTypeInfo;
	private final long latencyMs;
	private final int laterRetryTimes;
	private transient ListState<Tuple3<RowData, Long, Integer>> listState;
	private transient long minTriggerTimestamp = NO_CACHE_STATE;

	public LookupJoinWithRetryRunner(
			GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
			GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector,
			RowDataTypeInfo leftRowTypeInfo,
			boolean isLeftOuterJoin,
			int tableFieldsCount,
			long latencyMs,
			int laterRetryTimes) {
		super(generatedFetcher, generatedCollector, isLeftOuterJoin, tableFieldsCount);
		this.latencyMs = latencyMs;
		this.leftTypeInfo = leftRowTypeInfo;
		this.laterRetryTimes = laterRetryTimes;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		Iterable<Tuple3<RowData, Long, Integer>> tuple3Iterable = listState.get();
		if (tuple3Iterable == null || !tuple3Iterable.iterator().hasNext()) {
			minTriggerTimestamp = NO_CACHE_STATE;
		} else {
			minTriggerTimestamp = tuple3Iterable.iterator().next().f1;
		}
	}

	@Override
	public void registerState(StateRegistry stateRegistry) throws Exception {
		TupleTypeInfo<Tuple3<RowData, Long, Integer>> tupleTypeInfo =
			new TupleTypeInfo<>(leftTypeInfo, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		listState = stateRegistry
			.getOperatorListState(new ListStateDescriptor<>("LookUpJoinList", tupleTypeInfo));
	}

	@Override
	public void processElement(RowData in, Context ctx, Collector<RowData> out) throws Exception {
		long curTimestamp = ctx.timerService().currentProcessingTime();
		processCachedRows(out, curTimestamp);
		processElement(in, out,
			getNextTimestamp(ctx.timerService().currentProcessingTime()), laterRetryTimes, listState::add);
	}

	private void processElement(
			RowData in,
			Collector<RowData> out,
			long nextTime,
			int leftTimes,
			ThrowingConsumer<Tuple3<RowData, Long, Integer>, Exception> consumer) throws Exception {
		boolean isCollected = doJoin(in, out);
		if (!isCollected) {
			consumer.accept(new Tuple3<>(in, nextTime, leftTimes));
			minTriggerTimestamp = Math.min(nextTime, minTriggerTimestamp);
		}
	}

	// this stream may not a keyedStream so we can't use timeService to register timer
	private void processCachedRows(Collector<RowData> out, long curTimestamp) throws Exception {
		if (minTriggerTimestamp > curTimestamp) {
			return;
		}
		Iterable<Tuple3<RowData, Long, Integer>> tuple3Iterable = listState.get();
		if (tuple3Iterable == null) {
			minTriggerTimestamp = NO_CACHE_STATE;
			return;
		}
		long nextTimeStamp = getNextTimestamp(curTimestamp);
		List<Tuple3<RowData, Long, Integer>> remainingData = new ArrayList<>();
		Iterator<Tuple3<RowData, Long, Integer>> tuple3Iterator = tuple3Iterable.iterator();
		while (tuple3Iterator.hasNext()) {
			Tuple3<RowData, Long, Integer> tuple3 = tuple3Iterator.next();
			if (tuple3.f1 <= curTimestamp) {
				int leftRetryTimes = tuple3.f2 - 1;
				if (leftRetryTimes == 0) {
					super.processElement(tuple3.f0, null, out);
				} else {
					processElement(tuple3.f0, out, nextTimeStamp, leftRetryTimes, remainingData::add);
				}
			} else {
				minTriggerTimestamp = Math.min(minTriggerTimestamp, tuple3.f1);
				remainingData.add(tuple3);
			}
		}

		listState.update(remainingData);
		if (remainingData.size() == 0) {
			minTriggerTimestamp = NO_CACHE_STATE;
		}
	}

	private long getNextTimestamp(long curTimestamp) {
		// Reduce access of state
		return curTimestamp / 1000 * 1000 + latencyMs;
	}
}
