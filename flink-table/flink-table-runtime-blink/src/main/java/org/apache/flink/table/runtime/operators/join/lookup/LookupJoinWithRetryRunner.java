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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The join runner to lookup the dimension table with retry.
 */
public class LookupJoinWithRetryRunner extends LookupJoinRunner {
	private static final long serialVersionUID = 1L;
	private static final long NO_CACHE_STATE = -1L;

	private final BaseRowTypeInfo leftTypeInfo;
	private transient ListState<Tuple2<BaseRow, Long>> listState;
	private long minTriggerTimestamp = 0L;
	private long latencyMs;
	public LookupJoinWithRetryRunner(
			GeneratedFunction<FlatMapFunction<BaseRow, BaseRow>> generatedFetcher,
			GeneratedCollector<TableFunctionCollector<BaseRow>> generatedCollector,
			BaseRowTypeInfo leftRowTypeInfo,
			boolean isLeftOuterJoin,
			int tableFieldsCount,
			long latencyMs) {
		super(generatedFetcher, generatedCollector, isLeftOuterJoin, tableFieldsCount);
		this.latencyMs = latencyMs;
		this.leftTypeInfo = leftRowTypeInfo;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		TupleTypeInfo<Tuple2<BaseRow, Long>> tupleTypeInfo =
			new TupleTypeInfo<>(leftTypeInfo, BasicTypeInfo.LONG_TYPE_INFO);
		if (getRuntimeContext() instanceof StreamingRuntimeContext) {
			listState = ((StreamingRuntimeContext) getRuntimeContext())
				.getOperatorListState(new ListStateDescriptor<>("LookUpJoinList", tupleTypeInfo));
		} else {
			throw new RuntimeException("Lookup join with retry only support streaming runtime context.");
		}
	}

	@Override
	public void processElement(BaseRow in, Context ctx, Collector<BaseRow> out) throws Exception {
		processCachedRows(out);
		boolean isCollected = doJoin(in, out);
		if (!isCollected) {
			long nextTime = getNextTimestamp();
			if (minTriggerTimestamp == NO_CACHE_STATE) {
				minTriggerTimestamp = nextTime;
			}
			listState.add(new Tuple2<>(in, nextTime));
		}
	}

	// this stream may not a keyedStream so we can't use timeService to register timer
	private void processCachedRows(Collector<BaseRow> out) throws Exception {
		long curTimestamp = System.currentTimeMillis();
		if (minTriggerTimestamp == NO_CACHE_STATE || minTriggerTimestamp > curTimestamp) {
			return;
		}
		Iterable<Tuple2<BaseRow, Long>> tuple2Iterable = listState.get();
		if (tuple2Iterable == null) {
			minTriggerTimestamp = NO_CACHE_STATE;
			return;
		}
		List<Tuple2<BaseRow, Long>> remainingData = new ArrayList<>();
		Iterator<Tuple2<BaseRow, Long>> tuple2Iterator = tuple2Iterable.iterator();
		while (tuple2Iterator.hasNext()) {
			Tuple2<BaseRow, Long> tuple2 = tuple2Iterator.next();
			if (tuple2.f1 <= curTimestamp) {
				super.processElement(tuple2.f0, null, out);
				tuple2Iterator.remove();
			} else {
				minTriggerTimestamp = Math.min(minTriggerTimestamp, tuple2.f1);
				remainingData.add(tuple2);
			}
		}

		listState.update(remainingData);
	}

	private long getNextTimestamp() {
		return System.currentTimeMillis() / 1000 * 1000 + latencyMs;
	}
}
