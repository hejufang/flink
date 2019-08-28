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

package com.bytedance.flink.component;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.pyflink.PYFlinkProgressCache;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.bytedance.flink.collector.BoltCollector;
import com.bytedance.flink.collector.NoOutputCollector;
import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.pojo.Schema;
import com.bytedance.flink.utils.CommonUtils;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Batch bolt process is a Flink Process Function.
 */
public class BatchBoltProcess<IN, OUT> extends ProcessFunction<IN, OUT> {
	protected static final Logger LOG = LoggerFactory.getLogger(BatchBoltProcess.class);

	private Bolt bolt;
	private String name;
	private Integer numberOfOutputAttribute;
	private BoltCollector boltCollector;
	private Schema outputSchema;
	private long timeoutMs;
	private TypeInformation<IN> elementTypeInfo;
	private ListState<IN> listState;
	private ValueState<Boolean> isEmptyState;

	/**
	 * Number of this parallel subtask, The numbering starts from 0 and goes up to parallelism-1.
	 */
	private Integer subTaskId;
	private volatile boolean localFailover;
	private volatile PyBoltProcess boltProcess;

	public BatchBoltProcess(Bolt bolt, String name, Schema outputSchema, long timeoutMs,
							TypeInformation<IN> elementTypeInfo) {
		this.bolt = bolt;
		this.name = name;
		this.outputSchema = outputSchema;
		this.timeoutMs = timeoutMs;
		this.elementTypeInfo = elementTypeInfo;
		this.numberOfOutputAttribute = outputSchema.size();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		listState = getRuntimeContext().getListState(new ListStateDescriptor<>(name, elementTypeInfo));
		isEmptyState = getRuntimeContext()
			.getState(new ValueStateDescriptor<>(Constants.EMPTY_STATE_IDENTIFY, Boolean.class));

		subTaskId = getRuntimeContext().getIndexOfThisSubtask();
		RuntimeConfig runtimeConfig = (RuntimeConfig)
			getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		runtimeConfig.setSubTaskId(subTaskId);
		runtimeConfig.setTaskName(name);
		EnvironmentInitUtils.prepareLocalDir(runtimeConfig, bolt);
		localFailover = (boolean) runtimeConfig.getOrDefault(Constants.LOCAL_FAILOVER, false);

		boltCollector = new BoltCollector<>(numberOfOutputAttribute, new NoOutputCollector<OUT>());
		String boltProgressKey = runtimeConfig.getJobName() + "-" + this.name + "-"
			+ runtimeConfig.getSubTaskId();
		boolean attached = false;
		if (localFailover) {
			boltProcess = (PyBoltProcess) PYFlinkProgressCache.getInstance().get(boltProgressKey);
			if (boltProcess != null) {
				try {
					ShellBolt shellBolt = (ShellBolt) boltProcess.getBolt();
					shellBolt.attach(boltCollector);
					this.bolt = shellBolt;
					attached = true;
					boltProcess.markInUse();
					LOG.warn("attach successed batch bolt, {}", boltProgressKey);
				} catch (Exception e) {
					LOG.warn("attach failed batch bolt, " + boltProgressKey, e);
				}
			} else {
				LOG.warn("attach init batch bolt, {}" + boltProgressKey);
			}
		}

		if (!attached) {
			bolt.open(runtimeConfig, boltCollector);
			if (localFailover) {
				boltProcess = new PyBoltProcess(runtimeConfig, name, subTaskId, bolt);
				PYFlinkProgressCache.getInstance().put(boltProgressKey, boltProcess);
				LOG.info("cached batch bolt progress, name:{}, taskId:{}", name, subTaskId);
			}
		}
	}

	@Override
	public void processElement(IN in, Context context, Collector<OUT> collector) throws Exception {
		Boolean isEmptyList = isEmptyState.value();
		if (isEmptyList == null || isEmptyList) {
			long nextTimer = context.timerService().currentProcessingTime() + timeoutMs;
			context.timerService().registerProcessingTimeTimer(nextTimer);
			isEmptyState.update(Boolean.FALSE);
		}
		listState.add(in);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
		Iterable<IN> iterable = listState.get();
		List<Object> list = new ArrayList<>();
		for (IN in : iterable) {
			if (in instanceof Tuple) {
				Tuple t = (Tuple) in;
				list.add(CommonUtils.tupleToList(t));
			} else {
				throw new RuntimeException("Process messages must be Tuple value.");
			}
		}
		listState.clear();
		isEmptyState.clear();
		bolt.execute(list);
	}
}
