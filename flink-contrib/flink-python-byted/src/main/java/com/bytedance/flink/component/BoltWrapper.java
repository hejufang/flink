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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.runtime.pyflink.PYFlinkProgressCache;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.bytedance.flink.collector.BoltCollector;
import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.pojo.Schema;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A bolt wrapper is a Flink operator which wraps a ShellBolt.
 */
public class BoltWrapper<IN, OUT> extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT> {
	protected static final Logger LOG = LoggerFactory.getLogger(BoltWrapper.class);

	private Bolt bolt;
	private String name;
	private Integer numberOfOutputAttribute;
	private BoltCollector boltCollector;
	private Schema outputSchema;
	/**
	 * Number of this parallel subtask, The numbering starts from 0 and goes up to parallelism-1.
	 */
	private Integer subTaskId;
	private volatile boolean localFailover;
	private volatile PyBoltProcess boltProcess;

	public BoltWrapper(Bolt bolt, String name, Schema outputSchema) {
		this.bolt = bolt;
		this.name = name;
		this.numberOfOutputAttribute = outputSchema.size();
		this.outputSchema = outputSchema;
	}

	@Override
	public void processElement(StreamRecord<IN> streamRecord) throws Exception {
		boltCollector.setTimestamp(streamRecord);
		IN value = streamRecord.getValue();
		if (value instanceof Tuple) {
			this.bolt.execute((Tuple) value);
		} else {
			this.bolt.execute(new Tuple1<>(value));
		}
	}

	@Override
	public void open() throws Exception {
		super.open();
		subTaskId = getRuntimeContext().getIndexOfThisSubtask();
		RuntimeConfig runtimeConfig = (RuntimeConfig)
			getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
		runtimeConfig.setSubTaskId(subTaskId);
		runtimeConfig.setTaskName(name);
		EnvironmentInitUtils.prepareLocalDir(runtimeConfig, bolt);

		TimestampedCollector<OUT> flinkCollector = new TimestampedCollector<>(this.output);

		boltCollector = new BoltCollector<>(numberOfOutputAttribute, flinkCollector);
		localFailover = (boolean) runtimeConfig.getOrDefault(Constants.LOCAL_FAILOVER, false);

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
					LOG.warn("attach successed bolt, {}", boltProgressKey);
				} catch (Exception e) {
					LOG.warn("attach failed bolt, " + boltProgressKey, e);
				}
			} else {
				LOG.warn("attach init bolt, {}", boltProgressKey);
			}
		}

		if (!attached) {
			bolt.open(runtimeConfig, boltCollector);
			if (localFailover) {
				boltProcess = new PyBoltProcess(runtimeConfig, name, subTaskId, bolt);
				PYFlinkProgressCache.getInstance().put(boltProgressKey, boltProcess);
				LOG.info("cached bolt progress, name:{}, taskId:{}", name, subTaskId);
			}
		}
	}

	@Override
	public void dispose() throws Exception {
		LOG.info("Try to dispose bolt {}-{}", name, subTaskId);
		super.dispose();
		if (bolt != null) {
			if (localFailover) {
				LOG.info("Suspend bolt progress");
				((ShellBolt) bolt).suspend();
				if (boltProcess != null) {
					LOG.info("Mark bolt as unused.");
					boltProcess.markUnUse();
				}
			} else {
				bolt.close();
				if (boltProcess != null) {
					boltProcess.clear();
				}
			}
		}
	}
}
