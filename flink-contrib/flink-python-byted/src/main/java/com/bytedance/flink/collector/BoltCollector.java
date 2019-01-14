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

package com.bytedance.flink.collector;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bolt collector.
 */
public class BoltCollector<OUT> extends BaseCollector<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(BaseCollector.class);
	private final Collector<OUT> timestampedCollector;

	public BoltCollector(int numberOfAttributes, Collector<OUT> timestampedCollector)
		throws IllegalAccessException, InstantiationException {
		super(numberOfAttributes, Tuple.getTupleClass(numberOfAttributes).newInstance());
		this.timestampedCollector = timestampedCollector;
	}

	@Override
	public void doEmit(OUT tuple) {
		this.timestampedCollector.collect(tuple);
	}

	@Override
	public void close() {
		this.timestampedCollector.close();
	}

	public void setTimestamp(StreamRecord<?> timestampBase) {
		if (this.timestampedCollector instanceof TimestampedCollector) {
			((TimestampedCollector<OUT>) this.timestampedCollector).setTimestamp(timestampBase);
		}
	}
}
