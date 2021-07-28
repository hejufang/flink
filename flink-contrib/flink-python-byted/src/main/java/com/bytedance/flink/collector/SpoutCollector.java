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
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spout collector.
 */
public class SpoutCollector<OUT> extends BaseCollector<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(SpoutCollector.class);
	private final SourceFunction.SourceContext<OUT> sourceContext;

	public SpoutCollector(int numberOfAttributes, SourceFunction.SourceContext<OUT> sourceContext)
		throws IllegalAccessException, InstantiationException {
		super(numberOfAttributes, Tuple.getTupleClass(numberOfAttributes).newInstance());
		this.sourceContext = sourceContext;
	}

	@Override
	public void doEmit(OUT tuple) {
		this.sourceContext.collect(tuple);
	}

	@Override
	public void close() {
		this.sourceContext.close();
	}
}
