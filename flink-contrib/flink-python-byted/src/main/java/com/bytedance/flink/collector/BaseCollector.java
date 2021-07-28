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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Base collector class.
 */
public abstract class BaseCollector<OUT> implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(BaseCollector.class);

	public int numberOfAttributes;
	private Tuple outputTuple;

	public BaseCollector(int numberOfAttributes, Tuple outputTuple) {
		this.numberOfAttributes = numberOfAttributes;
		this.outputTuple = outputTuple;
	}

	public void emit(List<Object> tuple) {
		this.tansformAndEmit(tuple);
	}

	public abstract void doEmit(OUT tuple);

	public final void tansformAndEmit(List<Object> tuple) {
		int numAtt = numberOfAttributes;
		if (numberOfAttributes < 0) {
			numAtt = 1;
		}
		if (numAtt >= 0) {
			assert tuple.size() == numAtt;

			for (int i = 0; i < numAtt; ++i) {
				outputTuple.setField(tuple.get(i), i);
			}
			this.doEmit((OUT) outputTuple);
		} else {
			assert tuple.size() == 1;
			this.doEmit((OUT) tuple.get(0));
		}
	}

	public abstract void close();
}
