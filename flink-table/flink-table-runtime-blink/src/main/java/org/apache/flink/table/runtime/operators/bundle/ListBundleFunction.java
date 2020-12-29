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

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * This function is similar to MapBundleFunction but it is designed to produce list bundle data.
 * @param <IN> The input type
 * @param <OUT> The output type
 */
public interface ListBundleFunction<IN, OUT> extends Function {

	void open(ExecutionContext ctx) throws Exception;

	/**
	 * In object reuse mode, sometimes the input value will be reused
	 * thus the bundle list may contain duplicate data. {@link ListBundleFunction}
	 * should copy the value and return a new copy if necessary
	 * @param value Original value to be added to bundle, it may be a changing object if object reuse is enabled,
	 *              so it may be changed in next round.
	 * @return a new value to prevent object reuse if necessary
	 */
	IN addInput(IN value);

	/**
	 * Called when a bundle is finished. Transform a bundle to zero, one, or more output elements.
	 */
	void finishBundle(List<IN> buffer, Collector<OUT> out) throws Exception;

	default void close() throws Exception {}
}
