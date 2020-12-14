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

package org.apache.flink.cep;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 *
 * @param <IN>
 * @param <OUT>
 */
public interface MultiplePatternFlatTimeoutFunction<IN, OUT> extends Function, Serializable {

	/**
	 * Generates zero or more resulting timeout elements given a map of partial pattern events and
	 * the timestamp of the timeout. The events are identified by their specified names.
	 *
	 * @param pattern Map containing the partial pattern. Events are identified by their names.
	 * @param timeoutTimestamp Timestamp when the timeout occurred
	 * @param out Collector used to output the generated elements
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the
	 * 					 operation to fail and may trigger recovery.
	 */
	void timeout(Tuple2<String, Map<String, List<IN>>> pattern, long timeoutTimestamp, Collector<OUT> out) throws Exception;
}
