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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 *
 * @param <IN>
 * @param <OUT>
 */
public interface MultiplePatternTimeoutFunction<IN, OUT> extends Function, Serializable {

	/**
	 * Generates a timeout result from the given map of events and timeout timestamp. The partial
	 * events are identified by their names. Only one resulting element can be generated.
	 *
	 * @param pattern Map containing the found partial pattern. Events are identified by their names
	 * @param timeoutTimestamp Timestamp of the timeout
	 * @return Resulting timeout element
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the
	 * 					 operation to fail and may trigger recovery.
	 */
	OUT timeout(Tuple2<String, Map<String, List<IN>>> pattern, long timeoutTimestamp) throws Exception;
}
