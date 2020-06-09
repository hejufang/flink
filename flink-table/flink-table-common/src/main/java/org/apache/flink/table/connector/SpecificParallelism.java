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

package org.apache.flink.table.connector;

/**
 * This interface is an optional interface for sources and sinks.
 * Override this interface to return a specific parallelism for this connector,
 * most of cases, specified by user through DDL properties.
 * Specially, there are four cases:
 * 1. org.apache.flink.api.common.io.InputFormat
 * 2. org.apache.flink.streaming.api.functions.source.SourceFunction
 * 3. org.apache.flink.api.common.io.OutputFormat
 * 4. org.apache.flink.streaming.api.functions.sink.SinkFunction
 */
public interface SpecificParallelism {

	/**
	 * Returns the parallelism for this connector, only positive value is valid.
	 * @return parallelism for this connector.
	 */
	int getParallelism();
}
