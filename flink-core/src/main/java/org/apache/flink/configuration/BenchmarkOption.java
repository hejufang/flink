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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The set of configuration options for benchmark.
 */
@PublicEvolving
public class BenchmarkOption {

	public static final ConfigOption<Boolean> JOB_RECEIVE_THEN_FINISH_ENABLE = ConfigOptions
		.key("benchmark.job-receive-then-finish.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("Job reaches termination immediately when receive job start request.");

	public static final ConfigOption<Boolean> JOB_ANALYZED_THEN_FINISH_ENABLE = ConfigOptions
		.key("benchmark.job-analyzed-then-finish.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("Job reaches termination immediately before job schedule start");
}
