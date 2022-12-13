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

package org.apache.flink.runtime.resourcemanager.resourcegroup.client.apass;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * APaaS resource info definition, used to mapping the data of rds metadata service.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class APaaSResourceGroupInfo {
	private static final String FIELD_INSTANCE_NAME = "instance_name";

	private static final String FIELD_ID = "id";

	private static final String FIELD_RG_NAME = "name";

	private static final String FIELD_TP_CPU_CORES = "tp_cpu_cores";

	private static final String FIELD_TP_SCALABLE_CPU_CORES = "tp_scalable_cpu_cores";

	private static final String FIELD_TP_MEMORY = "tp_memory";

	private static final String FIELD_TP_SCALABLE_MEMORY = "tp_scalable_memory";

	private static final String FIELD_TP_CONCURRENCY = "tp_concurrency";

	private static final String FIELD_AP_CPU_CORES = "ap_cpu_cores";

	private static final String FIELD_AP_SCALABLE_CPU_CORES = "ap_scalable_cpu_cores";

	private static final String FIELD_AP_MEMORY = "ap_memory";

	private static final String FIELD_AP_SCALABLE_MEMORY = "ap_scalable_memory";

	private static final String FIELD_AP_CONCURRENCY = "ap_concurrency";

	private static final String FIELD_MATCH_RULES = "match_rules";

	private static final String FIELD_MATCH_PRIORITY = "match_priority";

	private static final String FIELD_CREATE_TIME = "create_time";

	private static final String FIELD_UPDATE_TIME = "update_time";

	@JsonProperty(FIELD_INSTANCE_NAME)
	public String instanceName;

	@JsonProperty(FIELD_ID)
	public String id;

	@JsonProperty(FIELD_RG_NAME)
	public String rgName;

	@JsonProperty(FIELD_TP_CPU_CORES)
	private Double tpCPUCores;

	@JsonProperty(FIELD_TP_SCALABLE_CPU_CORES)
	private Double tpScalableCPUCores;

	@JsonProperty(FIELD_TP_MEMORY)
	private Double tpMemory;

	@JsonProperty(FIELD_TP_SCALABLE_MEMORY)
	private Double tpScalableMemory;

	@JsonProperty(FIELD_TP_CONCURRENCY)
	private int tpConcurrency;

	@JsonProperty(FIELD_AP_CPU_CORES)
	public Double apCPUCores;

	@JsonProperty(FIELD_AP_SCALABLE_CPU_CORES)
	public Double apScalableCPUCores;

	@JsonProperty(FIELD_AP_MEMORY)
	public Double apMemory;

	@JsonProperty(FIELD_AP_SCALABLE_MEMORY)
	public Double apScalableMemory;

	@JsonProperty(FIELD_AP_CONCURRENCY)
	public int apConcurrency;

	@JsonProperty(FIELD_MATCH_RULES)
	private String matchRules;

	@JsonProperty(FIELD_MATCH_PRIORITY)
	private int matchPriority;

	@JsonProperty(FIELD_CREATE_TIME)
	private String createTime;

	@JsonProperty(FIELD_UPDATE_TIME)
	private String updateTime;

	@JsonCreator
	public APaaSResourceGroupInfo(
		@JsonProperty(FIELD_INSTANCE_NAME) String instanceName,
		@JsonProperty(FIELD_ID) String id,
		@JsonProperty(FIELD_RG_NAME) String rgName,
		@JsonProperty(FIELD_AP_CPU_CORES) Double apCPUCores,
		@JsonProperty(FIELD_AP_SCALABLE_CPU_CORES) Double apScalableCPUCores,
		@JsonProperty(FIELD_AP_MEMORY) Double apMemory,
		@JsonProperty(FIELD_AP_SCALABLE_MEMORY) Double apScalableMemory,
		@JsonProperty(FIELD_AP_CONCURRENCY) int apConcurrency
	) {
		this.instanceName = instanceName;
		this.id = id;
		this.rgName = rgName;
		this.apCPUCores = apCPUCores;
		this.apScalableCPUCores = apScalableCPUCores;
		this.apMemory = apMemory;
		this.apScalableMemory = apScalableMemory;
		this.apConcurrency = apConcurrency;
	}
}
