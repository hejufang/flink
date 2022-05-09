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
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.description.TextElement.code;

/**
 * Options which control the cluster behaviour.
 */
@PublicEvolving
public class ClusterOptions {

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> INITIAL_REGISTRATION_TIMEOUT = ConfigOptions
		.key("cluster.registration.initial-timeout")
		.defaultValue(100L)
		.withDescription("Initial registration timeout between cluster components in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> MAX_REGISTRATION_TIMEOUT = ConfigOptions
		.key("cluster.registration.max-timeout")
		.defaultValue(30000L)
		.withDescription("Maximum registration timeout between cluster components in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> ERROR_REGISTRATION_DELAY = ConfigOptions
		.key("cluster.registration.error-delay")
		.defaultValue(10000L)
		.withDescription("The pause made after an registration attempt caused an exception (other than timeout) in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> REFUSED_REGISTRATION_DELAY = ConfigOptions
		.key("cluster.registration.refused-registration-delay")
		.defaultValue(30000L)
		.withDescription("The pause made after the registration attempt was refused in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Long> CLUSTER_SERVICES_SHUTDOWN_TIMEOUT = ConfigOptions
		.key("cluster.services.shutdown-timeout")
		.defaultValue(30000L)
		.withDescription("The shutdown timeout for cluster services like executors in milliseconds.");

	@Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
	public static final ConfigOption<Integer> CLUSTER_IO_EXECUTOR_POOL_SIZE = ConfigOptions
		.key("cluster.io-pool.size")
		.intType()
		.noDefaultValue()
		.withDescription("The size of the IO executor pool used by the cluster to execute blocking IO operations (Master as well as TaskManager processes). " +
			"By default it will use 4 * the number of CPU cores (hardware contexts) that the cluster process has access to. " +
			"Increasing the pool size allows to run more IO operations concurrently.");

	@Documentation.Section(Documentation.Sections.EXPERT_SCHEDULING)
	public static final ConfigOption<Boolean> EVENLY_SPREAD_OUT_SLOTS_STRATEGY = ConfigOptions
		.key("cluster.evenly-spread-out-slots")
		.defaultValue(false)
		.withDescription(
			Description.builder()
				.text("Enable the slot spread out allocation strategy. This strategy tries to spread out " +
					"the slots evenly across all available %s.", code("TaskExecutors"))
				.build());

	public static final ConfigOption<Boolean> CLUSTER_SOCKET_ENDPOINT_ENABLE = ConfigOptions
		.key("cluster.socket-endpoint.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("Setup socket endpoint in the cluster.");

	public static final ConfigOption<Boolean> JM_RESOURCE_ALLOCATION_ENABLED = ConfigOptions
		.key("cluster.jm-resource-allocation.enabled")
		.booleanType()
		.defaultValue(false)
		.withDescription("Enable resource allocation in JM side.");

	/**
	 * The minimum number of workers required for resource manager.
	 */
	public static final ConfigOption<Integer> RM_MIN_WORKER_NUM = ConfigOptions
		.key("cluster.resourcemanager-min-worker-num")
		.intType()
		.defaultValue(20)
		.withDescription("The min required workers in resource manager");

	/**
	 * The maximum number of workers can be hold in resource manager.
	 */
	public static final ConfigOption<Integer> RM_MAX_WORKER_NUM = ConfigOptions
		.key("cluster.resourcemanager-max-worker-num")
		.intType()
		.defaultValue(100)
		.withDescription("The max workers can be registered in resource manager");

	/**
	 * The thread count for job results.
	 */
	public static final ConfigOption<Integer> CLUSTER_RESULT_THREAD_COUNT = ConfigOptions
		.key("cluster.result.thread.count")
		.intType()
		.defaultValue(1000)
		.withDescription("The thread count for job result process");

	/**
	 * The max idle time of the result time.
	 */
	public static final ConfigOption<Long> CLUSTER_RESULT_THREAD_IDLE_TIMEOUT_MILLS = ConfigOptions
		.key("cluster.result.thread.idle.timeout.mills")
		.longType()
		.defaultValue(30 * 60 * 1000L)
		.withDescription("When result thread idle time exceeds this value, the thread will be released");

	/**
	 * Enable job master deploy tasks to task executor by netty.
	 */
	public static final ConfigOption<Boolean> CLUSTER_DEPLOY_TASK_SOCKET_ENABLE = ConfigOptions
		.key("cluster.deploy-task-socket.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("If true, jobMaster will deploy tasks to netty server in task executor.");

	/**
	 * The max result queue size in each socket client.
	 */
	public static final ConfigOption<Integer> CLUSTER_CLIENT_RESULT_QUEUE_MAX_SIZE = ConfigOptions
		.key("cluster.client-result-queue.max-size")
		.intType()
		.defaultValue(1000)
		.withDescription("Each socket client has a queue to the results received, it will limit the queue max size");
}
