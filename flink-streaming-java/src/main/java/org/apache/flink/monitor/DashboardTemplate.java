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

package org.apache.flink.monitor;

/**
 * Dashboard template path, associated Grafana JSON template file location.
 */
public class DashboardTemplate {

	/**
	 * Templates for checkpoint overview metrics.
	 */
	public static final String CHECKPOINT_OVERVIEW_TEMPLATE = "checkpoint/checkpoint_overview_template.txt";
	public static final String CHECKPOINT_TIMER_TEMPLATE = "checkpoint/checkpoint_timer_template.txt";
	public static final String CHECKPOINT_TIMER_LAG_TARGET_TEMPLATE = "checkpoint/checkpoint_timer_lag_target_template.txt";
	public static final String CHECKPOINT_TIMER_RATE_TARGET_TEMPLATE = "checkpoint/checkpoint_timer_rate_target_template.txt";

	/**
	 * Templates for checkpoint operator metrics.
	 */
	public static final String CHECKPOINT_OPERATOR_TEMPLATE = "checkpoint/checkpoint_operator_template.txt";
	public static final String CHECKPOINT_BARRIER_ALIGN_DURATION_TEMPLATE = "checkpoint/checkpoint_barrier_align_duration_target_template.txt";
	public static final String CHECKPOINT_CONTENTION_LOCK_DURATION_TEMPLATE = "checkpoint/checkpoint_contention_lock_duration_target_template.txt";
	public static final String CHECKPOINT_SYNC_DURATION_TEMPLATE = "checkpoint/checkpoint_sync_duration_target_template.txt";
	public static final String CHECKPOINT_ASYNC_DURATION_TEMPLATE = "checkpoint/checkpoint_async_duration_target_template.txt";

	/**
	 * Templates for state performance metrics.
	 */
	public static final String OPERATOR_STATE_PERFORMANCE_TEMPLATE = "state/operator_state_performance_template.txt";
	public static final String STATE_KEY_VALUE_SIZE_TEMPLATE = "state/state_key_value_size_template.txt";
	public static final String STATE_OP_LATENCY_TEMPLATE = "state/state_operation_latency_template.txt";
	public static final String STATE_OP_RATE_TEMPLATE = "state/state_operation_rate_template.txt";
	public static final String STATE_MEMORY_SIZE_TEMPLATE = "state/state_memory_size_template.txt";
	public static final String STATE_TOTAL_SIZE_TEMPLATE = "state/state_total_size_template.txt";
	public static final String STATE_COMPACTION_FLUSH_TEMPLATE = "state/state_compaction_flush_template.txt";
	public static final String STATE_WRITE_STALL_TEMPLATE = "state/state_write_stall_template.txt";

	/**
	 * Templates for state feature metrics.
	 */
	public static final String STATE_CACHE_HIT_RATE = "state/state_cache_hit_rate_template.txt";
	public static final String STATE_CACHE_MEMORY_USAGE = "state/state_cache_memory_usage_template.txt";

	/**
	 * Templates for jvm metrics.
	 */
	public static final String JM_GC_TEMPLATE = "jvm/jm_gc_template.txt";
	public static final String JM_MEMORY_TEMPLATE = "jvm/jm_memory_template.txt";
	public static final String TM_GC_TEMPLATE = "jvm/tm_gc_template.txt";
	public static final String TM_MEMORY_TEMPLATE = "jvm/tm_memory_template.txt";
	public static final String JVM_ROW_TEMPLATE = "jvm/jvm_row_template.txt";
	public static final String JM_THREAD_TEMPLATE = "jvm/jm_thread_template.txt";
	public static final String TM_THREAD_TEMPLATE = "jvm/tm_thread_template.txt";
	public static final String JM_CORES_TEMPLATE = "jvm/jm_cores_template.txt";
	public static final String TM_CORES_TEMPLATE = "jvm/tm_cores_template.txt";

	/**
	 * Templates for kafka metrics.
	 */
	public static final String KAFKA_LAG_SIZE_TARGET_TEMPLATE = "kafka/kafka_lag_size_target_template.txt";
	public static final String KAFKA_LAG_SIZE_TEMPLATE = "kafka/kafka_lag_size_template.txt";
	public static final String KAFKA_LATENCY_TEMPLATE = "kafka/kafka_latency_template.txt";
	public static final String KAFKA_LATENCY_TARGET_TEMPLATE = "kafka/kafka_latency_target_template.txt";
	public static final String KAFKA_OFFSET_TARGET_TEMPLATE = "kafka/Kafka_offset_target_template.txt";
	public static final String KAFKA_OFFSET_TEMPLATE = "kafka/Kafka_offset_template.txt";
	public static final String KAFKA_LAG_LINK_TEMPLATE = "kafka/kafka_lag_link_template.txt";
	public static final String KAFKA_ROW_TEMPLATE = "kafka/kafka_row_template.txt";
	public static final String KAFKA_CONSUME_RATE_TARGET_TEMPLATE = "kafka/kafka_consume_rate_target_template.txt";
	public static final String KAFKA_CONSUME_RATE_TEMPLATE = "kafka/kafka_consume_rate_template.txt";

	/**
	 * Templates for network metrics.
	 */
	public static final String NETWORK_ROW_TEMPLATE = "network/network_row_template.txt";
	public static final String IN_POOL_USAGE_TARGET_TEMPLATE = "network/in_pool_usage_target_template.txt";
	public static final String IN_POOL_USAGE_TEMPLATE = "network/in_pool_usage_template.txt";
	public static final String TASK_STUCK_TARGET_TEMPLATE = "network/task_stuck_target_template.txt";
	public static final String TASK_STUCK_TEMPLATE = "network/task_stuck_template.txt";
	public static final String NETWORK_MEMORY_TEMPLATE = "network/network_memory_template.txt";
	public static final String OUT_POOL_USAGE_TARGET_TEMPLATE = "network/out_pool_usage_target_template.txt";
	public static final String OUT_POOL_USAGE_TEMPLATE = "network/out_pool_usage_template.txt";
	public static final String POOL_USAGE_TEMPLATE = "network/pool_usage_template.txt";
	public static final String RECORD_IN_NUM_TEMPLATE = "network/record_in_num_template.txt";
	public static final String RECORD_IN_PER_SECOND_TARGET_TEMPLATE = "network/record_in_per_second_target_template.txt";
	public static final String RECORD_NUM_TEMPLATE = "network/record_num_template.txt";
	public static final String RECORD_OUT_NUM_TEMPLATE = "network/record_out_num_template.txt";
	public static final String RECORD_OUT_PER_SECOND_TARGET_TEMPLATE = "network/record_out_per_second_target_template.txt";

	/**
	 *  Templates for rocketmq metrics.
	 */
	public static final String ROCKETMQ_LAG_SIZE_TARGET_TEMPLATE = "rocketmq/rocketmq_lag_size_target_template.txt";
	public static final String ROCKETMQ_LAG_SIZE_TEMPLATE = "rocketmq/rocketmq_lag_size_template.txt";
	public static final String ROCKETMQ_LAG_LINK_TEMPLATE = "rocketmq/rocketmq_lag_link_template.txt";
	public static final String ROCKETMQ_ROW_TEMPLATE = "rocketmq/rocketmq_row_template.txt";
	public static final String ROCKETMQ_CONSUME_RATE_TARGET_TEMPLATE = "rocketmq/rocketmq_consume_rate_target_template.txt";
	public static final String ROCKETMQ_CONSUME_RATE_TEMPLATE = "rocketmq/rocketmq_consume_rate_template.txt";
	public static final String ROCKETMQ_POLL_LATENCY_TARGET_TEMPLATE = "rocketmq/rocketmq_poll_latency_target_template.txt";
	public static final String ROCKETMQ_POLL_LATENCY_TEMPLATE = "rocketmq/rocketmq_poll_latency_template.txt";

	/**
	 *  Templates for schedule related metrics.
	 */
	public static final String SCHEDULE_INFO_ROW_TEMPLATE = "scheduleinfo/schedule_info_row_template.txt";
	public static final String SLOW_CONTAINER_TEMPLATE = "scheduleinfo/slow_container_template.txt";
	public static final String TASK_MANAGER_SLOT_TEMPLATE = "scheduleinfo/taskmanager_slot_template.txt";
	public static final String YARN_CONTAINER_TEMPLATE = "scheduleinfo/yarn_container_template.txt";
	public static final String COMPLETED_CONTAINER_TEMPLATE = "scheduleinfo/completed_container_template.txt";

	/**
	 *  Templates for SQL operator related metrics.
	 */
	public static final String DIRTY_RECORDS_SOURCE_SKIPPED_TARGET_TEMPLATE = "sqloperator/dirty_records_source_skipped_target_template.txt";
	public static final String DIRTY_RECORDS_SOURCE_SKIPPED_TEMPLATE = "sqloperator/dirty_records_source_skipped_template.txt";
	public static final String LOOKUP_JOIN_FAIL_PER_SECOND_TARGET_TEMPLATE = "sqloperator/lookup_join_fail_per_second_target_template.txt";
	public static final String LOOKUP_JOIN_FAIL_PER_SECOND_TEMPLATE = "sqloperator/lookup_join_fail_per_second_template.txt";
	public static final String LOOKUP_JOIN_HIT_RATE_TARGET_TEMPLATE = "sqloperator/lookup_join_hit_rate_target_template.txt";
	public static final String LOOKUP_JOIN_HIT_RATE_TEMPLATE = "sqloperator/lookup_join_hit_rate_template.txt";
	public static final String LOOKUP_JOIN_REQUEST_DELAY_P99_PER_SECOND_TARGET_TEMPLATE = "sqloperator/lookup_join_request_delay_p99_per_second_target_template.txt";
	public static final String LOOKUP_JOIN_REQUEST_DELAY_P99_PER_SECOND_TEMPLATE = "sqloperator/lookup_join_request_delay_p99_per_second_template.txt";
	public static final String LOOKUP_JOIN_REQUEST_PER_SECOND_TARGET_TEMPLATE = "sqloperator/lookup_join_request_per_second_target_template.txt";
	public static final String LOOKUP_JOIN_REQUEST_PER_SECOND_TEMPLATE = "sqloperator/lookup_join_request_per_second_template.txt";
	public static final String RECORDS_SINK_SKIPPED_TARGET_TEMPLATE = "sqloperator/records_sink_skipped_target_template.txt";
	public static final String RECORDS_SINK_SKIPPED_TEMPLATE = "sqloperator/records_sink_skipped_template.txt";
	public static final String SQLOPERATOR_ROW_TEMPLATE = "sqloperator/sqloperator_row_template.txt";

	/**
	 *  Templates for watermark related metrics.
	 */
	public static final String LATE_RECORD_DROPPED_TARGET_TEMPLATE = "watermark/late_record_dropped_target_template.txt";
	public static final String LATE_RECORDS_DROPPED_TEMPLATE = "watermark/late_records_dropped_template.txt";
	public static final String WATERMARK_ROW_TEMPLATE = "watermark/watermark_row_template.txt";
	public static final String WATERMARK_LATENCY_TARGET_TEMPLATE = "watermark/watermark_latency_target_template.txt";
	public static final String WATERMARK_LATENCY_TEMPLATE = "watermark/watermark_latency_template.txt";

	/**
	 *  Templates for overview.
	 */
	public static final String OVERVIEW_ROW_TEMPLATE = "overview_row_template.txt";
	public static final String OVERVIEW_TEMPLATE = "overview_template.txt";
	public static final String JOB_INFO_TEMPLATE = "job_info_template.txt";
	public static final String OPERATOR_LATENCY_TARGET_TEMPLATE = "operatorlatency/operator_latency_target_template.txt";
	public static final String OPERATOR_LATENCY_TEMPLATE = "operatorlatency/operator_latency_template.txt";
	public static final String OPERATOR_LATENCY_ROW_TEMPLATE = "operatorlatency/operator_latency_row_template.txt";

	/**
	 * Process Latency.
	 */
	public static final String PROCESS_LATENCY_JOB_TEMPLATE = "process-latency/process_latency_sink_template.txt";
	//except for sink operators.
	public static final String PROCESS_LATENCY_OPERATOR_TEMPLATE = "process-latency/process_latency_operator_template.txt";
	public static final String PROCESS_LATENCY_OPERATOR_TARGET_TEMPLATE = "process-latency/process_latency_operator_target_template.txt";

	public static final String DASHBOARD_TEMPLATE = "dashboard_template.txt";
}
