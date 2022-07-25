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

package org.apache.flink.runtime.metrics;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import java.util.Set;

/**
 * Collection of metric names.
 */
public class MetricNames {
	private MetricNames() {
	}

	public static final String SUFFIX_RATE = "PerSecond";

	public static final String IO_NUM_RECORDS_IN = "numRecordsIn";
	public static final String IO_NUM_RECORDS_OUT = "numRecordsOut";
	public static final String IO_NUM_RECORDS_IN_RATE = IO_NUM_RECORDS_IN + SUFFIX_RATE;
	public static final String IO_NUM_RECORDS_OUT_RATE = IO_NUM_RECORDS_OUT + SUFFIX_RATE;

	public static final String IO_NUM_RECORDS_DROPPED = "numRecordsDropped";
	public static final String IO_NUM_BUFFERS_DROPPED = "numBuffersInDropped";

	public static final String IO_NUM_BYTES_IN = "numBytesIn";
	public static final String IO_NUM_BYTES_OUT = "numBytesOut";
	public static final String IO_NUM_BYTES_IN_RATE = IO_NUM_BYTES_IN + SUFFIX_RATE;
	public static final String IO_NUM_BYTES_OUT_RATE = IO_NUM_BYTES_OUT + SUFFIX_RATE;

	public static final Set<String> IO_METRIC_NAMES = Sets.newHashSet(MetricNames.IO_NUM_BYTES_IN, MetricNames.IO_NUM_BYTES_OUT, MetricNames.IO_NUM_BUFFERS_IN, MetricNames.IO_NUM_BUFFERS_OUT, MetricNames.IO_NUM_RECORDS_IN, MetricNames.IO_NUM_RECORDS_OUT);

	// the time unit is microsecond
	public static final String OPERATOR_PROCESS_LATENCY = "latency";

	public static final String IO_NUM_BUFFERS_IN = "numBuffersIn";
	public static final String IO_NUM_BUFFERS_OUT = "numBuffersOut";
	public static final String IO_NUM_BUFFERS_OUT_RATE = IO_NUM_BUFFERS_OUT + SUFFIX_RATE;

	public static final String IO_CURRENT_INPUT_WATERMARK = "currentInputWatermark";
	@Deprecated
	public static final String IO_CURRENT_INPUT_1_WATERMARK = "currentInput1Watermark";
	@Deprecated
	public static final String IO_CURRENT_INPUT_2_WATERMARK = "currentInput2Watermark";
	public static final String IO_CURRENT_INPUT_WATERMARK_PATERN = "currentInput%dWatermark";
	public static final String IO_CURRENT_OUTPUT_WATERMARK = "currentOutputWatermark";

	public static final String NUM_RUNNING_JOBS = "numRunningJobs";
	public static final String NUM_SUBMITTED_JOBS = "numSubmittedJobs";
	public static final String NUM_FAILED_JOBS = "numFailedJobs";
	public static final String NUM_CANCELED_JOBS = "numCanceledJobs";
	public static final String NUM_FINISHED_JOBS = "numFinishedJobs";
	public static final String NUM_SLOW_JOBS = "numSlowJobs";
	public static final String NUM_REJECTED_JOBS = "numRejectedJobs";

	public static final String JOB_DURATION = "jobDuration";
	public static final String FAILED_JOB_DURATION = "failedJobDuration";
	public static final String JOB_LATENCY_UNTIL_SCHEDULED = "jobLatencyUntilScheduled";

	public static final String TASK_SLOTS_AVAILABLE = "taskSlotsAvailable";
	public static final String TASK_SLOTS_TOTAL = "taskSlotsTotal";
	public static final String NUM_REGISTERED_TASK_MANAGERS = "numRegisteredTaskManagers";
	public static final String WORKER_FAILURE_RATE = "workerFailureRate";

	public static final String NUM_JOB_REQUIRED_WORKERS = "numJobRequiredWorkers";

	public static final String NUM_RESTARTS = "numRestarts";

	@Deprecated
	public static final String FULL_RESTARTS = "fullRestarts";
	public static final String NUM_RESTARTS_TAG_FAILOVER_STRATEGY_NAME = "failoverStrategy";
	public static final String NUM_RESTARTS_TAG_RESTART_STRATEGY_NAME = "restartStrategy";
	public static final String NUM_FAIL_FILTERED_BY_AGGREGATED_STRATEGY_NAME = "numOfFailFilteredByAggregatedStrategy";
	public static final String NUM_RESTARTS_AGGR_BY_BACKOFF_TIME = "numOfRestartsAggrByBackoffTime";
	public static final String FULL_RESTARTS_RATE = "fullRestartsRate";

	public static final String NO_RESOURCE_AVAILABLE_EXCEPTION = "noResourceAvailableException";

	public static final String EXECUTION_STATE_TAG_STATE_NAME = "state";
	public static final String EXECUTION_STATE_TAG_VERTEX_INDEX_NAME = "vertexIndex";
	public static final String EXECUTION_STATE_TAG_SUBTASK_ID_NAME = "subtaskId";
	public static final String EXECUTION_STATE_TIME = "executionStateTime";
	public static final String UPSTREAM_TASK_ERROR_NUM = "upstreamTaskErrorNum";
	public static final String TASK_EXECUTOR_EXECUTION_STATE_TIME = "taskExecutorExecutionStateTime";
	public static final String TASK_FAILING_TIME = "taskFailingTime";
	public static final String NUMBER_TASK_PER_TASK_EXECUTOR = "numberOfTasks";
	public static final String METRIC_INPUT_CHANNEL_NUMBER = "numberOfInputChannels";

	public static final String NUM_PENDING_TASK_MANAGER_SLOTS = "numPendingTaskManagerSlots";
	public static final String NUM_PENDING_SLOT_REQUESTS = "numPendingSlotRequests";
	public static final String NUM_LACK_SLOTS = "numLackSlots";
	public static final String NUM_EXCESS_WORKERS = "numExcessWorkers";
	public static final String NUM_LACK_WORKERS = "numLackWorkers";

	public static final String TIME_AM_START = "timeOfAMStart";

	public static final String NUM_RM_HEARTBEAT_TIMOUT_FROM_JM = "RMHeartbeatTimeoutFromJM";
	public static final String NUM_RM_HEARTBEAT_TIMOUT_FROM_TM = "RMHeartbeatTimeoutFromTM";
	public static final String NUM_JM_HEARTBEAT_TIMOUT_FROM_RM = "JMHeartbeatTimeoutFromRM";
	public static final String NUM_JM_HEARTBEAT_TIMOUT_FROM_TM = "JMHeartbeatTimeoutFromTM";
	public static final String NUM_TM_HEARTBEAT_TIMOUT_FROM_RM = "TMHeartbeatTimeoutFromRM";
	public static final String NUM_TM_HEARTBEAT_TIMOUT_FROM_JM = "TMHeartbeatTimeoutFromJM";

	public static final String MEMORY_USED = "Used";
	public static final String MEMORY_COMMITTED = "Committed";
	public static final String MEMORY_MAX = "Max";

	public static final String SOURCE_TOPIC_PARTITIONS = "sourceTopicPartitions";

	public static final String IS_BACKPRESSURED = "isBackPressured";

	public static final String CHECKPOINT_ALIGNMENT_TIME = "checkpointAlignmentTime";
	public static final String CHECKPOINT_START_DELAY_TIME = "checkpointStartDelayNanos";

	public static String currentInputWatermarkName(int index) {
		return String.format(IO_CURRENT_INPUT_WATERMARK_PATERN, index);
	}

	public static final String TASK_IDLE_TIME = "idleTimeMs" + SUFFIX_RATE;

	public static final String NUM_PENDING_IO_TASK = "numPendingIOTask";
	public static final String NUM_RUNNING_IO_TASK = "numRunningIOTask";
	public static final String IO_THREAD_POOL_USAGE = "ioThreadPoolUsage";

	public static final String NUM_PENDING_FUTURE_TASK = "numPendingFutureTask";
	public static final String NUM_RUNNING_FUTURE_TASK = "numRunningFutureTask";
	public static final String FUTURE_THREAD_POOL_USAGE = "futureThreadPoolUsage";

	public static final String NUM_CHANNELS_UPDATED_BY_JM = "numChannelsUpdatedByJM";
	public static final String NUM_CHANNELS_UPDATED_BY_TASK = "numChannelsUpdatedByTask";
	public static final String NUM_CHANNELS_INJECTED_ERROR = "numChannelsInjectedError";
	public static final String NUM_CHANNELS_CACHED = "numChannelsCached";

	public static final String ALLOCATED_CONTAINER_NUM = "allocatedContainerNum";
	public static final String PENDING_REQUESTED_CONTAINER_NUM = "pendingRequestedContainerNum";
	public static final String STARTING_CONTAINERS = "startingContainers";
	public static final String ALLOCATED_CPU = "allocatedCPU";
	public static final String ALLOCATED_MEMORY = "allocatedMemory";
	public static final String PENDING_CPU = "pendingCPU";
	public static final String PENDING_MEMORY = "pendingMemory";
	public static final String PENDING_PHASE_PODS = "pendingPhasePods";
	public static final String COMPLETED_CONTAINER = "completedContainer";

	public static final String TM_CONSTRUCTOR_CACHE_HIT_RATE = "constructorCacheHitRate";
}
