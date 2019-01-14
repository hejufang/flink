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

package com.bytedance.flink.configuration;

/**
 * Config constants.
 */
public class Constants {
	public static final String TOPOLOGY_NAME = "topology_name";
	public static final String TOPOLOGY_YAML = "topology.yaml";
	public static final String OWNERS = "owners";
	public static final String ENVIRONMENT = "environment";
	public static final String PYTHONPATH_KEY = "PYTHONPATH";
	public static final String PYTHONPATH_VAL = "/opt/tiger/pyFlink";
	public static final String USE_NEW_MODE = "use_new_mode";
	public static final String RUN_MODE = "topology_standalone_mode";
	public static final int RUN_MODE_CLUSTER = 0;
	public static final int RUN_MODE_STANDLONE = 1;
	public static final String RUN_TIME_KEY = "topology_standalone_time";
	public static final int RUN_TIME_VAL = 100;
	public static final String FLINK_ARGS = "flink_args";
	public static final String FAILOVER_STRATEGY = "jobmanager.execution.failover-strategy";
	public static final String LOCAL_FAILOVER_STRATEGY = "local";
	public static final String MAX_SPOUT_PENDING_KEY = "max_spout_pending";
	public static final int MAX_SPOUT_PENDING_VAL = 50;
	public static final String MAX_BOLT_PENDING_KEY = "max_shellbolt_pending";
	public static final int MAX_BOLT_PENDING_VAL = 200;
	public static final String SERIALIZER = "serializer";
	public static final String INTERPRETER_KEY = "interpreter";
	public static final String INTERPRETER_VAL = "python";
	public static final String COMMON_ARGS = "common_args";
	public static final String SPOUT = "spout";
	public static final String CONSUMER_GROUP = "consumer_group";
	public static final String OUTPUT_FIELDS = "output_fields";
	public static final String SCRIPT = "script_name";

	public static final String KAFKA_SPOUT_EXT_KEY = "kafka_spout_ext";
	public static final int KAFKA_SPOUT_EXT_VAL = 1;
	public static final String IS_THREAD_NUM_DETREMINED_BY_PARTITION =
		"is_thread_num_determined_by_partition";
	public static final String WHENCE = "whence";
	public static final String SPOUT_LIST = "spout_list";
	public static final String SPOUT_NAME = "spout_name";
	public static final String ARGS = "args";
	public static final String TOPIC_NAME = "topic_name";
	public static final String KAFKA_CLUSTER = "kafka_cluster";
	public static final String KAFKA_SOURCE = "kafka_sources";
	public static final String KAFKA_PARTITION_RANGE = "partition_range";
	public static final String NSQ_THREADS = "nsq_threads";
	public static final String PARTITION = "partition";
	public static final String TOTAL_PARTITION = "total_partition";
	public static final String CONSUMED_PARTITION = "consumed_partition";
	public static final String IS_AUTO_PARTITION = "auto_partition";
	public static final String MULTI_SOURCE = "kafka_multi_source";
	public static final String NSQ_FLAG = "nsq";

	public static final String BOLT = "bolt";
	public static final String BOLT_LIST = "bolt_list";
	public static final String PARALLELISM = "thread_num";
	public static final String BOLT_NAME = "bolt_name";
	public static final String GROUP_LIST = "group_list";
	public static final String GROUP_TYPE = "group_type";
	public static final String GROUP_FIELD = "group_field";
	public static final String STREAM_FROM = "stream_from";

	public static final String BATCH_BOLT = "batch_bolt";
	public static final String FLUSH_INTERVAL_SECONDS_KEY = "flush_interval";
	public static final int FLUSH_INTERVAL_SECONDS_VAL = 600;
	public static final String BATCH_THRESHOLD = "batch_threshold";
	public static final String EMPTY_STATE_IDENTIFY = "__eml";

	public static final String FLINK_BATCH_RUNNER =
		"/opt/tiger/pyFlink/pytopology/runner/batch_bolt_new.py";
	public static final String FLINK_BOLT_RUNNER =
		"/opt/tiger/pyFlink/pytopology/runner/bolt.py";

	public static final String FLINK_SPOUT_RUNNER =
		"/opt/tiger/pyFlink/pytopology/runner/kafka_spout.py";
	public static final String FLINK_MULTI_SPOUT_RUNNER =
		"/opt/tiger/pyFlink/pytopology/runner/multi_kafka_spout.py";

	public static final String FLINK_GEVENT_SPOUT_RUNNER =
		"/opt/tiger/pyFlink/pytopology/runner/kafka_spout_gevent.py";
	public static final String FLINK_NSQ_SPOUT_RUNNER =
		"/opt/tiger/pyFlink/pytopology/runner/nsq_spout.py";
	public static final String FLINK_NSQ_ADSTAT_SPOUT_RUNNER =
		"/opt/tiger/pyFlink/pytopology/runner/nsq_spout_adstat.py";

	public static final String BATCH_BOLT_NEW_RUNNER_FLAG = "/batch_bolt_new.py";
	public static final String BATCH_BOLT_RUNNER_FLAG = "/batch_bolt.py";
	public static final String BOLT_RUNNER_FLAG = "/bolt.py";

	public static final String KAFKA_SPOUT_RUNNER_FLAG = "/kafka_spout.py";
	public static final String KAFKA_SPOUT_GEVENT_RUNNER_FLAG = "/kafka_spout_gevent.py";
	public static final String MULTI_KAFKA_SPOUT_RUNNER_FLAG = "/multi_kafka_spout.py";

	public static final String NSQ_SPOUT_RUNNER_FLAG = "/nsq_spout.py";
	public static final String NSQ_SPOUT_ADSTAT_RUNNER_FLAG = "/nsq_spout_adstat.py";

	public static final String FLINK_CONF_DIR_PROPERTY = "flinkConfDir";
	public static final String YAML_CONF_PROPERTY = "yamlConf";
	public static final String USER_JAR = "user.jar";
	public static final String RESOURCE_FILE_PREFIX = "resources/";
	public static final String DETACHED_PROPERTY = "detached";
	public static final String RESET_BOLT_THREAD = "reset_bolt_thread";

	public static final String CLUSTER_NAME = "clusterName";
	public static final String JOB_NAME = "jobName";
	public static final String IS_KILL_PROCESS_GROUP_KEY = "kill_process_group";
	public static final boolean IS_KILL_PROCESS_GROUP_VAL = true;

	public static final String INDEX = "index";
	public static final String SUB_TASK_ID = "subTaskId";
	public static final String RESOURCE_FILES = "resourceFiles";
	public static final String TASK_NAME = "taskName";
	public static final String TEMP_DIR_KEY = "tempDir";
	public static final String TEMP_DIR_VAL = "code";
	public static final String CODE_DIR_KEY = "codeDir";
	public static final String CODE_DIR_VAL = "flink-dist";
	public static final String PID_DIR_KEY = "pidDir";
	public static final String PID_DIR_VAL = "pid-dir";
	public static final String CONF = "conf";
	public static final String PID = "pid";
	public static final String COMMAND = "command";
	public static final String TUPLE = "tuple";
	public static final String MESSAGE = "msg";
	public static final String IS_IGNORE_MISMATCHED_MSG = "is_ignore_mismatched_msg";
	public static final String DONT_WRITE_BYTECODE = "dont_write_bytecode";
	public static final String LOCAL_FAILOVER = "flink.localfailover";
	public static final String LOG_FILE = "log_file";

	public static final String NEXT = "next";
	public static final String EMIT = "emit";
	public static final String LOG = "log";
	public static final String ERROR = "error";
	public static final String SYNC = "sync";

	public static final String SHUFFLE = "shuffle";
	public static final String LOCAL_FIRST = "local_first";
	public static final String FORWARD = "forward";
	public static final String RESCALE = "rescale";
	public static final String ALL = "all";
	public static final String BROADCAST = "broadcast";
	public static final String REBALANCE = "rebalance";
	public static final String KEY_BY = "key_by";
	public static final String FIELDS = "fields";

	public static final int FLINK_TUPLE_MAX_FIELD_SIZE = 25;
}
