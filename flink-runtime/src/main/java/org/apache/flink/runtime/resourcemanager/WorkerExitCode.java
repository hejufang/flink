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

package org.apache.flink.runtime.resourcemanager;

/**
 * Exit code of worker.
 */
public class WorkerExitCode {
	public static final int TASKMANAGER_GENERAL_ERROR_CODE = 2;
	public static final int UNKNOWN = -80000;
	public static final int SLOW_CONTAINER = -80001;
	public static final int SLOW_CONTAINER_TIMEOUT = -80014;
	public static final int START_CONTAINER_ERROR = -80002;
	public static final int HEARTBEAT_TIMEOUT = -80003;
	public static final int IN_BLACKLIST = -80004;
	public static final int IDLE_TIMEOUT = -80005;
	public static final int EXIT_BY_TASK_MANAGER = -80006;
	public static final int MAX_SLOT_EXCEED = -80007;
	public static final int EXCESS_CONTAINER = -80008;
	public static final int EXIT_BY_JOB_MANAGER = -80009;
	public static final int PREVIOUS_TM_TIMEOUT = -80010;
	public static final int IN_BLACKLIST_BECAUSE_CRITICAL_ERROR = -80011;

	/** For Kubernetes. */
	public static final int POD_DELETED = -80012;
	public static final int POD_TERMINATED = -80013;

	/** For TaskManager. */
	public static final int TASKMANAGER_START_ERROR = 200;
	public static final int TASKMANAGER_RELEASE_PARTITION_ERROR = 201;
	public static final int TASKMANAGER_RELEASE_SLOT_ERROR = 202;
	public static final int TASKMANAGER_RELEASE_SLOT_NOTFOUND = 203;
	public static final int TASKMANAGER_REGISTRAR_RM_TIMEOUT = 204;
	public static final int TASKMANAGER_UPDATE_STATE_RPC_ERROR = 205;
	public static final int TASKMANAGER_UPDATE_STATE_ERROR = 206;
	public static final int TASKMANAGER_LISTEN_LEADER_RM_ERROR = 207;
	public static final int TASKMANAGER_LISTEN_LEADER_JM_ERROR = 208;
	public static final int TASKMANAGER_REGISTRAR_RM_ERROR = 209;
	public static final int TASKMANAGER_TASK_EXIT_TIMEOUT = 210;
	public static final int TASKMANAGER_PROCESS_EXCEPTION_ERROR = 211;
	public static final int TASKMANAGER_CLEAN_RESOURCE_ERROR = 212;
	public static final int TASKMANAGER_CANCEL_TASK_TIMEOUT = 213;
}
