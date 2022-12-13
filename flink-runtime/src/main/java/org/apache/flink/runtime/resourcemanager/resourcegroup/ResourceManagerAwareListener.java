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

package org.apache.flink.runtime.resourcemanager.resourcegroup;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;

/**
 * The listener responsible for monitoring the cluster TaskManager info change in resource manager.
 */
public interface ResourceManagerAwareListener {
	// 注册新的TM时进行RG重分配并同步TM给Dispatcher
	void onRegisterTaskManager(TaskExecutorGateway taskExecutorGateway, TaskExecutorRegistration taskExecutorRegistration, UnresolvedTaskManagerLocation unresolvedTaskManagerLocation);

	// 关闭老的TM连接时进行RG重分配并同步TM给Dispatcher
	void onCloseTaskManager(ResourceID resourceID);
}
