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

package org.apache.flink.runtime.checkpoint.handler;

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;

import java.util.Collection;
import java.util.Map;

/**
 * Handler used to handle with acks, expiration and other behaviours which may affect the checkpoint.
 */
public interface CheckpointHandler {

	void loadPendingCheckpoints(Map<Long, PendingCheckpoint> pendingCheckpoints);

	void onCheckpointComplete(CompletedCheckpoint completedCheckpoint);

	/**
	 * Try to make an expired checkpoint successful.
	 * @return true if the expired checkpoint could be handled.
	 */
	boolean tryHandleExpireCheckpoint(PendingCheckpoint pendingCheckpoint);

	/**
	 * Try to recover the checkpoint from a failed task.
	 * @return true if task failure does not affect the checkpoint.
	 */
	boolean tryHandleTasksFailure(Collection<ExecutionVertex> vertices);

	/**
	 * Try to recover the checkpoint from a decline message.
	 * @return true if decline message can be ignored.
	 */
	boolean tryHandleDeclineMessage(DeclineCheckpoint message);

	/**
	 * Check whether the ack is available or not.
	 * @return true if the ack can be ignored.
	 */
	boolean tryHandleAck(AcknowledgeCheckpoint message);

	/**
	 * Check whether the notifyCheckpointComplete notification should be sent or not.
	 * @return true if the notification can be ignored.
	 */
	boolean tryHandleCompletedNotification(ExecutionVertex vertex, long checkpointId);

	/**
	 * Clear a specific checkpoint's data.
	 */
	void clearCheckpoint(long checkpointId);
}
