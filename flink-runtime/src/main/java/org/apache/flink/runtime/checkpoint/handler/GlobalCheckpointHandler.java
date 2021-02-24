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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;

import java.util.Collection;
import java.util.Map;

/**
 * CheckpointHandler for global checkpoints.
 */
public class GlobalCheckpointHandler implements CheckpointHandler {
	@Override
	public void loadPendingCheckpoints(Map<Long, PendingCheckpoint> pendingCheckpoints) {}

	@Override
	public void onCheckpointComplete(CompletedCheckpoint completedCheckpoint) {}

	@Override
	public boolean tryHandleExpireCheckpoint(PendingCheckpoint pendingCheckpoint) {
		return false;
	}

	@Override
	public boolean tryHandleTasksFailure(Collection<ExecutionVertex> vertices) {
		return false;
	}

	@Override
	public boolean tryHandleDeclineMessage(DeclineCheckpoint message) {
		return false;
	}

	@Override
	public boolean tryHandleAck(AcknowledgeCheckpoint message) {
		return false;
	}

	@Override
	public boolean tryHandleCompletedNotification(ExecutionVertex vertex, long checkpointId) {
		return false;
	}

	@Override
	public boolean tryHandleFailUnacknowledgedPendingCheckpoints(ExecutionAttemptID executionAttemptId, Throwable cause) {
		return false;
	}

	@Override
	public void clearCheckpoint(long checkpointId) {}
}
