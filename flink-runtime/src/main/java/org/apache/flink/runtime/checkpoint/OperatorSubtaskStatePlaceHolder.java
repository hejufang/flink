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

package org.apache.flink.runtime.checkpoint;

import java.io.DataInputStream;

/**
 * This class wraps a {@link OperatorSubtaskState} instance from a historical checkpoint. This won't be
 * discarded physically on file system because it's just a placeholder for a historical subtask state.
 * Note: The {@link OperatorSubtaskStatePlaceHolder} can only be instantiated when creating a
 * new {@link PendingCheckpoint}. While recovering from an existing checkpoint, the instance would be
 * transformed to an {@link OperatorSubtaskState} instance, more details
 * on {@link org.apache.flink.runtime.checkpoint.savepoint.SavepointV2Serializer#deserializeSubtaskState(DataInputStream)}.
 */
public class OperatorSubtaskStatePlaceHolder extends OperatorSubtaskState {

	private static final long serialVersionUID = -2391234597971923995L;

	public OperatorSubtaskStatePlaceHolder(OperatorSubtaskState subtaskState) {
		super(subtaskState.getManagedOperatorState(),
			subtaskState.getRawOperatorState(),
			subtaskState.getManagedKeyedState(),
			subtaskState.getRawKeyedState());
	}

	@Override
	public void discardState() {
		// do nothing
	}
}
