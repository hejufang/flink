/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.TaskState;

import java.util.Collection;

/**
 * SavepointV3 for dts upgrading.
 */
public class SavepointV3 implements Savepoint {

	/** The savepoint version. */
	public static final int VERSION = 3;

	@Override
	public int getVersion() {
		throw new UnsupportedOperationException("");
	}

	@Override
	public void dispose() throws Exception {
		throw new UnsupportedOperationException("");

	}

	@Override
	public long getCheckpointId() {
		throw new UnsupportedOperationException("");
	}

	@Override
	public Collection<TaskState> getTaskStates() {
		throw new UnsupportedOperationException("");
	}

	@Override
	public Collection<MasterState> getMasterStates() {
		throw new UnsupportedOperationException("");
	}

	@Override
	public Collection<OperatorState> getOperatorStates() {
		throw new UnsupportedOperationException("");
	}
}
