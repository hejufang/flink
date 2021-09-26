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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.util.Disposable;

import java.io.Serializable;
import java.util.Collection;

/**
 * The State Metadata of a snapshot (checkpoint or savepoint).
 */
public class CheckpointStateMetadata implements Disposable , Serializable {

	/** The checkpoint ID. */
	private final long checkpointId;

	/** The operator states. */
	private final Collection<OperatorStateMeta> operatorStateMetas;

	public CheckpointStateMetadata(long checkpointId, Collection<OperatorStateMeta> operatorStateMetas) {
		this.checkpointId = checkpointId;
		this.operatorStateMetas = operatorStateMetas;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public Collection<OperatorStateMeta> getOperatorStateMetas(){
		return operatorStateMetas;
	}

	@Override
	public void dispose() throws Exception {
		operatorStateMetas.clear();
	}

	@Override
	public String toString() {
		return "Checkpoint State Metadata " +
			", " + operatorStateMetas;
	}
}
