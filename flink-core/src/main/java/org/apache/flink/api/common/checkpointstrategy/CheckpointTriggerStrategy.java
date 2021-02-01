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

package org.apache.flink.api.common.checkpointstrategy;

import java.io.Serializable;

/**
 * Strategies to trigger Checkpoint.
 */
public enum CheckpointTriggerStrategy implements Serializable {
	DEFAULT(true, true, false),

	TRIGGER_WITH_SOURCE(false, true, false),

	REVERSE_TRIGGER_WITH_SOURCE(false, true, true),

	TRIGGER_WITHOUT_SOURCE(false, false, false),

	REVERSE_TRIGGER_WITHOUT_SOURCE(false, false, true);

	private final boolean barrierMode;

	private final boolean triggerSource;

	private final boolean reverseTrigger;

	CheckpointTriggerStrategy(boolean barrierMode, boolean triggerSource, boolean reverseTrigger) {
		this.barrierMode = barrierMode;
		this.triggerSource = triggerSource;
		this.reverseTrigger = reverseTrigger;
	}

	public boolean isBarrierMode() {
		return barrierMode;
	}

	public boolean isTriggerSource() {
		return triggerSource;
	}

	public boolean isReverseTrigger() {
		return reverseTrigger;
	}
}
