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

package org.apache.flink.streaming.runtime.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PriorityTwoInputSelectionHandler.
 */
public class PriorityTwoInputSelectionHandler extends TwoInputSelectionHandler {

	private static final Logger LOG = LoggerFactory.getLogger(PriorityTwoInputSelectionHandler.class);
	private static final int NO_PRIORITY_INPUT_SIDE = -1;

	private int priorityInputSide = NO_PRIORITY_INPUT_SIDE;

	public PriorityTwoInputSelectionHandler(TwoInputSelectionHandler selectionHandler) {
		super(selectionHandler.inputSelectable);
	}

	@Override
	int selectNextInputIndex(int lastReadInputIndex) {
		if (priorityInputSide != NO_PRIORITY_INPUT_SIDE) {
			return priorityInputSide;
		}
		return super.selectNextInputIndex(lastReadInputIndex);
	}

	public void setPriorityInputSide(int priorityInputSide) {
		this.priorityInputSide = priorityInputSide;
		LOG.info("The operator priority input side is set to be {}.", this.priorityInputSide);
	}

	public void unsetPriorityInputSide() {
		this.priorityInputSide = NO_PRIORITY_INPUT_SIDE;
		LOG.info("The operator priority input side is unset to be {}.", this.priorityInputSide);
	}
}
