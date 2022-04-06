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

package org.apache.flink.api.common.io;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * LoopInputSplitAssigner.
 */
public class LoopInputSplitAssigner implements InputSplitAssigner {
	private static final Logger LOG = LoggerFactory.getLogger(LoopInputSplitAssigner.class);

	private final List<InputSplit> splits = new ArrayList<>();
	private DefaultInputSplitAssigner inputSplitAssigner;
	private final Set<Integer> finishedTaskIdSet = new HashSet<>();
	private int parallelism = -1;

	public LoopInputSplitAssigner(InputSplit[] splits) {
		Collections.addAll(this.splits, splits);
		this.inputSplitAssigner = new DefaultInputSplitAssigner(this.splits);
	}

	@Override
	public InputSplit getNextInputSplit(String host, int taskId) {
		InputSplit inputSplit = inputSplitAssigner.getNextInputSplit(host, taskId);
		if (inputSplit == null) {
			finishedTaskIdSet.add(taskId);
			assert parallelism > 0;
			if (finishedTaskIdSet.size() == this.parallelism) {
				LOG.info("All tasks received finished status");
				finishedTaskIdSet.clear();
				inputSplitAssigner = new DefaultInputSplitAssigner(splits);
			}
		}
		return inputSplit;
	}

	@Override
	public void returnInputSplit(List<InputSplit> splits, int taskId) {
		inputSplitAssigner.returnInputSplit(splits, taskId);
	}

	public void setTaskParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
}
