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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.ExternalBlockSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * An implementation of {@link ExternalBlockSubpartitionViewScheduler} used to decide the scheduling order of
 * {@link ExternalBlockSubpartitionView}s based on credits.
 * Since the credit of each {@link ExternalBlockSubpartitionView} may be increased
 * by {@link org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel} dynamically,
 * there is a trade-off between objective and performance. Groups {@link ExternalBlockSubpartitionView}
 * by {@link ResultPartitionID} and sums the total credits of each result partition,
 * then serves {@link ExternalBlockSubpartitionView}s of the result partition with the max total credits.
 * Once subpartition views of the head result partition are all served, recalculates the next result partition
 * with the max total credits and serves again.
 */
public class CreditBasedSubpartitionViewScheduler implements ExternalBlockSubpartitionViewScheduler {

	@VisibleForTesting
	protected final Map<ResultPartitionID, ResultPartitionNode> resultPartitionNodeMap;

	@VisibleForTesting
	protected final ArrayDeque<ExternalBlockSubpartitionView> cacheList;

	public CreditBasedSubpartitionViewScheduler() {
		this.resultPartitionNodeMap = new HashMap<>(16);
		this.cacheList = new ArrayDeque<>(16);
	}

	@Override
	public void addToSchedule(ExternalBlockSubpartitionView subpartitionView) {
		ResultPartitionNode node = resultPartitionNodeMap.get(subpartitionView.getResultPartitionID());
		if (node == null) {
			node = new ResultPartitionNode(subpartitionView.getResultPartitionID());
			resultPartitionNodeMap.put(subpartitionView.getResultPartitionID(), node);
		}
		// {@link PriorityQueue} will always return true.
		node.subpartitionViews.offer(subpartitionView);
	}

	@Override
	public ExternalBlockSubpartitionView schedule() {
		while (!cacheList.isEmpty() || !resultPartitionNodeMap.isEmpty()) {
			if (!cacheList.isEmpty()) {
				// no need to check null pointer because ArrayDeque prohibits null pointer while offer()
				return cacheList.poll();
			}

			if (resultPartitionNodeMap.isEmpty()) {
				return null;
			}

			// Search the result partition with the max total credits.
			long currentTimestamp = System.currentTimeMillis();
			ResultPartitionNode nodeWithMaxCredits = null;
			long maxCredit = 0L;
			Iterator<Map.Entry<ResultPartitionID, ResultPartitionNode>> iterator =
				resultPartitionNodeMap.entrySet().iterator();
			while (iterator.hasNext()) {
				ResultPartitionNode currentNode = iterator.next().getValue();
				currentNode.updateTotalCredits(currentTimestamp);
				if (currentNode.totalCredits < 1) {
					iterator.remove();
					continue;
				}
				if (currentNode.totalCredits > maxCredit) {
					maxCredit = currentNode.totalCredits;
					nodeWithMaxCredits = currentNode;
				}
			}
			if (nodeWithMaxCredits != null) {
				// Even though its subpartition views are drained off, its ResultPartitionNode will be recycled
				// in the next search.
				for (int i = nodeWithMaxCredits.subpartitionViews.size(); i > 0; i--) {
					cacheList.offer(nodeWithMaxCredits.subpartitionViews.poll());
				}
				nodeWithMaxCredits.timestamp = currentTimestamp;
			}
		}
		return null;
	}

	/**
	 * Groups {@link ExternalBlockSubpartitionView} by {@link ResultPartitionID} and sum the total credits on demand.
	 */
	private static class ResultPartitionNode {

		/** Total credits of all the subpartition views. */
		long totalCredits = 0L;

		/** Don't need to use {@link java.util.concurrent.BlockingQueue} since the framework will make sure that
		 * {@link #addToSchedule} and {@link #schedule} will NOT be called concurrently. */
		final PriorityQueue<ExternalBlockSubpartitionView> subpartitionViews;

		final ResultPartitionID resultPartitionID;

		/** The last timestamp of this result partition been scheduled. */
		long timestamp;

		ResultPartitionNode(ResultPartitionID resultPartitionID) {
			this.resultPartitionID = resultPartitionID;
			this.subpartitionViews = new PriorityQueue<>(16, new SimpleLocalityBasedSubpartitionViewComparator());
			this.timestamp = System.currentTimeMillis();
		}

		void updateTotalCredits(long currentTimestamp) {
			long newTotalCredits = 0L;
			for (ExternalBlockSubpartitionView subpartitionView : subpartitionViews) {
				newTotalCredits += subpartitionView.getCreditUnsafe();
			}
			// Revises total credits considering its idle duration in order to avoid starvation.
			totalCredits = newTotalCredits * (1 + (currentTimestamp - timestamp) / 10_000L);
		}
	}

	/**
	 * Comparator for sub partitions.
	 */
	private static class SimpleLocalityBasedSubpartitionViewComparator implements Comparator<ExternalBlockSubpartitionView> {
		@Override
		public int compare(ExternalBlockSubpartitionView o1, ExternalBlockSubpartitionView o2) {
			return (o1.getSubpartitionIndex() - o2.getSubpartitionIndex());
		}
	}
}
