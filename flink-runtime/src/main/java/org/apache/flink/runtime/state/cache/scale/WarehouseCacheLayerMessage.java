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

package org.apache.flink.runtime.state.cache.scale;

import org.apache.flink.metrics.warehouse.WarehouseMessage;

/**
 * Warehouse message for statebackend cache layer.
 */
public class WarehouseCacheLayerMessage extends WarehouseMessage {

	private final int taskId;
	private final String cacheName;

	private final long initialSize;
	private final long minConfiguredSize;
	private final long maxConfiguredSize;
	private final long totalConfiguredSize;

	private final double hitRate;
	private final double serializationReduceRate;
	private final long estimatedKVSize;

	private final long scaleUpCount;
	private final long scaleDownCount;
	private final long usedMemorySize;
	private final long maxMemorySize;

	public WarehouseCacheLayerMessage(
			int taskId,
			String cacheName,
			long initialSize,
			long minConfiguredSize,
			long maxConfiguredSize,
			long totalConfiguredSize,
			double hitRate,
			double serializationReduceRate,
			long scaleUpCount,
			long scaleDownCount,
			long estimatedKVSize,
			long usedMemorySize,
			long maxMemorySize) {
		this.taskId = taskId;
		this.cacheName = cacheName;
		this.initialSize = initialSize;
		this.minConfiguredSize = minConfiguredSize;
		this.maxConfiguredSize = maxConfiguredSize;
		this.totalConfiguredSize = totalConfiguredSize;
		this.hitRate = hitRate;
		this.serializationReduceRate = serializationReduceRate;
		this.scaleUpCount = scaleUpCount;
		this.scaleDownCount = scaleDownCount;
		this.estimatedKVSize = estimatedKVSize;
		this.usedMemorySize = usedMemorySize;
		this.maxMemorySize = maxMemorySize;
	}

	public int getTaskId() {
		return taskId;
	}

	public String getCacheName() {
		return cacheName;
	}

	public long getInitialSize() {
		return initialSize;
	}

	public long getMinConfiguredSize() {
		return minConfiguredSize;
	}

	public long getMaxConfiguredSize() {
		return maxConfiguredSize;
	}

	public long getTotalConfiguredSize() {
		return totalConfiguredSize;
	}

	public double getHitRate() {
		return hitRate;
	}

	public double getSerializationReduceRate() {
		return serializationReduceRate;
	}

	public long getScaleUpCount() {
		return scaleUpCount;
	}

	public long getScaleDownCount() {
		return scaleDownCount;
	}

	public long getEstimatedKVSize() {
		return estimatedKVSize;
	}

	public long getUsedMemorySize() {
		return usedMemorySize;
	}

	public long getMaxMemorySize() {
		return maxMemorySize;
	}

	@Override
	public String toString() {
		return "WarehouseCacheLayerMessage{" +
			"taskId=" + taskId +
			", cacheName='" + cacheName + '\'' +
			", initialSize=" + initialSize +
			", minConfiguredSize=" + minConfiguredSize +
			", maxConfiguredSize=" + maxConfiguredSize +
			", totalConfiguredSize=" + totalConfiguredSize +
			", hitRate=" + hitRate +
			", serializationReduceRate=" + serializationReduceRate +
			", estimatedKVSize=" + estimatedKVSize +
			", scaleUpCount=" + scaleUpCount +
			", scaleDownCount=" + scaleDownCount +
			", usedMemorySize=" + usedMemorySize +
			", maxMemorySize=" + maxMemorySize +
			'}';
	}
}
