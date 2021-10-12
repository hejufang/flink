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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.cache.scale.ScaleCondition;

import java.util.Objects;

/**
 * Cache-related configuration items.
 */
public class CacheConfiguration {

	//-------------------------- cache configuration --------------------------

	private boolean enableCache;
	private String cacheStrategy;
	private MemorySize cacheInitialSize;
	private int sampleCount;

	//--------------------- memory manager configuration ----------------------

	private MemorySize maxHeapSize;
	private MemorySize blockSize;
	private double scaleUpRatio;
	private double scaleDownRatio;

	//------------------- heap status monitor configuration -------------------

	private long heapMonitorInterval;

	//-------------------------- scaling configuration ----------------------//
	private boolean enableScale;
	private int scaleNum;
	private MemorySize cacheMinSize;
	private MemorySize cacheMaxSize;
	private ScaleCondition scaleCondition;
	private double scaleUpRetainedSizeWeight;
	private double scaleUpLoadSuccessCountWeight;
	private double scaleDownRetainedSizeWeight;
	private double scaleDownLoadSuccessCountWeight;
	private long incrementalRemoveCount;

	public CacheConfiguration(
			boolean enableCache,
			String cacheStrategy,
			MemorySize cacheInitialSize,
			int sampleCount,
			MemorySize maxHeapSize,
			MemorySize blockSize,
			double scaleUpRatio,
			double scaleDownRatio,
			long heapMonitorInterval,
			boolean enableScale,
			int scaleNum,
			MemorySize cacheMinSize,
			MemorySize cacheMaxSize,
			ScaleCondition scaleCondition,
			double scaleUpRetainedSizeWeight,
			double scaleUpLoadSuccessCountWeight,
			double scaleDownRetainedSizeWeight,
			double scaleDownLoadSuccessCountWeight,
			long incrementalRemoveCount) {
		this.enableCache = enableCache;
		this.cacheStrategy = cacheStrategy;
		this.cacheInitialSize = cacheInitialSize;
		this.sampleCount = sampleCount;
		this.maxHeapSize = maxHeapSize;
		this.blockSize = blockSize;
		this.scaleUpRatio = scaleUpRatio;
		this.scaleDownRatio = scaleDownRatio;
		this.heapMonitorInterval = heapMonitorInterval;
		this.enableScale = enableScale;
		this.scaleNum = scaleNum;
		this.cacheMinSize = cacheMinSize;
		this.cacheMaxSize = cacheMaxSize;
		this.scaleCondition = scaleCondition;
		this.scaleUpRetainedSizeWeight = scaleUpRetainedSizeWeight;
		this.scaleUpLoadSuccessCountWeight = scaleUpLoadSuccessCountWeight;
		this.scaleDownRetainedSizeWeight = scaleDownRetainedSizeWeight;
		this.scaleDownLoadSuccessCountWeight = scaleDownLoadSuccessCountWeight;
		this.incrementalRemoveCount = incrementalRemoveCount;
	}

	public boolean isEnableCache() {
		return enableCache;
	}

	public void setEnableCache(boolean enableCache) {
		this.enableCache = enableCache;
	}

	public String getCacheStrategy() {
		return cacheStrategy;
	}

	public void setCacheStrategy(String cacheStrategy) {
		this.cacheStrategy = cacheStrategy;
	}

	public MemorySize getCacheInitialSize() {
		return cacheInitialSize;
	}

	public void setCacheInitialSize(MemorySize cacheInitialSize) {
		this.cacheInitialSize = cacheInitialSize;
	}

	public int getSampleCount() {
		return sampleCount;
	}

	public void setSampleCount(int sampleCount) {
		this.sampleCount = sampleCount;
	}

	public MemorySize getMaxHeapSize() {
		return maxHeapSize;
	}

	public void setMaxHeapSize(MemorySize maxHeapSize) {
		this.maxHeapSize = maxHeapSize;
	}

	public MemorySize getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(MemorySize blockSize) {
		this.blockSize = blockSize;
	}

	public double getScaleUpRatio() {
		return scaleUpRatio;
	}

	public void setScaleUpRatio(double scaleUpRatio) {
		this.scaleUpRatio = scaleUpRatio;
	}

	public double getScaleDownRatio() {
		return scaleDownRatio;
	}

	public void setScaleDownRatio(double scaleDownRatio) {
		this.scaleDownRatio = scaleDownRatio;
	}

	public long getHeapMonitorInterval() {
		return heapMonitorInterval;
	}

	public void setHeapMonitorInterval(long heapMonitorInterval) {
		this.heapMonitorInterval = heapMonitorInterval;
	}

	public boolean isEnableScale() {
		return enableScale;
	}

	public void setEnableScale(boolean enableScale) {
		this.enableScale = enableScale;
	}

	public int getScaleNum() {
		return scaleNum;
	}

	public void setScaleNum(int scaleNum) {
		this.scaleNum = scaleNum;
	}

	public MemorySize getCacheMinSize() {
		return cacheMinSize;
	}

	public void setCacheMinSize(MemorySize cacheMinSize) {
		this.cacheMinSize = cacheMinSize;
	}

	public MemorySize getCacheMaxSize() {
		return cacheMaxSize;
	}

	public void setCacheMaxSize(MemorySize cacheMaxSize) {
		this.cacheMaxSize = cacheMaxSize;
	}

	public ScaleCondition getScaleCondition() {
		return scaleCondition;
	}

	public void setScaleCondition(ScaleCondition scaleCondition) {
		this.scaleCondition = scaleCondition;
	}

	public double getScaleUpRetainedSizeWeight() {
		return scaleUpRetainedSizeWeight;
	}

	public void setScaleUpRetainedSizeWeight(double scaleUpRetainedSizeWeight) {
		this.scaleUpRetainedSizeWeight = scaleUpRetainedSizeWeight;
	}

	public double getScaleUpLoadSuccessCountWeight() {
		return scaleUpLoadSuccessCountWeight;
	}

	public void setScaleUpLoadSuccessCountWeight(double scaleUpLoadSuccessCountWeight) {
		this.scaleUpLoadSuccessCountWeight = scaleUpLoadSuccessCountWeight;
	}

	public double getScaleDownRetainedSizeWeight() {
		return scaleDownRetainedSizeWeight;
	}

	public void setScaleDownRetainedSizeWeight(double scaleDownRetainedSizeWeight) {
		this.scaleDownRetainedSizeWeight = scaleDownRetainedSizeWeight;
	}

	public double getScaleDownLoadSuccessCountWeight() {
		return scaleDownLoadSuccessCountWeight;
	}

	public void setScaleDownLoadSuccessCountWeight(double scaleDownLoadSuccessCountWeight) {
		this.scaleDownLoadSuccessCountWeight = scaleDownLoadSuccessCountWeight;
	}

	public long getIncrementalRemoveCount() {
		return incrementalRemoveCount;
	}

	public void setIncrementalRemoveCount(long incrementalRemoveCount) {
		this.incrementalRemoveCount = incrementalRemoveCount;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CacheConfiguration that = (CacheConfiguration) o;
		return enableCache == that.enableCache &&
			sampleCount == that.sampleCount &&
			Double.compare(that.scaleUpRatio, scaleUpRatio) == 0 &&
			Double.compare(that.scaleDownRatio, scaleDownRatio) == 0 &&
			heapMonitorInterval == that.heapMonitorInterval &&
			enableScale == that.enableScale &&
			scaleNum == that.scaleNum &&
			Objects.equals(scaleCondition, that.scaleCondition) &&
			Double.compare(that.scaleUpRetainedSizeWeight, scaleUpRetainedSizeWeight) == 0 &&
			Double.compare(that.scaleUpLoadSuccessCountWeight, scaleUpLoadSuccessCountWeight) == 0 &&
			Double.compare(that.scaleDownRetainedSizeWeight, scaleDownRetainedSizeWeight) == 0 &&
			Double.compare(that.scaleDownLoadSuccessCountWeight, scaleDownLoadSuccessCountWeight) == 0 &&
			Objects.equals(cacheStrategy, that.cacheStrategy) &&
			Objects.equals(cacheInitialSize, that.cacheInitialSize) &&
			Objects.equals(maxHeapSize, that.maxHeapSize) &&
			Objects.equals(blockSize, that.blockSize) &&
			Objects.equals(cacheMinSize, that.cacheMinSize) &&
			Objects.equals(cacheMaxSize, that.cacheMaxSize) &&
			Objects.equals(incrementalRemoveCount, that.incrementalRemoveCount);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			enableCache,
			cacheStrategy,
			cacheInitialSize,
			sampleCount,
			maxHeapSize,
			blockSize,
			scaleUpRatio,
			scaleDownRatio,
			heapMonitorInterval,
			enableScale,
			scaleNum,
			cacheMinSize,
			cacheMaxSize,
			scaleCondition,
			scaleUpRetainedSizeWeight,
			scaleUpLoadSuccessCountWeight,
			scaleDownRetainedSizeWeight,
			scaleDownLoadSuccessCountWeight,
			incrementalRemoveCount);
	}

	public static CacheConfiguration fromConfiguration(Configuration configuration) {
		boolean enableCache = configuration.getBoolean(CacheConfigurableOptions.CACHE_ENABLED);
		String cacheStrategy = configuration.getString(CacheConfigurableOptions.CACHE_STRATEGY);
		MemorySize cacheInitialSize = configuration.get(CacheConfigurableOptions.CACHE_INITIAL_SIZE);
		int sampleCount = configuration.getInteger(CacheConfigurableOptions.CACHE_SAMPLE_COUNT);

		//-------------------------- memory manager configuration --------------------------//
		MemorySize maxHeapSize = configuration.get(CacheConfigurableOptions.CACHE_MAX_HEAP_SIZE);
		MemorySize blockSize = configuration.get(CacheConfigurableOptions.CACHE_BLOCK_SIZE);
		double scaleUpRatio = configuration.getDouble(CacheConfigurableOptions.CACHE_SCALE_UP_RATIO);
		double scaleDownRatio = configuration.get(CacheConfigurableOptions.CACHE_SCALE_DOWN_RATIO);

		//-------------------------- heap status monitor configuration ---------------------//
		long heapMonitorInterval = configuration.getLong(CacheConfigurableOptions.HEAP_MONITOR_INTERVAL);

		//-------------------------- scaling manager configuration ----------------------//
		boolean enableScale = configuration.getBoolean(CacheConfigurableOptions.CACHE_SCALE_ENABLE);
		int scaleNum = configuration.getInteger(CacheConfigurableOptions.CACHE_SCALE_NUM);
		MemorySize cacheMinSize = configuration.get(CacheConfigurableOptions.CACHE_MIN_SIZE);
		MemorySize cacheMaxSize = configuration.get(CacheConfigurableOptions.CACHE_MAX_SIZE);
		long maxGcTimeThreshold = configuration.getLong(CacheConfigurableOptions.MAX_GC_TIME_THRESHOLD);
		long avgGcTimeThreshold = configuration.getLong(CacheConfigurableOptions.AVG_GC_TIME_THRESHOLD);
		long gcCountThreshold = configuration.getLong(CacheConfigurableOptions.GC_COUNT_THRESHOLD);
		double lowHeapThreshold = configuration.getDouble(CacheConfigurableOptions.LOW_HEAP_THRESHOLD);
		ScaleCondition scaleCondition = new ScaleCondition(maxGcTimeThreshold, avgGcTimeThreshold, gcCountThreshold, lowHeapThreshold);
		double scaleUpRetainedSizeWeight = configuration.getDouble(CacheConfigurableOptions.SCALE_UP_RETAINED_SIZE_WEIGHT);
		double scaleUpLoadSuccessCountWeight = configuration.getDouble(CacheConfigurableOptions.SCALE_UP_LOAD_SUCCESS_COUNT_WEIGHT);
		double scaleDownRetainedSizeWeight = configuration.getDouble(CacheConfigurableOptions.SCALE_DOWN_RETAINED_SIZE_WEIGHT);
		double scaleDownLoadSuccessCountWeight = configuration.getDouble(CacheConfigurableOptions.SCALE_DOWN_LOAD_SUCCESS_COUNT_WEIGHT);
		long incrementalRemoveCount = configuration.getLong(CacheConfigurableOptions.CACHE_INCREMENTAL_REMOVE_COUNT);

		return new CacheConfiguration(
			enableCache,
			cacheStrategy,
			cacheInitialSize,
			sampleCount,
			maxHeapSize,
			blockSize,
			scaleUpRatio,
			scaleDownRatio,
			heapMonitorInterval,
			enableScale,
			scaleNum,
			cacheMinSize,
			cacheMaxSize,
			scaleCondition,
			scaleUpRetainedSizeWeight,
			scaleUpLoadSuccessCountWeight,
			scaleDownRetainedSizeWeight,
			scaleDownLoadSuccessCountWeight,
			incrementalRemoveCount);
	}
}
