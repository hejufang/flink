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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.cache.memory.CacheMemoryManager;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Cache-related configuration items.
 */
public class CacheConfigurableOptions {

	//--------------------------------------------------------------------------
	// Cache configuration
	//--------------------------------------------------------------------------

	public static final ConfigOption<Boolean> CACHE_ENABLED =
		key("state.backend.cache.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to enable the caching function of StateBackend, the default value is false.");

	public static final ConfigOption<String> CACHE_STRATEGY =
		key("state.backend.cache.strategy")
			.stringType()
			.defaultValue("LRU")
			.withDescription("The caching strategy used by the cache. The LRU strategy is used by default.");

	public static final ConfigOption<MemorySize> CACHE_INITIAL_SIZE =
		key("state.backend.cache.initial.size")
			.memoryType()
			.defaultValue(MemorySize.ofMebiBytes(64L))
			.withDescription("The initial size of the cache. The default is 64MB.");

	public static final ConfigOption<Integer> CACHE_SAMPLE_COUNT =
		key("state.backend.cache.sample.count")
			.intType()
			.defaultValue(1000)
			.withDescription("The state size sampling interval in the cache.");

	//--------------------------------------------------------------------------
	// Memory manager configuration
	//--------------------------------------------------------------------------

	public static final ConfigOption<MemorySize> CACHE_MAX_HEAP_SIZE =
		key("state.backend.cache.maxHeapSize")
			.memoryType()
			.defaultValue(MemorySize.ZERO)
			.withDescription("The maximum memory that Cache can use, the default value is 0.");

	public static final ConfigOption<MemorySize> CACHE_BLOCK_SIZE =
		key("state.backend.cache.blockSize")
			.memoryType()
			.defaultValue(CacheMemoryManager.DEFAULT_BLOCK_SIZE)
			.withDescription("The smallest unit of memory management.");

	public static final ConfigOption<Double> CACHE_SCALE_UP_RATIO =
		key("state.backend.cache.scaleUpRatio")
			.doubleType()
			.defaultValue(0.1)
			.withDescription("The ratio of each scale up.");

	public static final ConfigOption<Double> CACHE_SCALE_DOWN_RATIO =
		key("state.backend.cache.scaleDownRatio")
			.doubleType()
			.defaultValue(0.1)
			.withDescription("The ratio of each scale down.");

	//--------------------------------------------------------------------------
	// Heap status monitor configuration
	//--------------------------------------------------------------------------

	public static final ConfigOption<Long> HEAP_MONITOR_INTERVAL =
		key("state.backend.cache.heap.monitor.interval")
			.longType()
			.defaultValue(60000L)
			.withDescription("The monitoring period of the heap status. " +
				"The default monitoring time is 60 seconds.");

	//--------------------------------------------------------------------------
	// Scaling manager configuration
	//--------------------------------------------------------------------------

	public static final ConfigOption<Boolean> CACHE_SCALE_ENABLE =
		key("state.backend.cache.scale.enable")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to enable the scale feature for the cache layer, the default value is false.");

	public static final ConfigOption<Integer> CACHE_SCALE_NUM =
		key("state.backend.cache.scale.num")
			.intType()
			.defaultValue(3)
			.withDescription("The number of TopN caches that need to be selected for each scale down/up operation.");

	public static final ConfigOption<MemorySize> CACHE_MIN_SIZE =
		key("state.backend.cache.minSize")
			.memoryType()
			.defaultValue(MemorySize.ZERO)
			.withDescription("The minimum space reserved by the cache in the memory. The cache cannot " +
				"be lower than this value when it is scaled down.");

	public static final ConfigOption<MemorySize> CACHE_MAX_SIZE =
		key("state.backend.cache.maxSize")
			.memoryType()
			.defaultValue(MemorySize.ofMebiBytes(64L))
			.withDescription("The maximum space reserved by the cache in the memory. The cache cannot " +
				"be larger than this value when it is scaled up.");

	public static final ConfigOption<Long> MAX_GC_TIME_THRESHOLD =
		key("state.backend.cache.maxGcTimeThreshold")
			.longType()
			.defaultValue(5000L)
			.withDescription("The maximum time for single GC in a heap monitoring cycle. If the " +
				"threshold is exceeded, the cache scale down operation will be triggered.");

	public static final ConfigOption<Long> AVG_GC_TIME_THRESHOLD =
		key("state.backend.cache.avgGcTimeThreshold")
			.longType()
			.defaultValue(2000L)
			.withDescription("The average time allowed for GC in a heap monitoring cycle. If the " +
				"threshold is exceeded, the cache scale down operation will be triggered.");

	public static final ConfigOption<Long> GC_COUNT_THRESHOLD =
		key("state.backend.cache.gcCountThreshold")
			.longType()
			.defaultValue(20L)
			.withDescription("The maximum count allowed for GC in a heap monitoring cycle. If the " +
				"threshold is exceeded, the cache scale down operation will be triggered.");

	public static final ConfigOption<Double> LOW_HEAP_THRESHOLD =
		key("state.backend.cache.lowHeapThreshold")
			.doubleType()
			.defaultValue(0.3)
			.withDescription("When the current Heap usage is lower than the threshold, the cache " +
				"scale up operation will be triggered.");

	public static final ConfigOption<Double> SCALE_UP_RETAINED_SIZE_WEIGHT =
		key("state.backend.cache.scale.up.retained.size.weight")
			.doubleType()
			.defaultValue(-0.3)
			.withDescription("The weight value of retained size in the scale up operation.");

	public static final ConfigOption<Double> SCALE_UP_LOAD_SUCCESS_COUNT_WEIGHT =
		key("state.backend.cache.scale.up.load.success.count.weight")
			.doubleType()
			.defaultValue(0.7)
			.withDescription("The weight value of load-success-count in the scale up operation.");

	public static final ConfigOption<Double> SCALE_DOWN_RETAINED_SIZE_WEIGHT =
		key("state.backend.cache.scale.down.retained.size.weight")
			.doubleType()
			.defaultValue(0.7)
			.withDescription("The weight value of retained size in the scale down operation.");

	public static final ConfigOption<Double> SCALE_DOWN_LOAD_SUCCESS_COUNT_WEIGHT =
		key("state.backend.cache.scale.down.load.success.count.weight")
			.doubleType()
			.defaultValue(-0.3)
			.withDescription("The weight value of load-success-count in the scale down operation.");

	public static final ConfigOption<Long> CACHE_INCREMENTAL_REMOVE_COUNT =
		key("state.backend.cache.incremental.remove.count")
			.longType()
			.defaultValue(3L)
			.withDescription("The maximum number of cache remove data at a time.");
}
