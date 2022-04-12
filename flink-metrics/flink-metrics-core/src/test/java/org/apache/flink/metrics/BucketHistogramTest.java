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

package org.apache.flink.metrics;

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for {@link TagBucketHistogram}.
 */
public class BucketHistogramTest extends TestLogger {

	private static final int TEST_NUM = 10000;
	private static final int MAX_DATA_SIZE = 3000;
	private static final long MAX_DATA_VALUE = 1200L;
	private static final long MAX_BUCKET_POINT = 1800L;
	private static final int MAX_BUCKET_NUM = 1000;
	private static final int MAX_TAGS_NUM = 30;

	@Test
	public void testDefaultBucketsHistogram() {
		for (int i = 0; i < TEST_NUM; i++) {
			testHistogram(TagBucketHistogram.DEFAULT_BUCKETS, TagBucketHistogram.DEFAULT_QUANTILES);
		}
	}

	@Test
	public void testArbitraryBucketsHistogram() {
		for (int i = 0; i < TEST_NUM; i++) {
			int size = ThreadLocalRandom.current().nextInt(2, MAX_BUCKET_NUM);
			long[] buckets = new long[size];
			Set<Long> points = new HashSet<>();
			for (int j = 1; j < buckets.length; j++) {
				long p;
				do {
					p = ThreadLocalRandom.current().nextLong(1, MAX_BUCKET_POINT);
				} while (points.contains(p));
				buckets[j] = p;
				points.add(p);
			}
			testHistogram(buckets, TagBucketHistogram.DEFAULT_QUANTILES);
		}
	}

	@Test
	public void testTagBucketHistogram() {
		TagBucketHistogram histogram = TagBucketHistogram.builder().build();
		for (int i = 0; i < TEST_NUM / 10; i++) {
			int tagNum = ThreadLocalRandom.current().nextInt(2, MAX_TAGS_NUM);
			Map<Map<String, String>, List<Long>> dataSet = new HashMap<>();
			for (int j = 0; j < tagNum; j++) {
				Map<String, String> tag = new HashMap<>();
				tag.put("k1", "v1" + j);
				tag.put("k2", "v2" + j);
				int size = ThreadLocalRandom.current().nextInt(1, MAX_DATA_SIZE);
				List<Long> data = new ArrayList<>(size);
				for (int k = 0; k < size; k++) {
					long value = ThreadLocalRandom.current().nextLong(0, MAX_DATA_VALUE);
					data.add(value);
					histogram.update(value, tag);
				}
				dataSet.put(tag, data);
			}
			Map<Map<String, String>, BucketHistogramStatistic> statisticMap = histogram.getStatisticsOfAllTags();
			for (Map.Entry<Map<String, String>, BucketHistogramStatistic> entry : statisticMap.entrySet()) {
				Map<String, String> tag = entry.getKey();
				BucketHistogramStatistic statistic = entry.getValue();
				Map<Double, Double> actual = brutalForceCalc(dataSet.get(tag), TagBucketHistogram.DEFAULT_QUANTILES, TagBucketHistogram.DEFAULT_BUCKETS);
				for (double percentile : actual.keySet()) {
					Assert.assertEquals(actual.get(percentile), statistic.getQuantile(percentile), 0.0001);
				}
			}
		}
	}

	private static void testHistogram(long[] buckets, double[] quantiles) {
		TagBucketHistogram.BucketHistogram histogram = new TagBucketHistogram.BucketHistogram(buckets, quantiles);
		int size = ThreadLocalRandom.current().nextInt(1, MAX_DATA_SIZE);
		List<Long> data = new ArrayList<>(size);
		for (int i = 0; i < size; i++) {
			long value = ThreadLocalRandom.current().nextLong(0, MAX_DATA_VALUE);
			histogram.update(value);
			data.add(value);
		}
		Map<Double, Double> actual = brutalForceCalc(data, quantiles, buckets);
		BucketHistogramStatistic statistic = (BucketHistogramStatistic) histogram.getStatistics();
		for (Map.Entry<Double, Double> entry : actual.entrySet()) {
			Assert.assertEquals(entry.getValue(), statistic.getQuantile(entry.getKey()), 0.0001);
		}
	}

	private static Map<Double, Double> brutalForceCalc(List<Long> data, double[] percentiles, long[] buckets) {
		Map<Double, Double> ret = new HashMap<>();
		Collections.sort(data);
		for (double percentile : percentiles) {
			int target = (int) Math.ceil(percentile * data.size());
			long actual = data.get(target - 1);
			int pos = Arrays.binarySearch(buckets, actual);
			pos = pos >= 0 ? pos : -pos - 2;
			ret.put(percentile, BucketHistogramStatistic.getBucketMean(buckets, pos));
		}
		return ret;
	}
}
