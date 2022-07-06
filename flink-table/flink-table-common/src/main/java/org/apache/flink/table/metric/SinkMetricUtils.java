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

package org.apache.flink.table.metric;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.TagBucketHistogram;
import org.apache.flink.table.api.TableSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_NUMBER;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_SERIES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_SIZE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_EVENT_TS_NAME;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_EVENT_TS_WRITEABLE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_PROPS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_QUANTILES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_TAGS_NAMES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_TAGS_WRITEABLE;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utils for sink table metrics.
 */
public class SinkMetricUtils {

	private static final Logger LOG = LoggerFactory.getLogger(SinkMetricUtils.class);

	public static final String SINK_EVENT_TIME_LATENCY = "sinkConnectorLatency";

	private SinkMetricUtils() {}

	public static SinkMetricsOptions getSinkMetricsOptions(ReadableConfig config, TableSchema schema) {
		if (!config.getOptional(SINK_METRICS_EVENT_TS_NAME).isPresent()) {
			return SinkMetricsOptions.builder().build();
		}

		// get and check if event-ts column name is in the schema
		SinkMetricsOptions.Builder builder = SinkMetricsOptions.builder();
		String colName = config.getOptional(SINK_METRICS_EVENT_TS_NAME).get();
		Optional<Integer> index = schema.getFieldNameIndex(colName);
		checkState(index.isPresent(), "The specified event-ts column name " +
			colName + " is not in the table!");
		builder.setEventTsColName(colName);
		builder.setEventTsColIndex(index.get());
		builder.setEventTsWriteable(config.get(SINK_METRICS_EVENT_TS_WRITEABLE));

		// check tag column names are in the schema if they are configured
		if (config.getOptional(SINK_METRICS_TAGS_NAMES).isPresent()) {
			List<String> tagNames = config.getOptional(SINK_METRICS_TAGS_NAMES).get();
			builder.setTagNames(tagNames);

			List<Integer> tagNameIndices = new ArrayList<>(tagNames.size());
			for (int i = 0; i < tagNames.size(); i++) {
				String tagName = tagNames.get(i);
				index = schema.getFieldNameIndex(tagName);
				checkState(index.isPresent(), "The tag name " + tagName + " is not in the table!");
				tagNameIndices.add(index.get());
			}
			builder.setTagNameIndices(tagNameIndices);
			builder.setTagWriteable(config.get(SINK_METRICS_TAGS_WRITEABLE));
		}

		config.getOptional(SINK_METRICS_QUANTILES).ifPresent(builder::setPercentiles);
		config.getOptional(SINK_METRICS_PROPS).ifPresent(builder::setProps);
		config.getOptional(SINK_METRICS_BUCKET_SIZE).ifPresent(duration -> builder.setBucketsSize(duration.getSeconds()));
		config.getOptional(SINK_METRICS_BUCKET_NUMBER).ifPresent(builder::setBucketsNum);
		config.getOptional(SINK_METRICS_BUCKET_SERIES).ifPresent(builder::setBuckets);
		return builder.build();
	}

	public static TagBucketHistogram initLatencyMetrics(SinkMetricsOptions sinkMetricsOptions, MetricGroup metricGroup) {
		TagBucketHistogram.TagBucketHistogramBuilder builder = TagBucketHistogram.builder();
		List<Double> quantiles = sinkMetricsOptions.getPercentiles();
		if (quantiles != null && !quantiles.isEmpty()) {
			builder.setQuantiles(quantiles);
		}
		if (sinkMetricsOptions.getBucketsSize() > 0 && sinkMetricsOptions.getBucketsNum() > 0) {
			List<Long> buckets = new ArrayList<>(sinkMetricsOptions.getBucketsNum());
			long sum = 0L;
			for (int i = 0; i < sinkMetricsOptions.getBucketsNum(); i++) {
				buckets.add(sum);
				sum += sinkMetricsOptions.getBucketsSize();
			}
			builder.setBuckets(buckets);
		}
		List<Long> buckets = sinkMetricsOptions.getBuckets();
		if (buckets != null && !buckets.isEmpty()) {
			builder.setBuckets(buckets);
		}
		Map<String, String> props = sinkMetricsOptions.getProps();
		if (props != null && !props.isEmpty()) {
			builder.setProps(props);
		}
		TagBucketHistogram histogram = builder.build();
		return metricGroup.histogram(SINK_EVENT_TIME_LATENCY, histogram);
	}

	public static void updateLatency(
			TagBucketHistogram latencyHistogram,
			Map<String, String> tags,
			long latency) {
		try {
			if (tags != null && !tags.isEmpty()) {
				latencyHistogram.update(latency, tags);
			}
			// need to calculate a connector overall latency either tags are configured or not
			latencyHistogram.update(latency);
		} catch (Throwable throwable) {
			LOG.error("Failed to update sink latency", throwable);
		}
	}

}
