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

// --------------------------------------------------------------
//  THIS IS A GENERATED SOURCE FILE. DO NOT EDIT!
//  GENERATED FROM org.apache.flink.api.java.tuple.TupleGenerator.
// --------------------------------------------------------------

package org.apache.flink.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Store for {@link TagGauge}.
 */
public class TagGaugeStore {

	private final int maxSize;

	private List<TagGaugeMetric> metricValuesList;

	private final boolean clearAfterReport;

	public TagGaugeStore(int maxSize) {
		this(maxSize, false);
	}

	public TagGaugeStore(int maxSize, boolean clearAfterReport) {
		this.maxSize = maxSize;
		this.metricValuesList = new ArrayList<>();
		this.clearAfterReport = clearAfterReport;
	}

	public void addMetric(double metricValue, TagValues tagValues) {
		if (metricValuesList.size() == maxSize) {
			if (metricValuesList.size() > 0) {
				metricValuesList.remove(0);
			}
		}

		metricValuesList.add(new TagGaugeMetric(metricValue, tagValues));
	}

	public boolean isClearAfterReport() {
		return clearAfterReport;
	}

	public void reset() {
		metricValuesList.clear();
	}

	public List<TagGaugeMetric> getMetricValuesList() {
		return metricValuesList;
	}

	/**
	 * TagValues.
	 */
	public static class TagValues {

		private Map<String, String> tagValues;

		TagValues(Map<String, String> tagValues) {
			this.tagValues = tagValues;
		}

		public Map<String, String> getTagValues() {
			return tagValues;
		}
	}

	/**
	 * Build for TagValues.
	 */
	public static class TagValuesBuilder {

		private final Map<String, String> tagValuesMap;

		public TagValuesBuilder() {
			this.tagValuesMap = new HashMap<>(8);
		}

		public TagValuesBuilder addTagValue(String tag, String value) {
			tagValuesMap.put(tag, value);
			return this;
		}

		public TagValues build() {
			return new TagValues(tagValuesMap);
		}
	}

	/**
	 * TagGaugeMetric.
	 */
	public static class TagGaugeMetric {

		private final double metricValue;
		private final TagValues tagValues;

		TagGaugeMetric(double metricValue, TagValues tagValues) {
			this.metricValue = metricValue;
			this.tagValues = tagValues;
		}

		public double getMetricValue() {
			return metricValue;
		}

		public TagValues getTagValues() {
			return tagValues;
		}
	}
}
