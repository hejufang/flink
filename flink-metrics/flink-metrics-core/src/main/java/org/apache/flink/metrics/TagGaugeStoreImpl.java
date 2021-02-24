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
import java.util.List;

/**
 * Store for {@link TagGauge}.
 */
public class TagGaugeStoreImpl implements TagGaugeStore {

	private final int maxSize;

	private final List<TagGaugeMetric> metricValuesList;

	private final boolean clearAfterReport;

	private final boolean clearWhenFull;

	public TagGaugeStoreImpl(int maxSize, boolean clearAfterReport, boolean clearWhenFull) {
		this.maxSize = maxSize;
		this.metricValuesList = new ArrayList<>();
		this.clearAfterReport = clearAfterReport;
		this.clearWhenFull = clearWhenFull;
	}

	public void addMetric(double metricValue, TagValues tagValues) {
		if (metricValuesList.size() == maxSize) {
			if (clearWhenFull) {
				metricValuesList.clear();
			} else {
				if (metricValuesList.size() > 0) {
					metricValuesList.remove(0);
				}
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

	@Override
	public void metricReported() {
		if (isClearAfterReport()) {
			reset();
		}
	}
}
