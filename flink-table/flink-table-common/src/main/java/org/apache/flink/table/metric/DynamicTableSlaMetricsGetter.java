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

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Used by dynamic sink to get sla metrics.
 */
public class DynamicTableSlaMetricsGetter implements SinkMetricsGetter<RowData>, Serializable {
	private static final long serialVersionUID = 1L;

	private final SinkMetricsOptions metricsOptions;
	private final RowData.FieldGetter[] fieldGetters;

	public DynamicTableSlaMetricsGetter(SinkMetricsOptions metricsOptions, RowData.FieldGetter[] fieldGetters) {
		this.metricsOptions = metricsOptions;
		this.fieldGetters = fieldGetters;
	}

	@Override
	public long getEventTs(RowData record) {
		int eventTsIdx = metricsOptions.getEventTsColIndex();
		return (long) Objects.requireNonNull(fieldGetters[eventTsIdx].getFieldOrNull(record),
			"Get null of event ts column of index " + eventTsIdx);
	}

	@Override
	public Map<String, String> getTags(RowData record) {
		List<Integer> tagIndices = metricsOptions.getTagNameIndices();
		Map<String, String> tags = new HashMap<>();
		if (tagIndices != null && !tagIndices.isEmpty()) {
			Iterator<Integer> tagIdxIter = tagIndices.iterator();
			for (String tagName : metricsOptions.getTagNames()) {
				int idx = tagIdxIter.next();
				String tagValue = Objects.requireNonNull(fieldGetters[idx].getFieldOrNull(record),
					"get null of tag name " + tagName).toString();
				tags.put(tagName, tagValue);
			}
		}
		return tags;
	}
}
