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

package org.apache.flink.metrics.warehouse;

import java.util.Map;

/**
 * Record connector sink latency.
 */
public class ConnectorSinkMetricMessage extends WarehouseMessage {

	private Map<String, String> tags;
	private Map<Double, Double> percentiles;
	private long count;
	private Map<String, String> props;

	public ConnectorSinkMetricMessage() {}

	public ConnectorSinkMetricMessage(
			Map<String, String> tags,
			Map<Double, Double> percentiles,
			long count,
			Map<String, String> props) {
		this.tags = tags;
		this.percentiles = percentiles;
		this.count = count;
		this.props = props;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public Map<Double, Double> getPercentiles() {
		return percentiles;
	}

	public long getCount() {
		return count;
	}

	public Map<String, String> getProps() {
		return props;
	}

	@Override
	public String toString() {
		return "SinkMetricMessage{" +
			"tags=" + tags +
			", percentiles=" + percentiles +
			", count=" + count +
			", props=" + props +
			'}';
	}
}
