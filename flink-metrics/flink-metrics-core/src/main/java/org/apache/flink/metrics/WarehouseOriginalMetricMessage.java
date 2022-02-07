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

import org.apache.flink.metrics.warehouse.WarehouseMessage;

import java.util.Objects;

/**
 * Used for reporting Flink's metrics to Flink data warehouse.
 */
public class WarehouseOriginalMetricMessage extends WarehouseMessage {

	private String metricName;
	private double metricValue;

	public WarehouseOriginalMetricMessage() {}

	public WarehouseOriginalMetricMessage(String metricName, double metricValue) {
		this.metricName = metricName;
		this.metricValue = metricValue;
	}

	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	public Double getMetricValue() {
		return metricValue;
	}

	public void setMetricValue(double metricValue) {
		this.metricValue = metricValue;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WarehouseOriginalMetricMessage that = (WarehouseOriginalMetricMessage) o;
		return Objects.equals(metricName, that.metricName) &&
				Objects.equals(metricValue, that.metricValue);
	}

	@Override
	public int hashCode() {
		return Objects.hash(metricName, metricValue);
	}

	@Override
	public String toString() {
		return "OriginalMetric{" +
				"metricName='" + metricName + '\'' +
				", metricValue='" + metricValue + '\'' +
				'}';
	}
}
