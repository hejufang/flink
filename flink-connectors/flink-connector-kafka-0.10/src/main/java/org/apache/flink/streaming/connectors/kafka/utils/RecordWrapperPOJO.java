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

package org.apache.flink.streaming.connectors.kafka.utils;

import java.io.Serializable;

/**
 * RecordWrapperPOJO contains cluster, topic and record.
 */
public class RecordWrapperPOJO<T> implements Serializable {
	String cluster;
	String topic;
	T record;

	public RecordWrapperPOJO(String cluster, String topic, T record) {
		this.cluster = cluster;
		this.topic = topic;
		this.record = record;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public T getRecord() {
		return record;
	}

	public void setRecord(T record) {
		this.record = record;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}

		if (other == null || getClass() != other.getClass()) {
			return false;
		}

		RecordWrapperPOJO<?> that = (RecordWrapperPOJO<?>) other;

		if (cluster != null ? !cluster.equals(that.cluster) : that.cluster != null) {
			return false;
		}

		if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
			return false;
		}

		return record != null ? record.equals(that.record) : that.record == null;
	}

	@Override
	public int hashCode() {
		int result = cluster != null ? cluster.hashCode() : 0;
		result = 31 * result + (topic != null ? topic.hashCode() : 0);
		result = 31 * result + (record != null ? record.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "RecordWrapperPOJO{" +
			"cluster='" + cluster + '\'' +
			", topic='" + topic + '\'' +
			", record=" + record +
			'}';
	}
}
