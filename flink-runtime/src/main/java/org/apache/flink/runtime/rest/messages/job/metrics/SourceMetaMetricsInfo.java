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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 *  Metadata for source.
 */
public class SourceMetaMetricsInfo {

	private static final String FIELD_NAME_CLUSTER = "cluster";

	private static final String FIELD_NAME_GROUP = "group";

	private static final String FIELD_NAME_TOPICS = "topics";

	@JsonProperty(FIELD_NAME_CLUSTER)
	private final String cluster;

	@JsonProperty(FIELD_NAME_GROUP)
	private final String group;

	@JsonProperty(FIELD_NAME_TOPICS)
	private final List<String> topics;

	@JsonCreator
	public SourceMetaMetricsInfo(
		@JsonProperty(FIELD_NAME_CLUSTER) String cluster,
		@JsonProperty(FIELD_NAME_GROUP) String group,
		@JsonProperty(FIELD_NAME_TOPICS) List<String> topics) {
		this.cluster = cluster;
		this.group = group;
		this.topics = topics;
	}

	public String getCluster() {
		return cluster;
	}

	public String getGroup() {
		return group;
	}

	public List<String> getTopics() {
		return topics;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SourceMetaMetricsInfo that = (SourceMetaMetricsInfo) o;
		return cluster.equals(that.cluster) &&
			group.equals(that.group) &&
			topics.equals(that.topics);
	}

	@Override
	public int hashCode() {
		return Objects.hash(cluster, group, topics);
	}
}
