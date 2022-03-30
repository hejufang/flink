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

import java.util.Objects;

/**
 * Metas for {@link Message}.
 */
public class MessageMeta {

	private long timestamp;

	private String jobName;

	private MessageType messageType;

	private String region;

	private String cluster;

	private String queue;

	private String metricName;

	private String user;

	private String host;

	private String applicationId;

	private String tmId;

	private String commitId;

	private String commitDate;

	private String version;

	private String resourceType;

	private String flinkClusterId;

	public MessageMeta() {}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public MessageType getMessageType() {
		return messageType;
	}

	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getJobName() {
		return jobName;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}

	public String getTmId() {
		return tmId;
	}

	public void setTmId(String tmId) {
		this.tmId = tmId;
	}

	public String getCommitId() {
		return commitId;
	}

	public void setCommitId(String commitId) {
		this.commitId = commitId;
	}

	public String getCommitDate() {
		return commitDate;
	}

	public void setCommitDate(String commitDate) {
		this.commitDate = commitDate;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getResourceType() {
		return this.resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public String getFlinkClusterId() {
		return this.flinkClusterId;
	}

	public void setFlinkClusterId(String flinkClusterId) {
		this.flinkClusterId = flinkClusterId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MessageMeta that = (MessageMeta) o;
		return timestamp == that.timestamp &&
			Objects.equals(jobName, that.jobName) &&
			messageType == that.messageType &&
			Objects.equals(region, that.region) &&
			Objects.equals(cluster, that.cluster) &&
			Objects.equals(queue, that.queue) &&
			Objects.equals(metricName, that.metricName) &&
			Objects.equals(user, that.user) &&
			Objects.equals(host, that.host) &&
			Objects.equals(applicationId, that.applicationId) &&
			Objects.equals(tmId, that.tmId) &&
			Objects.equals(commitId, that.commitId) &&
			Objects.equals(commitDate, that.commitDate) &&
			Objects.equals(version, that.version) &&
			Objects.equals(resourceType, that.resourceType) &&
			Objects.equals(flinkClusterId, that.flinkClusterId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(timestamp, jobName, messageType, region, cluster, queue, metricName, user, host, applicationId, tmId, commitId, commitDate, version, resourceType, flinkClusterId);
	}

	@Override
	public String toString() {
		return "MessageMeta{" +
			"timestamp=" + timestamp +
			", jobName='" + jobName + '\'' +
			", messageType=" + messageType +
			", region='" + region + '\'' +
			", cluster='" + cluster + '\'' +
			", queue='" + queue + '\'' +
			", metricName='" + metricName + '\'' +
			", user='" + user + '\'' +
			", host='" + host + '\'' +
			", applicationId='" + applicationId + '\'' +
			", tmId='" + tmId + '\'' +
			", commitId='" + commitId + '\'' +
			", commitDate='" + commitDate + '\'' +
			", version='" + version + '\'' +
			", resourceType='" + resourceType + '\'' +
			", flinkClusterId='" + flinkClusterId + '\'' +
			'}';
	}
}
