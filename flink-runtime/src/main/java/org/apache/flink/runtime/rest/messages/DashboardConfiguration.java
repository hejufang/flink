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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.cluster.DashboardConfigHandler;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Objects;

/**
 * Response of the {@link DashboardConfigHandler} containing general configuration
 * values such as the time zone and the refresh interval.
 */
public class DashboardConfiguration implements ResponseBody {

	public static final String FIELD_NAME_REFRESH_INTERVAL = "refresh-interval";
	public static final String FIELD_NAME_TIMEZONE_OFFSET = "timezone-offset";
	public static final String FIELD_NAME_TIMEZONE_NAME = "timezone-name";
	public static final String FIELD_NAME_FLINK_VERSION = "flink-version";
	public static final String FIELD_NAME_FLINK_REVISION = "flink-revision";
	public static final String FIELD_NAME_JM_LOG = "jmLog";
	public static final String FIELD_NAME_JM_WEB_SHELL = "jmWebShell";

	@JsonProperty(FIELD_NAME_REFRESH_INTERVAL)
	private final long refreshInterval;

	@JsonProperty(FIELD_NAME_TIMEZONE_NAME)
	private final String timeZoneName;

	@JsonProperty(FIELD_NAME_TIMEZONE_OFFSET)
	private final int timeZoneOffset;

	@JsonProperty(FIELD_NAME_FLINK_VERSION)
	private final String flinkVersion;

	@JsonProperty(FIELD_NAME_FLINK_REVISION)
	private final String flinkRevision;

	@JsonProperty(FIELD_NAME_JM_LOG)
	private final String jmLog;

	@JsonProperty(FIELD_NAME_JM_WEB_SHELL)
	private final String jmWebShell;

	@JsonCreator
	public DashboardConfiguration(
			@JsonProperty(FIELD_NAME_REFRESH_INTERVAL) long refreshInterval,
			@JsonProperty(FIELD_NAME_TIMEZONE_NAME) String timeZoneName,
			@JsonProperty(FIELD_NAME_TIMEZONE_OFFSET) int timeZoneOffset,
			@JsonProperty(FIELD_NAME_FLINK_VERSION) String flinkVersion,
			@JsonProperty(FIELD_NAME_FLINK_REVISION) String flinkRevision,
			@JsonProperty(FIELD_NAME_JM_LOG) String jmLog,
			@JsonProperty(FIELD_NAME_JM_WEB_SHELL) String jmWebShell) {
		this.refreshInterval = refreshInterval;
		this.timeZoneName = Preconditions.checkNotNull(timeZoneName);
		this.timeZoneOffset = timeZoneOffset;
		this.flinkVersion = Preconditions.checkNotNull(flinkVersion);
		this.flinkRevision = Preconditions.checkNotNull(flinkRevision);
		this.jmLog = (jmLog == null) ? "NoJmLog" : jmLog;
		this.jmWebShell = (jmWebShell == null) ? "NoJmWebShell" : jmWebShell;
	}

	public long getRefreshInterval() {
		return refreshInterval;
	}

	public int getTimeZoneOffset() {
		return timeZoneOffset;
	}

	public String getTimeZoneName() {
		return timeZoneName;
	}

	public String getFlinkVersion() {
		return flinkVersion;
	}

	public String getFlinkRevision() {
		return flinkRevision;
	}

	public String getJmLog() {
		return jmLog;
	}

	public String getJmWebShell() {
		return jmWebShell;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DashboardConfiguration that = (DashboardConfiguration) o;
		return refreshInterval == that.refreshInterval &&
			timeZoneOffset == that.timeZoneOffset &&
			Objects.equals(timeZoneName, that.timeZoneName) &&
			Objects.equals(flinkVersion, that.flinkVersion) &&
			Objects.equals(flinkRevision, that.flinkRevision) &&
			Objects.equals(jmLog, that.jmLog) &&
			Objects.equals(jmWebShell, that.jmWebShell);
	}

	@Override
	public int hashCode() {
		return Objects.hash(refreshInterval, timeZoneName, timeZoneOffset, flinkVersion, flinkRevision,
			jmLog, jmWebShell);
	}

	public static DashboardConfiguration from(long refreshInterval, ZonedDateTime zonedDateTime) {

		final String flinkVersion = EnvironmentInformation.getVersion();

		final EnvironmentInformation.RevisionInformation revision = EnvironmentInformation.getRevisionInformation();
		final String flinkRevision;

		if (revision != null) {
			flinkRevision = revision.commitId + " @ " + revision.commitDate;
		} else {
			flinkRevision = "unknown revision";
		}

		final String jmContainerId = WebMonitorUtils.getJMContainerId();
		final String jmIp = WebMonitorUtils.getIp();
		final String jmLog = WebMonitorUtils.getContainerLog(jmContainerId, jmIp);
		final String jmWebShell = WebMonitorUtils.getContainerWebShell(jmContainerId, jmIp);

		return new DashboardConfiguration(
			refreshInterval,
			zonedDateTime.getZone().getDisplayName(TextStyle.FULL, Locale.getDefault()),
			// convert zone date time into offset in order to not do the day light saving adaptions wrt the offset
			zonedDateTime.toOffsetDateTime().getOffset().getTotalSeconds() * 1000,
			flinkVersion,
			flinkRevision,
			jmLog,
			jmWebShell);
	}
}
