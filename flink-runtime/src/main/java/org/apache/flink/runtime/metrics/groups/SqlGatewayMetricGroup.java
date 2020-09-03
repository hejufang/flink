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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a SqlGateway.
 *
 * <p>Contains extra logic for adding sessions with sql jobs, and removing sessions when they do
 * not contain sql jobs any more.
 */
public class SqlGatewayMetricGroup extends ComponentMetricGroup<SqlGatewayMetricGroup> {

	private final Map<String, SqlGatewaySessionMetricGroup> sessions = new HashMap<>();

	private final String hostname;

	public SqlGatewayMetricGroup(MetricRegistry registry, String hostname) {
		super(registry, registry.getScopeFormats().getSqlGatewayFormat().formatScope(hostname), null);
		this.hostname = hostname;
	}

	public String hostname() {
		return hostname;
	}

	@Override
	protected QueryScopeInfo.SqlGatewayQueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
		return new QueryScopeInfo.SqlGatewayQueryScopeInfo();
	}

	// ------------------------------------------------------------------------
	//  session groups
	// ------------------------------------------------------------------------
	public SqlGatewaySessionMetricGroup addSession(String sessionID, String sessionName) {
		// get or create a session metric group
		SqlGatewaySessionMetricGroup currentSessionGroup;
		synchronized (this) {
			if (!isClosed()) {
				currentSessionGroup = sessions.get(sessionID);

				if (currentSessionGroup == null || currentSessionGroup.isClosed()) {
					currentSessionGroup = new SqlGatewaySessionMetricGroup(
						registry,
						this,
						sessionID,
						sessionName);
					sessions.put(sessionID, currentSessionGroup);
				}
				return currentSessionGroup;
			} else {
				return null;
			}
		}
	}

	public void removeSession(String sessionID) {
		if (sessionID == null || sessionID.isEmpty()) {
			return;
		}

		synchronized (this) {
			SqlGatewaySessionMetricGroup containedGroup = sessions.remove(sessionID);
			if (containedGroup != null) {
				containedGroup.close();
			}
		}
	}

	public int numRegisteredSessionMetricGroups() {
		return sessions.size();
	}

	// ------------------------------------------------------------------------
	//  Component Metric Group Specifics
	// ------------------------------------------------------------------------

	@Override
	protected void putVariables(Map<String, String> variables) {
		variables.put(ScopeFormat.SCOPE_HOST, hostname);
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return sessions.values();
	}

	@Override
	protected String getGroupName(CharacterFilter filter) {
		return "sqlgateway";
	}
}
