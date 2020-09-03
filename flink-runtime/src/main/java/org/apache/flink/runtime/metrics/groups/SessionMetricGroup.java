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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Special abstract {@link org.apache.flink.metrics.MetricGroup} representing everything belonging to
 * a specific sql gateway session.
 *
 * @param <C> The type of the parent ComponentMetricGroup.
 */
@Internal
public abstract class SessionMetricGroup<C extends ComponentMetricGroup<C>> extends ComponentMetricGroup<C> {

	/** The ID of the session represented by this metrics group. */
	protected final String sessionId;

	/** The name of the session represented by this metrics group. */
	@Nullable
	protected final String sessionName;

	// ------------------------------------------------------------------------

	protected SessionMetricGroup(
		MetricRegistry registry,
		C parent,
		String sessionId,
		@Nullable String sessionName,
		String[] scope) {
		super(registry, scope, parent);

		this.sessionId = sessionId;
		this.sessionName = sessionName;
	}

	public String sessionId() {
		return sessionId;
	}

	@Nullable
	public String sessionName() {
		return sessionName;
	}

	@Override
	protected QueryScopeInfo.SessionQueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
		return new QueryScopeInfo.SessionQueryScopeInfo(this.sessionId);
	}

	// ------------------------------------------------------------------------
	//  Component Metric Group Specifics
	// ------------------------------------------------------------------------

	@Override
	protected void putVariables(Map<String, String> variables) {
		variables.put(ScopeFormat.SCOPE_SESSION_ID, sessionId);
		variables.put(ScopeFormat.SCOPE_SESSION_NAME, sessionName);
	}

	@Override
	protected String getGroupName(CharacterFilter filter) {
		return "session";
	}

}
