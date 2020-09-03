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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.runtime.metrics.groups.SqlGatewayMetricGroup;

/**
 * The scope format for the {@link org.apache.flink.runtime.metrics.groups.SqlGatewaySessionMetricGroup}.
 */
public class SqlGatewaySessionScopeFormat extends ScopeFormat {

	public SqlGatewaySessionScopeFormat(String format, SqlGatewayScopeFormat parentFormat) {
		super(format, parentFormat, new String[] {
			SCOPE_HOST,
			SCOPE_SESSION_ID,
			SCOPE_SESSION_NAME
		});
	}

	public String[] formatScope(SqlGatewayMetricGroup parent, String sessionId, String sessionName) {
		final String[] template = copyTemplate();
		final String[] values = {
			parent.hostname(),
			valueOrNull(sessionId),
			valueOrNull(sessionName)
		};
		return bindVariables(template, values);
	}
}
