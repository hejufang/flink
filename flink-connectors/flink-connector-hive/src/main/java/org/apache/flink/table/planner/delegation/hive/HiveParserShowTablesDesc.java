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

package org.apache.flink.table.planner.delegation.hive;

import java.io.Serializable;

/**
 * Desc for SHOW TABLES/VIEWS operation.
 */
public class HiveParserShowTablesDesc implements Serializable {
	private static final long serialVersionUID = -3381731226279052381L;

	private final String pattern;
	private final String dbName;
	private final boolean expectView;

	public HiveParserShowTablesDesc(String pattern, String dbName, boolean expectView) {
		this.pattern = pattern;
		this.dbName = dbName;
		this.expectView = expectView;
	}

	public String getPattern() {
		return pattern;
	}

	public String getDbName() {
		return dbName;
	}

	public boolean isExpectView() {
		return expectView;
	}
}
