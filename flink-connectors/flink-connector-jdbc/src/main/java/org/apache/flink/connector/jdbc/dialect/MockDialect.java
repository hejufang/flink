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

package org.apache.flink.connector.jdbc.dialect;

import java.util.Optional;

/**
 * JDBC dialect for mocking. This extends {@link MySQLDialect} because
 * we want to test for Mysql for most of the cases.
 */
public class MockDialect extends MySQLDialect {

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:mock:");
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.empty();
	}

	@Override
	public String dialectName() {
		return "Mock";
	}
}
