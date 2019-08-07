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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operation to describe a CREATE FUNCTION statement.
 */
public class CreateFunctionOperation implements CreateOperation {
	private final String functionName;
	private String className;
	private boolean ignoreIfExists;

	public CreateFunctionOperation(String functionName,
		String className,
		boolean ignoreIfExists) {
		this.functionName = functionName;
		this.className = className;
		this.ignoreIfExists = ignoreIfExists;
	}

	public String getClassName() {
		return className;
	}

	public String getFunctionName() {
		return functionName;
	}

	public boolean isIgnoreIfExists() {
		return ignoreIfExists;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("functionName", getFunctionName());
		params.put("className", getClassName());
		params.put("ignoreIfExists", isIgnoreIfExists());

		return OperationUtils.formatWithChildren(
			"CREATE FUNCTION",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}
}
