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
 * Operation to describe a CREATE FUNCTION statement for legacy(1.9) function.
 */
public class CreateLegacyFunctionOperation implements CreateOperation {
	private final String functionName;
	private final String functionClass;

	public CreateLegacyFunctionOperation(
			String functionName,
			String functionClass) {
		this.functionName = functionName;
		this.functionClass = functionClass;
	}

	public String getFunctionName() {
		return this.functionName;
	}

	public String getFunctionClass() {
		return this.functionClass;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("functionName", functionName);
		params.put("functionClass", functionClass);

		return OperationUtils.formatWithChildren(
			"CREATE LEGACY FUNCTION",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}
}
