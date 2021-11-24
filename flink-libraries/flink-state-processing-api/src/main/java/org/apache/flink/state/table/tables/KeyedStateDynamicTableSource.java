/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.state.table.tables;

import org.apache.flink.state.table.connector.KeyedStateInputFormatV2;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.MULTI_STATE_NAME_SEPARATOR;

/**
 * StateMetaDynamicTableSource.
 */
public class KeyedStateDynamicTableSource implements ScanTableSource {

	private String savepointPath;
	private DataType dataType;
	private String operatorID;
	private String stateNames;

	public KeyedStateDynamicTableSource(String savepointPath, String operatorID, String stateNames, DataType dataType) {
		this.savepointPath = savepointPath;
		this.dataType = dataType;
		this.operatorID = operatorID;
		this.stateNames = stateNames;
	}

	@Override
	public DynamicTableSource copy() {
		return new KeyedStateDynamicTableSource(savepointPath, operatorID, stateNames, dataType);
	}

	@Override
	public String asSummaryString() {
		return "keyedState";
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		List<String> stateNameList = Arrays.stream(stateNames.split(MULTI_STATE_NAME_SEPARATOR)).collect(Collectors.toList());
		KeyedStateInputFormatV2.Builder builder = new KeyedStateInputFormatV2.Builder(savepointPath, operatorID, stateNameList, dataType);
		return InputFormatProvider.of(builder.build());
	}
}


