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

/**
 * StateMetaDynamicTableSource.
 */
public class KeyedStateDynamicTableSource implements ScanTableSource{

	private String savepointPath;
	private DataType producedDataType;
	private String operatorID;
	private String stateName;

	public KeyedStateDynamicTableSource(String savepointPath, String operatorID, String stateName, DataType producedDataType) {
		this.savepointPath = savepointPath;
		this.producedDataType = producedDataType;
		this.operatorID = operatorID;
		this.stateName = stateName;
	}

	@Override
	public DynamicTableSource copy() {
		return new KeyedStateDynamicTableSource(savepointPath, operatorID, stateName, producedDataType);
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
			DataStructureConverter converter = runtimeProviderContext.createDataStructureConverter(producedDataType);

			KeyedStateInputFormatV2.Builder builder = new KeyedStateInputFormatV2.Builder(savepointPath, operatorID, stateName, converter);
			return InputFormatProvider.of(builder.build());

	}
}


