/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.table.connector.converter;

import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredKeyedStateMeta;
import org.apache.flink.runtime.checkpoint.StateMetaData;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

/**
 *  A class used to convert the keyed State to the {@link RowData}
 *  with schema  {@link org.apache.flink.state.table.catalog.SavepointCatalogUtils#KEYED_STATE_TABLE_SCHEMA}.
 */
public class StateMetaRowDataConverter implements RowDataConverter<StateMetaData> {

	private final DynamicTableSource.DataStructureConverter converter;

	public StateMetaRowDataConverter(DynamicTableSource.DataStructureConverter converter) {
		this.converter = converter;
	}

	@Override
	public RowData converterToRowData(StateMetaData stateMetaData, Context context) {

		OperatorStateMeta operatorStateMeta = ((StateMetaConverterContext) context).getOperatorStateMeta();

		Row row = new Row(9);
		row.setField(0, operatorStateMeta.getOperatorID().toString());
		row.setField(1, operatorStateMeta.getOperatorName());
		row.setField(2, operatorStateMeta.getUid());

		boolean isKeyedState;
		String keyType = null;
		BackendType backendType;

		if (stateMetaData instanceof RegisteredKeyedStateMeta.KeyedStateMetaData) {
			isKeyedState = true;

			RegisteredKeyedStateMeta curKeyedStateMeta = operatorStateMeta.getKeyedStateMeta();
			backendType = curKeyedStateMeta.getBackendType();
			keyType = FormatterFactory.TypeFormatter.INSTANCE.format(curKeyedStateMeta.getKeySerializer());
		} else {
			isKeyedState = false;
			backendType = operatorStateMeta.getOperatorStateMeta().getBackendType();
		}

		row.setField(3, isKeyedState);
		row.setField(4, keyType);
		row.setField(5, stateMetaData.getName());
		row.setField(6, stateMetaData.getType().toString());
		row.setField(7, backendType.getBackendType());
		row.setField(8, FormatterFactory.TypeFormatter.INSTANCE.format(stateMetaData.getStateDescriptor().getSerializer()));

		return (RowData) converter.toInternal(row);
	}

	/**
	 * StateMetaConverterContext.
	 */
	public static class StateMetaConverterContext implements Context {

		private OperatorStateMeta operatorStateMeta;

		public OperatorStateMeta getOperatorStateMeta() {
			return operatorStateMeta;
		}

		public void setOperatorStateMeta(OperatorStateMeta operatorStateMeta) {
			this.operatorStateMeta = operatorStateMeta;
		}
	}

}

