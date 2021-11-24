/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.table.connector.converter;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * A class used to convert the value to the RowData of the table in QueryableState.
 */
public abstract class AbstractStateConverter<V> implements RowDataConverter<V>{

	public static final String OPERATOR_ID_FIELD_NAME = "operator_id";
	public static final String STATE_NAME_FIELD_NAME = "state_name";

	protected final DataStructureConverter converter;
	protected final FormatterFactory.Formatter valueFormatter;
	protected final List<String> fieldNames;

	AbstractStateConverter(TypeSerializer valueSerializer, DataType dataType){
		this.converter = DataStructureConverters.getConverter(dataType);
		this.valueFormatter = FormatterFactory.getFormatter(valueSerializer);
		this.fieldNames = ((RowType) dataType.getLogicalType()).getFieldNames();
	}

	public String getStateName(Context context){
		return ((StateContext) context).getStateName();
	}

	public String getOperatorId(Context context){
		return ((StateContext) context).getOperatorID();
	}

	/**
	 * StateContext.
	 */
	public static class StateContext implements Context{

		private String operatorID;
		private String stateName;

		public String getOperatorID() {
			return operatorID;
		}

		public void setOperatorID(String operatorID) {
			this.operatorID = operatorID;
		}

		public String getStateName() {
			return stateName;
		}

		public void setStateName(String stateName) {
			this.stateName = stateName;
		}
	}

}
