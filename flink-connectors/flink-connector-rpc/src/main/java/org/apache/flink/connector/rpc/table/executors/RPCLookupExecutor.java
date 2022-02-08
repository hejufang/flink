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

package org.apache.flink.connector.rpc.table.executors;

import org.apache.flink.connector.rpc.table.RPCAsyncLookupFunction;
import org.apache.flink.connector.rpc.table.descriptors.RPCLookupOptions;
import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * An executor for reading data from RPC servers.
 * A dimension RPC table consists of a breakup request and a row type response.
 * Which means if the request class has n fields, the table will have n + 1 fields
 * with the response object as the last field.
 * todo: refactor the {@link RPCAsyncLookupFunction} to reuse this executor too.
 */
public class RPCLookupExecutor extends BaseRPCLookupExecutor<RowData> {
	private static final long serialVersionUID = 1L;

	public RPCLookupExecutor(
			RPCLookupOptions rpcLookupOptions,
			RPCOptions rpcOptions,
			int[] keyIndices,
			DataType dataType,
			String[] fieldNames) {
		super(rpcLookupOptions, rpcOptions, keyIndices, dataType, fieldNames);
	}

	@Override
	protected Object prepareRequest(RowData lookupKeys) {
		return requestConverter.toExternal(lookupKeys);
	}

	@Override
	protected RowData resolveResponse(RowData lookupKeys, Object response) throws Exception {
		RowData responseValue = responseConverter.toInternal(response);
		return assembleRow((GenericRowData) lookupKeys, responseValue);
	}
}
