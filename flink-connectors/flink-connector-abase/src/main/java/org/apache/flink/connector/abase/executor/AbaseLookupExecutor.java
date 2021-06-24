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

package org.apache.flink.connector.abase.executor;

import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.utils.AbaseClientTableUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.bytedance.abase.AbaseClient;
import com.bytedance.abase.AbaseTable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Abstract AbaseLookupExecutor.
 */
public abstract class AbaseLookupExecutor implements Serializable {

	protected transient AbaseClient abaseClient;
	protected transient AbaseTable abaseTable;
	protected final AbaseNormalOptions normalOptions;

	public AbaseLookupExecutor(AbaseNormalOptions normalOptions) {
		this.normalOptions = normalOptions;
	}

	public abstract RowData doLookup(Object key) throws IOException;

	public void open(FunctionContext context) throws Exception{
		this.abaseClient = AbaseClientTableUtils.getAbaseClient(normalOptions);
		this.abaseTable = abaseClient.getTable(normalOptions.getTable());
	}

	public void close() throws Exception {
		if (abaseClient != null) {
			abaseClient.close();
		}
	}
}
