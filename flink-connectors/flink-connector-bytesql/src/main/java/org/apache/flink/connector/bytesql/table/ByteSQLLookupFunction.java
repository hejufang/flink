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

package org.apache.flink.connector.bytesql.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

/**
 * A sync lookup function for reading from ByteSQL.
 */
public class ByteSQLLookupFunction extends TableFunction<RowData>  {
	private static final long serialVersionUID = 1L;
	private final ByteSQLLookupExecutor lookupExecutor;

	public ByteSQLLookupFunction(ByteSQLLookupExecutor lookupExecutor) {
		this.lookupExecutor = lookupExecutor;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		lookupExecutor.init(context);
	}

	public void eval(Object... keys) {
		for (RowData row: lookupExecutor.doLookup(keys)) {
			collect(row);
		}
	}

	@Override
	public void close() throws Exception {
		lookupExecutor.close();
	}
}
