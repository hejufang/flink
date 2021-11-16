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

package org.apache.flink.connector.bytesql.table.client;

import org.apache.flink.connector.bytesql.client.ByteSQLDBBase;

import com.bytedance.infra.bytesql4j.ByteSQLOption;
import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Mock of a ByteSQLDB.
 */
public class ByteSQLDBMock implements ByteSQLDBBase {
	private final ByteSQLTransaction byteSQLTransaction;
	private static ByteSQLDBMock instance;

	private ByteSQLDBMock() {
		this.byteSQLTransaction = mock(ByteSQLTransaction.class);
	}

	public static synchronized ByteSQLDBMock getInstance(ByteSQLOption byteSQLOption) {
		if (instance == null) {
			instance = new ByteSQLDBMock();
		}
		return instance;
	}

	private static synchronized void refresh() {
		instance = new ByteSQLDBMock();
	}

	@Override
	public ByteSQLTransaction beginTransaction() throws ByteSQLException {
		return byteSQLTransaction;
	}

	@Override
	public void close() {
		//do nothing.
	}

	public List<String> getExecutedSQLs(int mockTimes) throws ByteSQLException {
		refresh();
		ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
		verify(byteSQLTransaction, times(mockTimes)).rawQuery(argument.capture());
		return argument.getAllValues();
	}
}
