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

package org.apache.flink.connector.bytesql.client;

import com.bytedance.infra.bytesql4j.ByteSQLDB;
import com.bytedance.infra.bytesql4j.ByteSQLOption;
import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;

/**
 * Wrapper of a real {@link ByteSQLDB}.
 */
public class ByteSQLDBWrapper implements ByteSQLDBBase {
	private final ByteSQLDB byteSQLDB;

	private ByteSQLDBWrapper(ByteSQLDB byteSQLDB) {
		this.byteSQLDB = byteSQLDB;
	}

	public static ByteSQLDBWrapper getInstance(ByteSQLOption byteSQLOption){
		return new ByteSQLDBWrapper(ByteSQLDB.newInstance(byteSQLOption));
	}

	@Override
	public ByteSQLTransaction beginTransaction() throws ByteSQLException {
		return byteSQLDB.beginTransaction();
	}

	@Override
	public void close() {
		byteSQLDB.close();
	}
}
