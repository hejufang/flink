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

package org.apache.flink.connectors.htap.connector;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Describes which table should be used in sources.
 *
 * <p>For sources reading from already existing tables, simply use @{@link HtapTableInfo#forTable(String, String)}
 */
@PublicEvolving
public class HtapTableInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String dbName;
	private final String tableName;

	private HtapTableInfo(String dbName, String tableName) {
		this.dbName = Preconditions.checkNotNull(dbName);
		this.tableName = Preconditions.checkNotNull(tableName);
	}

	public static HtapTableInfo forTable(String dbName, String tableName) {
		return new HtapTableInfo(dbName, tableName);
	}

	public String getDbName() {
		return dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getFullName() {
		return dbName + "." + tableName;
	}

	@Override
	public String toString() {
		return "HtapTableInfo{" +
			"dbName='" + dbName + '\'' +
			", tableName='" + tableName + '\'' +
			'}';
	}
}
