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

package org.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * ClickHouseAppendTableSink Builder.
 */
public class ClickHouseAppendTableSinkBuilder {
	private String username;
	private String password;
	private String drivername = "ru.yandex.clickhouse.ClickHouseDriver";
	private String dbURL;
	private String dbName;
	private String tableName;
	private String[] primaryKey;
	private String[] columnNames;
	private int[] parameterTypes;
	private TableSchema tableScehma;

	public ClickHouseAppendTableSinkBuilder setDrivername(String drivername) {
		this.drivername = drivername;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setDbUrl(String dbURL) {
		this.dbURL = dbURL;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setUsername(String username) {
		this.username = username;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setPassword(String password) {
		this.password = password;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setDbName(String dbName) {
		this.dbName = dbName;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setTableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setPrimaryKey(String[] primaryKey) {
		this.primaryKey = primaryKey;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setTableScehma(TableSchema tableScehma) {
		this.tableScehma = tableScehma;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setColumnNames(String[] columnNames) {
		// 传入Row的全部列名列表，舍弃最后一列事件类型
		this.columnNames = Arrays.copyOf(columnNames, columnNames.length - 1);
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setParameterTypes(TypeInformation<?>... types) {
		// 传入Row的全部类型列表，舍弃最后一列事件类型
		int[] ty = new int[types.length - 1];
		for (int i = 0; i < types.length - 1; ++i) {
			ty[i] = ClickHouseTypeUtil.typeInformationToSqlType(types[i]);
		}
		this.parameterTypes = ty;
		return this;
	}

	public ClickHouseAppendTableSink build() {
		Preconditions.checkNotNull(drivername,
			"drivername are not specified." +
				" Please specify types using the setDrivername() method.");

		Preconditions.checkNotNull(dbURL,
			"ClickHouse dbURL name are not specified." +
				" Please specify types using the setDbURL() method.");

		Preconditions.checkNotNull(dbName,
			"ClickHouse dbName name are not specified." +
				" Please specify types using the setDbName() method.");

		Preconditions.checkNotNull(tableName,
			"ClickHouse tableName name are not specified." +
				" Please specify types using the setTableName() method.");

		Preconditions.checkNotNull(primaryKey,
			"ClickHouse primaryKey name are not specified." +
				" Please specify types using the setPrimaryKey() method.");

		Preconditions.checkNotNull(columnNames,
			"ClickHouse columnNames name are not specified." +
				" Please specify types using the setColumnNames() method.");

		Preconditions.checkNotNull(parameterTypes,
			"ClickHouse parameterTypes name are not specified." +
				" Please specify types using the setParameterTypes() method.");

		ClickHouseOutputFormat format = ClickHouseOutputFormat.buildClickHouseOutputFormat()
			.setDrivername(drivername)
			.setDbURL(dbURL)
			.setUserName(username)
			.setPassword(password)
			.setDbName(dbName)
			.setTableName(tableName)
			.setPrimaryKey(primaryKey)
			.setColumnNames(columnNames)
			.setSqlTypes(parameterTypes)
			.setTableScehma(tableScehma)
			.build();

		return new ClickHouseAppendTableSink(format);

	}
}
