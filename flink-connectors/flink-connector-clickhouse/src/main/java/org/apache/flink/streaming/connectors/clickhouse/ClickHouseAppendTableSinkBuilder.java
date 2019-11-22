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

import com.bytedance.commons.consul.Discovery;
import com.bytedance.commons.consul.ServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * ClickHouseAppendTableSink Builder.
 */
public class ClickHouseAppendTableSinkBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(ClickHouseAppendTableSinkBuilder.class);
	private static final int DEFAULT_FLUSH_MAX_SIZE = 5000;

	private String username;
	private String password;
	private String drivername = "ru.yandex.clickhouse.ClickHouseDriver";
	private String dbURL;
	private String psm;
	private String dbName;
	private String tableName;
	private String signColumnName;
	private String[] columnNames;
	private int[] parameterTypes;
	private TableSchema tableScehma;
	private int parallelism;

	private int flushMaxSize = DEFAULT_FLUSH_MAX_SIZE;

	public ClickHouseAppendTableSinkBuilder setDrivername(String drivername) {
		this.drivername = drivername;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setDbUrl(String dbURL) {
		this.dbURL = dbURL;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setPsm(String psm) {
		this.psm = psm;
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

	public ClickHouseAppendTableSinkBuilder setTableScehma(TableSchema tableScehma) {
		this.tableScehma = tableScehma;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setSignColumn(String signColumnName) {
		this.signColumnName = signColumnName;
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

	public ClickHouseAppendTableSinkBuilder setFlushMaxSize(int flushMaxSize) {
		this.flushMaxSize = flushMaxSize;
		return this;
	}

	public ClickHouseAppendTableSinkBuilder setParallelism(int parallelism) {
		this.parallelism = parallelism;
		return this;
	}

	public ClickHouseAppendTableSink build() {
		Preconditions.checkNotNull(drivername,
			"drivername are not specified." +
				" Please specify types using the setDrivername() method.");

		Preconditions.checkNotNull(dbName,
			"ClickHouse dbName name are not specified." +
				" Please specify types using the setDbName() method.");

		Preconditions.checkNotNull(tableName,
			"ClickHouse tableName name are not specified." +
				" Please specify types using the setTableName() method.");

		Preconditions.checkNotNull(columnNames,
			"ClickHouse columnNames name are not specified." +
				" Please specify types using the setColumnNames() method.");

		Preconditions.checkNotNull(parameterTypes,
			"ClickHouse parameterTypes name are not specified." +
				" Please specify types using the setParameterTypes() method.");

		if (dbURL == null && psm == null) {
			throw new NullPointerException("ClickHouse dbURL or psm must be specified.");
		}

		String dbUrl = dbURL;
		if (psm != null) {
			Discovery discovery = new Discovery();
			List<ServiceNode> serviceNodeList = discovery.translateOne(psm);
			if (serviceNodeList == null || 0 == serviceNodeList.size()) {
				throw new IllegalArgumentException("Invalid clickhouse psm: " + psm);
			}
			Collections.shuffle(serviceNodeList, new Random(System.currentTimeMillis()));
			ServiceNode sn = serviceNodeList.get(0);
			String host = sn.getHost();
			int port = sn.getPort();
			dbUrl = "jdbc:clickhouse://" + host + ":" + port + "/";
			LOG.info("Get dbUrl from clickhouse psm: " + dbUrl);
		}
		LOG.info("dbUrl is : " + dbUrl);

		ClickHouseOutputFormat format = ClickHouseOutputFormat.buildClickHouseOutputFormat()
			.setDrivername(drivername)
			.setDbURL(dbUrl)
			.setUserName(username)
			.setPassword(password)
			.setDbName(dbName)
			.setTableName(tableName)
			.setSignColumnName(signColumnName)
			.setColumnNames(columnNames)
			.setSqlTypes(parameterTypes)
			.setTableScehma(tableScehma)
			.setFlushMaxSize(flushMaxSize)
			.setParallelism(parallelism)
			.build();

		return new ClickHouseAppendTableSink(format);

	}
}
