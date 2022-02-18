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

package org.apache.flink.connector.jdbc.internal.connection;

import com.bytedance.commons.consul.ServiceNode;
import com.bytedance.mysql.Consul;
import com.bytedance.mysql.DBInfo;
import com.bytedance.mysql.DbAuth;
import com.bytedance.mysql.DbInfoReq;
import com.bytedance.mysql.MysqlDriverManager;
import com.bytedance.mysql.Utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Dedicated connection provider for ByteDance RDS.
 * Most of codes are copied from {@link com.bytedance.mysql.MysqlDriverManager}, changes:
 * 1. Throw exception instead of logging when auth failed.
 * 2. Use `new com.mysql.jdbc.Driver().connect` instead of `DriverManager.getConnection`,
 *    because `DriverManager.getConnection` will only throw last driver's exception message.
 */
public class ByteDanceMysqlConnectionProvider {
	private static MysqlDriverManager.DSNResolved parseDsn(String url) throws SQLException {
		HashMap<String, String> props = MysqlDriverManager.parseUrl(url);
		if (!props.containsKey("db_consul")) {
			throw new SQLException("db_consul and psm must be set in params");
		} else {
			String dbConsul = props.remove("db_consul");
			MysqlDriverManager.DSNResolved resolved = new MysqlDriverManager.DSNResolved();
			String psm = props.remove("psm");
			String token = props.remove("token");
			String authEnable = props.remove("auth_enable");

			List<ServiceNode> dbNodes = Consul.translateOne(dbConsul);
			resolved.setEndpoints(dbNodes);
			if (authEnable != null && authEnable.equals("true")) {
				DbInfoReq dbInfoReq = new DbInfoReq(dbConsul, psm, token);
				DBInfo dbInfo = DbAuth.getDbInfoFromDbAuthModule(dbInfoReq);
				if (dbInfo == null) {
					throw new SQLException("query DB Auth service error");
				} else if ("".equals(dbInfo.getUser())) {
					throw new SQLException("auth failed, please check whether the user has permission to " + dbConsul);
				} else {
					resolved.setDatabase(dbInfo.getName());
					resolved.setPassword(dbInfo.getPwd());
					resolved.setUser(dbInfo.getUser());
				}
			}

			resolved.setDatabase(props.remove("dbName"));
			resolved.setParams(props);
			return resolved;
		}
	}

	public static Connection getConnection(String url) throws SQLException {
		String dsn = url + "&useSSL=false";
		MysqlDriverManager.DSNResolved resolved = parseDsn(dsn);

		List<ServiceNode> nodes = resolved.getEndpoints();
		if (nodes != null && !nodes.isEmpty()) {
			SQLException throwable = null;
			for (int i = 0; i < 3; ++i) {
				int index = Utils.randInt(nodes.size());
				try {
					Properties properties = new Properties();
					if (resolved.getUser() != null) {
						properties.put("user", resolved.getUser());
					}
					if (resolved.getPassword() != null) {
						properties.put("password", resolved.getPassword());
					}
					return new com.mysql.jdbc.Driver().connect(resolved.formatURL(index), properties);
				} catch (SQLException e) {
					throwable = e;
				}
			}
			throw throwable;
		} else {
			throw new SQLException("cannot get address for mysql for dsn: " + dsn);
		}
	}
}
