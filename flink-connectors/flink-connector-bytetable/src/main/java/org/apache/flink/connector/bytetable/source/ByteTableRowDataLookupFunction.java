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

package org.apache.flink.connector.bytetable.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.bytetable.util.ByteTableConfigurationUtil;
import org.apache.flink.connector.bytetable.util.ByteTableSchema;
import org.apache.flink.connector.bytetable.util.ByteTableSerde;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.util.StringUtils;

import com.bytedance.bytetable.hbase.BytetableTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The ByteTableRowDataLookupFunction is a standard user-defined table function, it can be used in tableAPI
 * and also useful for temporal table join plan in SQL. It looks up the result as {@link RowData}.
 */
@Internal
public class ByteTableRowDataLookupFunction extends TableFunction<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(ByteTableRowDataLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private final String byteTableName;
	private final byte[] serializedConfig;
	private final ByteTableSchema byteTableSchema;
	private final String nullStringLiteral;
	private final FlinkConnectorRateLimiter rateLimiter;

	private transient Connection bConnection;
	private transient BytetableTable table;
	private transient ByteTableSerde serde;
	private transient Meter lookupRequestPerSecond;
	private transient Histogram requestDelayMs;

	public ByteTableRowDataLookupFunction(
			Configuration configuration,
			String byteTableName,
			ByteTableSchema byteTableSchema,
			String nullStringLiteral,
			FlinkConnectorRateLimiter rateLimiter) {
		this.serializedConfig = ByteTableConfigurationUtil.serializeConfiguration(configuration);
		this.byteTableName = byteTableName;
		this.byteTableSchema = byteTableSchema;
		this.nullStringLiteral = nullStringLiteral;
		this.rateLimiter = rateLimiter;
	}

	/**
	 * The invoke entry point of lookup function.
	 * @param rowKey the lookup key. Currently only support single rowkey.
	 */
	public void eval(Object rowKey) throws IOException {
		// fetch result
		lookupRequestPerSecond.markEvent();
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		Get get = serde.createGet(rowKey);
		if (get != null) {
			long startRequest = System.currentTimeMillis();
			Result result = table.get(get);
			long requestDelay = System.currentTimeMillis() - startRequest;
			requestDelayMs.update(requestDelay);
			if (!result.isEmpty()) {
				// parse and collect
				collect(serde.convertToRow(result));
			}
		}
	}

	private Configuration prepareRuntimeConfiguration() {
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		// and overwrite configuration using serialized configuration from client-side env (`hbase-site.xml` in classpath).
		// user params from client-side have the highest priority
		Configuration runtimeConfig = ByteTableConfigurationUtil.deserializeConfiguration(
			serializedConfig,
			HBaseConfiguration.create());

		// do validation: check key option(s) in final runtime configuration
		if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
			LOG.error("can not connect to ByteTable without {} configuration", HConstants.ZOOKEEPER_QUORUM);
			throw new IllegalArgumentException("check ByteTable configuration failed, lost: '" + HConstants.ZOOKEEPER_QUORUM + "'!");
		}

		return runtimeConfig;
	}

	@Override
	public void open(FunctionContext context) {
		LOG.info("start open ...");
		Configuration config = prepareRuntimeConfiguration();
		try {
			bConnection = ConnectionFactory.createConnection(config);
			table = (BytetableTable) bConnection.getTable(TableName.valueOf(byteTableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("Table '{}' not found ", byteTableName, tnfe);
			throw new RuntimeException("ByteTable table '" + byteTableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to ByteTable.", ioe);
			throw new RuntimeException("Cannot create connection to ByteTable.", ioe);
		}
		this.serde = new ByteTableSerde(byteTableSchema, nullStringLiteral);
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
		if (rateLimiter != null) {
			rateLimiter.open(context.getRuntimeContext());
		}
		LOG.info("end open.");
	}

	@Override
	public void close() {
		LOG.info("start close ...");
		if (null != table) {
			try {
				table.close();
				table = null;
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close table", e);
			}
		}
		if (null != bConnection) {
			try {
				bConnection.close();
				bConnection = null;
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close connection", e);
			}
		}
		LOG.info("end close.");
	}

	@VisibleForTesting
	public String getByteTableName() {
		return byteTableName;
	}
}
