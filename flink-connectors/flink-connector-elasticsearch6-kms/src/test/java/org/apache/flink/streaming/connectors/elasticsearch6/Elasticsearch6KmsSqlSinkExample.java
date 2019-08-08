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

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * es6 sql example.
 */
public class Elasticsearch6KmsSqlSinkExample {
	public static void main(String[] args) throws Exception {
		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Order> orderA = env.fromCollection(Arrays.asList(
			new Order(1L, "beer", 3),
			new Order(1L, "diaper", 4),
			new Order(3L, "rubber", 2)));

		DataStream<Order> orderB = env.fromCollection(Arrays.asList(
			new Order(2L, "pen", 3),
			new Order(2L, "rubber", 3),
			new Order(4L, "beer", 1)));

		// convert DataStream to Table
		Table tableA = tEnv.fromDataStream(orderA, "userid, product, amount");
		// register DataStream as Table
		tEnv.registerDataStream("OrderB", orderB, "userid, product, amount");

		// union the two tables
		Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
			"SELECT * FROM OrderB WHERE amount < 2");

		tEnv.registerTable("result", result);

		//use your own consul and config
		String sinkDDL = "create table final_order(\n" +
			"    userid bigint, \n" +
			"    product varchar, \n" +
			"    amount int\n" +
			") with (\n" +
			"connector.type = 'elasticsearch',\n" +
			"connector.version = '6-kms',\n" +
			"`update-mode` = 'upsert',\n" +
			"connector.index = 'test',\n" +
			"`connector.document-type` = 'test',\n" +
			"`connector.connection-consul` = 'test',\n" +
			"`connector.key-field-indices` = '0',\n" +
			"`connector.kms-psm` = 'true',\n" +
			"`connector.kms-approle` = 'test',\n" +
			"`connector.kms-secret` = 'test',\n" +
			"`connector.kms-filename` = 'test'\n" +
			")";

		tEnv.sqlUpdate(sinkDDL);
		String sinkDML = "insert into final_order\n" +
			"select userid, product, amount\n" +
			"from `result`";
		tEnv.sqlUpdate(sinkDML);
		tEnv.execute("sql example");
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO.
	 */
	public static class Order {
		public Long userid;
		public String product;
		public int amount;

		public Order() {
		}

		public Order(Long userid, String product, int amount) {
			this.userid = userid;
			this.product = product;
			this.amount = amount;
		}

		@Override
		public String toString() {
			return "Order{" +
				"userid=" + userid +
				", product='" + product + '\'' +
				", amount=" + amount +
				'}';
		}
	}
}
