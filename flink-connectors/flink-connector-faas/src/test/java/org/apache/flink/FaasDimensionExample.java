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

package org.apache.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSinkBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * fass dimension table example.
 */
public class FaasDimensionExample {

	public void testFaasLookupTableSource() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

		DataStream<Row> ds = getRowDataStream(env);
		Table in = tEnv.fromDataStream(ds, "name,description");
		tEnv.registerTable("source", in);

		tEnv.sqlUpdate("" +
			"create table faas_dim(" +
			"	name VARCHAR,\n" +
			"	age INT,\n" +
			"	sex VARCHAR,\n" +
			"	nationality VARCHAR\n" +
			") with (\n" +
			"	'connector.type' = 'faas', \n" +
			"	'connector.url' = 'https://i1pl46vv.fn.bytedance.net', \n" +
			"	'connector.lookup.cache.max-rows' = '100', \n" +
			"	'connector.lookup.cache.ttl' = '10000', \n" +
			"	'connector.lookup.max-retries' = '3' \n" +
			")");

		final CollectionTableSink tableSink = (CollectionTableSink) new CollectionTableSink().configure(
			new String[]{"name", "description", "age", "sex", "nationality"},
			new TypeInformation[]{
				Types.STRING(),
				Types.STRING(),
				Types.INT(),
				Types.STRING(),
				Types.STRING()});
		tEnv.registerTableSink("sink", tableSink);

		tEnv.sqlUpdate("insert into sink" +
			" select T.name, T.description, D.age, D.sex, D.nationality " +
			" from (select source.name, source.description, PROCTIME() as proc from source) T " +
			" left join faas_dim FOR SYSTEM_TIME AS OF T.proc AS D" +
			" on T.name=D.name");

		tEnv.execute("faas test");
	}

	static class CollectionTableSink extends TableSinkBase implements AppendStreamTableSink {
		public ArrayList<String> list = new ArrayList();

		@Override
		public void emitDataStream(DataStream dataStream) {
			dataStream.print();
			dataStream.writeUsingOutputFormat(new LocalCollectionOutputFormat(list)).setParallelism(1);
		}

		@Override
		protected TableSinkBase copy() {
			return new CollectionTableSink();
		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream dataStream) {
			dataStream.print();
			return dataStream.writeUsingOutputFormat(new LocalCollectionOutputFormat(list));
		}

		@Override
		public TypeInformation getOutputType() {
			return new RowTypeInfo(
				Types.STRING(),
				Types.STRING(),
				Types.INT(),
				Types.STRING(),
				Types.STRING());
		}
	}

	public static DataStream<Row> getRowDataStream(StreamExecutionEnvironment env) {
		List<Row> testData = new ArrayList<>();
		for (int i = 0; i < 2000; i++) {
			testData.add(Row.of("Tom", "police"));
		}
		return env.fromCollection(testData);
	}

	public static void main(String[] args) throws Exception {
		FaasDimensionExample faasDimensionExample = new FaasDimensionExample();
		faasDimensionExample.testFaasLookupTableSource();
	}
}
