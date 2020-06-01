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
import org.apache.flink.api.java.DataSet;
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
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSinkBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test bytable dimension.
 */
public class BytableDimensionExample {

	public void testBytableLookupTableSource() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

		DataStream<Row> ds = getRowDataStream(env);
		Table in = tEnv.fromDataStream(ds, "a, b, c");
		tEnv.registerTable("src", in);

		tEnv.sqlUpdate("create table bytable_dim(" +
			"rk INT,\n" +
			"msg ROW<col1 INT>\n" +
			") with (\n" +
			"'connector.type' = 'bytable', \n" +
			"'connector.master-urls' = '10.31.204.16:2001,10.31.204.17:2001,10.31.204.18:2001', \n" +
			"'connector.table' = 'flink-test', \n" +
			"'connector.lookup.cache.ttl' = '60000', " +
			"'connector.lookup.cache.max-rows' = '20' " +
			")");

		final CollectionTableSink tableSink = (CollectionTableSink) new CollectionTableSink().configure(
			new String[]{"id", "msg", "content"},
			new TypeInformation[]{Types.INT(), Types.INT(), Types.STRING()});
		tEnv.registerTableSink("sink", tableSink);

		tEnv.sqlUpdate("insert into sink" +
			" select T.a, D.msg.col1, T.c " +
			" from (select src.a, src.c, PROCTIME() as proc from src) T " +
			" left join bytable_dim FOR SYSTEM_TIME AS OF T.proc as D" +
			" ON T.a = D.rk");

		tEnv.execute("Bytable test");
	}

	static class CollectionTableSink extends TableSinkBase implements BatchTableSink, AppendStreamTableSink {
		public ArrayList<String> list = new ArrayList();

		@Override
		public void emitDataSet(DataSet dataSet) {
			try {
				dataSet.print();
			} catch (Exception e) {
				e.printStackTrace();
			}
			dataSet.output(new LocalCollectionOutputFormat(list)).setParallelism(1);
		}

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
			return dataStream.writeUsingOutputFormat(new LocalCollectionOutputFormat(list)).setParallelism(1);
		}

		@Override
		public TypeInformation getOutputType() {
			return new RowTypeInfo(Types.INT(), Types.INT(), Types.STRING());
		}
	}

	public static DataStream<Row> getRowDataStream(StreamExecutionEnvironment env) {
		List<Row> testData2 = new ArrayList<>();
		testData2.add(Row.of(11999999, 1L, "Hi"));
		testData2.add(Row.of(22999999, 2L, "Hello"));
		testData2.add(Row.of(33999999, 2L, "Hello world"));
		testData2.add(Row.of(33999999, 3L, "Hello world!"));

		Collections.shuffle(testData2);
		return env.fromCollection(testData2);
	}

	public static void main(String[] args) throws Exception {
		BytableDimensionExample bytableDimensionExample = new BytableDimensionExample();
		bytableDimensionExample.testBytableLookupTableSource();
	}
}
