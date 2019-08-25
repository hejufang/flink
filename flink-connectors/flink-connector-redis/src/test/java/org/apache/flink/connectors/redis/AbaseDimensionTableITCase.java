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

package org.apache.flink.connectors.redis;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSinkBase;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * test abase dimension.
 */
public class AbaseDimensionTableITCase {
	@Test
	public void testDimensionTable() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		tEnv.getConfig().getConfiguration()
				.setString("table.exec.resource.default-parallelism", "1");

		DataStream<Tuple2<Integer, String>> ds = getTuple2DataStream(env);
		Table t = tEnv.fromDataStream(ds, "id, text");
		tEnv.registerTable("source", t);

		tEnv.sqlUpdate("create table abase_dim(" +
				" id bigint," +
				" title varchar" +
				") with (" +
				"'connector.type' = 'abase'," +
				"'connector.table' = 'sandbox'," +
				"'connector.cluster' = 'abase_sandbox.service.lf'," +
				"'connector.psm' = 'inf.compute.test'" +
				")");

		final CollectionTableSink tableSink = (CollectionTableSink) new CollectionTableSink().configure(
				new String[]{"id", "text", "title"},
				new TypeInformation[]{Types.INT(), Types.STRING(), Types.STRING()});
		tEnv.registerTableSink("sink", tableSink);

		tEnv.sqlUpdate("insert into sink" +
				" select T.id, T.text, D.title" +
				" from (select source.id, source.text, PROCTIME() as proc from source) T" +
				" left join abase_dim FOR SYSTEM_TIME AS OF T.proc AS D" +
				"  on T.id=D.id ");

		tEnv.execute("luoqi test");
	}

	static class CollectionTableSink extends TableSinkBase implements BatchTableSink, org.apache.flink.table.sinks.AppendStreamTableSink {
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
			return new RowTypeInfo(Types.INT(), Types.STRING(), Types.STRING());
		}
	}

	public static DataStream<Tuple2<Integer, String>> getTuple2DataStream(StreamExecutionEnvironment env) {
		List<Tuple2<Integer, String>> data = new ArrayList<>();
		data.add(new Tuple2<>(1, "Hi"));
		data.add(new Tuple2<>(2, "Hello"));
		data.add(new Tuple2<>(3, "Hello world"));
		data.add(new Tuple2<>(4, "Hello world, how are you?"));
		data.add(new Tuple2<>(5, "I am fine."));
		data.add(new Tuple2<>(6, "Luke Skywalker"));
		data.add(new Tuple2<>(7, "Comment#1"));
		data.add(new Tuple2<>(8, "Comment#2"));
		data.add(new Tuple2<>(9, "Comment#3"));
		data.add(new Tuple2<>(10, "Comment#4"));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}
}

