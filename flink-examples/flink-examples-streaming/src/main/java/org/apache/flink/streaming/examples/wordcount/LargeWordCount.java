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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * A Large wordcount example.
 */
public class LargeWordCount {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(20000);
		DataStream<Tuple2<String, Integer>> source = env.addSource(new ParallelSourceFunction<Tuple2<String, Integer>>() {
			boolean running = true;
			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
				int i = 1;
				while (running && i < 100) {
					ctx.collect(new Tuple2<>("test", 1));
					i++;
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		source.keyBy(0).sum(1).print();
		env.execute("large word count");
	}

}
