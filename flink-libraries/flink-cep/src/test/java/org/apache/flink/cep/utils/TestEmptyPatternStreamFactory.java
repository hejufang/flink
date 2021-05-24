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

package org.apache.flink.cep.utils;

import org.apache.flink.cep.functions.EmptyPatternStreamFactory;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * EmptyPatternStreamFactory used to create EmptyPatternSource.
 *
 */
public class TestEmptyPatternStreamFactory implements EmptyPatternStreamFactory {

	@Override
	public <IN> DataStream<Pattern<IN, IN>> createEmptyPatternStream() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		return env.addSource(new TestEmptyPatternSource());
	}

	private static class TestEmptyPatternSource<IN> extends RichSourceFunction<Pattern<IN, ?>> {

		@Override
		public void open(Configuration parameters) throws Exception {
		}

		@Override
		public void run(SourceContext ctx) throws Exception {

		}

		@Override
		public void close() {
			// leave main method
		}

		@Override
		public void cancel() {
			close();
		}
	}
}

