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

package org.apache.flink.cep.functions;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * SimpleEmptyPatternStreamFactory used to create EmptyPatternSource.
 * EmptyPatternSource will do nothing keep waiting until EmptyPatternSource.close was invoked
 */
public class SimpleEmptyPatternStreamFactory implements EmptyPatternStreamFactory{

	@Override
	public <IN> DataStream<Pattern<IN, IN>> createEmptyPatternStream() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		return env.addSource(new EmptyPatternSource());
	}

	private static class EmptyPatternSource<IN> extends RichSourceFunction<Pattern<IN, ?>> {

		private boolean isRunning = true;
		private transient Object waitLock;

		@Override
		public void open(Configuration parameters) throws Exception {
			waitLock = new Object();
		}

		@Override
		public void run(SourceContext ctx) throws Exception {
			// just wait now
			while (isRunning) {
				synchronized (waitLock) {
					waitLock.wait(100L);
				}
			}
		}

		@Override
		public void close() {
			this.isRunning = false;
			// leave main method
			synchronized (waitLock) {
				waitLock.notify();
			}
		}

		@Override
		public void cancel() {
			close();
		}
	}
}

