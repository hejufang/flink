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

package org.apache.flink.rocketmq.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.rocketmq.source.split.RocketMQSplitState;

/**
 * The {@link RecordEmitter} implementation for {@link RocketMQSourceReader}.
 * */
public class RocketMQRecordEmitter<OUT>
		implements RecordEmitter<Tuple3<OUT, Long, Long>, OUT, RocketMQSplitState>{

	@Override
	public void emitRecord(
			Tuple3<OUT, Long, Long> element,
			SourceOutput<OUT> output,
			RocketMQSplitState splitState) throws Exception {
		output.collect(element.f0, element.f2);
		splitState.setCurrentOffset(element.f1 + 1);
	}
}
