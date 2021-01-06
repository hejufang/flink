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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * IT tests for RedisOutputFormat.
 */
public class RedisOutputFormatITCase {
	RedisOutputFormat outputFormat;
	@Before
	public void before() {
		RedisOptions.RedisOptionsBuilder redisOptionsBuilder =
			RedisOptions.builder();
		redisOptionsBuilder.setCluster("dummy_cluster");
		redisOptionsBuilder.setPsm("dummy_psm");
		RedisOutputFormat.RedisOutputFormatBuilder builder =
			RedisOutputFormat.buildRedisOutputFormat();
		builder.setOptions(redisOptionsBuilder.build());
		outputFormat = builder.build();
	}

	@Test
	public void testBufferReduce(){
		Map<Object, Object> reduceBuffer = new HashMap<>();
		Tuple2<Boolean, Row> tuple1 = new Tuple2<>(true, Row.of(new byte[]{1}, 1));
		outputFormat.addToReduceBuffer(tuple1, reduceBuffer);
		Tuple2<Boolean, Row> tuple2 = new Tuple2<>(true, Row.of(new byte[]{1}, 2));
		outputFormat.addToReduceBuffer(tuple2, reduceBuffer);
		assertEquals(reduceBuffer.size(), 1);
	}
}
