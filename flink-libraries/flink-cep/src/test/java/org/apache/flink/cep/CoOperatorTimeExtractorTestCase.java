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

package org.apache.flink.cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.functions.timestamps.CepTimestampExtractor;
import org.apache.flink.cep.pattern.parser.TestTupleParser;
import org.apache.flink.cep.time.Time;
import org.apache.flink.cep.utils.CEPUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * CoOperatorITCase.
 */
public class CoOperatorTimeExtractorTestCase implements Serializable{

	private static ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<>(1024);

	@Before
	public void setup() {
		queue.clear();
	}

	private String readPatternFromResource(String path) throws IOException {
		StringBuilder contentBuilder = new StringBuilder();
		boolean skip = false;
		try (InputStream stream = getClass().getClassLoader().getResourceAsStream(path)) {
			BufferedReader br = new BufferedReader(new InputStreamReader(stream));
			String line;
			line = br.readLine();
			while (line != null){
				if (line.trim().equals("/*")) {
					skip = true;
				} else if (line.trim().equals("*/")) {
					skip = false;
				} else {
					if (!skip) {
						contentBuilder.append(line);
						contentBuilder.append("\n");
					}
				}
				line = br.readLine();
			}
		}
		return contentBuilder.toString();
	}

	@Test
	public void testCustomizedConditionWithTimestampExtractor() throws IOException {
		Object[] data = {
				readPatternFromResource("patterns/customized_pattern.txt"),
				new Barrier(),
				Tuple2.of(new Event(0, "buy", 9999999.0), 999999L),

				Tuple2.of(new Event(1, "buy", 9999999.0), 12L), //buffer
				Tuple2.of(new Event(1, "buy", 9999999.0), 13L), //buffer

				Tuple2.of(new Event(2, "buy", 9999999.0), 12L), //result
				Tuple2.of(new Event(2, "buy", 9999999.0), 18L), //buffer

				Tuple2.of(new Event(3, "buy", 9999999.0), 12L), //result
				Tuple2.of(new Event(3, "buy", 9999999.0), 15L), //buffer
				Tuple2.of(new Event(3, "buy", 9999999.0), 18L), //buffer

				Tuple2.of(new Event(4, "buy", 9999999.0), 12L), //result
				Tuple2.of(new Event(4, "buy", 9999999.0), 20L), //result
				Tuple2.of(new Event(4, "buy", 9999999.0), 18L), //result
				Tuple2.of(new Event(4, "buy", 9999999.0), 14L), //drop
				Tuple2.of(new Event(4, "buy", 9999999.0), 26L), //buffer
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Arrays.asList("2,12", "3,12", "4,12", "4,18", "4,20"), runTestWithTimestampExtractor(
			new CepTimestampExtractor<Tuple2<Event, Long>>(Time.of(5, TimeUnit.MILLISECONDS)){
				@Override
				public long extractTimestamp(Tuple2<Event, Long> element) {
					return element.f1;
				}
			}, 100000L));
	}

	@Test
	public void testCustomizedConditionWithTimestampExtractorTTL() throws IOException {
		Object[] data = {
			readPatternFromResource("patterns/customized_pattern.txt"),
			new Barrier(),
			Tuple2.of(new Event(1, "buy", 9999999.0), 12L), //buffer
			Tuple2.of(new Event(1, "buy", 9999999.0), 13L), //buffer
			Tuple2.of(new Event(1, "buy", 9999999.0), 14L), //buffer
			Tuple2.of(new Event(1, "buy", 9999999.0), 20L), //buffer
			new Barrier(),
			Tuple2.of(new Event(1, "buy", 9999999.0), 1L), //drop
			new BigBarrier(),
			Tuple2.of(new Event(1, "buy", 9999999.0), 2L), //buffer
			new BigBarrier(),
		};
		queue.addAll(Arrays.asList(data));
		assertEquals(Arrays.asList("1,12", "1,13", "1,14", "1,20", "1,2"), runTestWithTimestampExtractor((
			new CepTimestampExtractor<Tuple2<Event, Long>>(Time.of(5, TimeUnit.MILLISECONDS)){
			@Override
			public long extractTimestamp(Tuple2<Event, Long> element) {
				return element.f1;
			}
		}), 500L));
	}

	private List<String> runTestWithTimestampExtractor(CepTimestampExtractor cepTimestampExtractor, Long stateTTL) throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setBufferTimeout(100);

		HashMap<String, String> ttlProperty = new HashMap();
		ttlProperty.put(CEPUtils.TTL_KEY, String.valueOf(stateTTL));

		DataStream<String> patternJsonStream = env.addSource(new PatternJsonStream());
		DataStream<Tuple2<Event, Long>> input = env.addSource(new EventStream())
			.keyBy((KeySelector<Tuple2<Event, Long>, Integer>) value -> value.f0.getId());
		DataStream<String> result = CEP.pattern(input, patternJsonStream, TestTupleParser::new)
			.withTimestampExtractor(cepTimestampExtractor).withProperties(ttlProperty)
			.process(new MultiplePatternProcessFunction<Tuple2<Event, Long>, String>() {
				@Override
				public void processMatch(Tuple2<String, Map<String, List<Tuple2<Event, Long>>>> match, Context ctx, Object key, Collector<String> out) {

					out.collect(key + "," + match.f1.get("customized").get(0).f1);

				}
			});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		return resultList;
	}

	private static class PatternJsonStream implements SourceFunction<String> {

		PatternJsonStream() {}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (!queue.isEmpty()) {
				if (queue.peek() instanceof String) {
					ctx.collect((String) queue.poll());
					if (queue.peek() instanceof Barrier) {
						Thread.sleep(600);
						queue.poll();
					}
				} else {
					Thread.sleep(1000);
				}
			}
		}

		@Override
		public void cancel() {}
	}

	private static class EventStream implements SourceFunction<Tuple2<Event, Long>> {

		EventStream() {}

		@Override
		public void run(SourceContext<Tuple2<Event, Long>> ctx) throws Exception {
			while (!queue.isEmpty()) {
				if (queue.peek() instanceof Tuple2) {
					Tuple2<Event, Long> tuple2 = (Tuple2<Event, Long>) queue.poll();
					ctx.collect(tuple2);
					Thread.sleep(10);
					if (queue.peek() instanceof Barrier) {
						Thread.sleep(100);
						queue.poll();
					} else if (queue.peek() instanceof BigBarrier) {
						Thread.sleep(1000);
						queue.poll();
					}
				} else {
					Thread.sleep(1000);
				}
			}

		}

		@Override
		public void cancel() {}
	}

	/**
	 * Just for sleep 1s.
	 */
	private static class Barrier implements Serializable {}

	private static class BigBarrier implements Serializable {}

}
