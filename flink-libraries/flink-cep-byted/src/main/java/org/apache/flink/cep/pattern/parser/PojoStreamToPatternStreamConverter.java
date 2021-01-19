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

package org.apache.flink.cep.pattern.parser;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Converter from {@link org.apache.flink.cep.pattern.pojo.PatternPojo} to {@link Pattern}.
 */
public class PojoStreamToPatternStreamConverter {

	public static <IN> DataStream<Pattern<IN, IN>> convert(DataStream<String> patternJsonStream, CepEventParserFactory cepEventParserFactory) {
		return patternJsonStream.<Pattern<IN, IN>>flatMap(new ConvertFlatMapFunction<>(cepEventParserFactory)).setParallelism(1).uid("pattern-converter");
	}
}
