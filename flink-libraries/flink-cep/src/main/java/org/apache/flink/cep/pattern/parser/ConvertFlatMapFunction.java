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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.pojo.PatternPojo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Main implementation for convertion from {@link PatternPojo} to {@link Pattern}.
 * @param <IN>
 */
public class ConvertFlatMapFunction<IN> extends RichFlatMapFunction<String, Pattern<IN, IN>> {
	private static final Logger LOG = LoggerFactory.getLogger(ConvertFlatMapFunction.class);

	private ObjectMapper objectMapper;

	private final CepEventParser cepEventParser;

	public ConvertFlatMapFunction(CepEventParserFactory factory) {
		this.cepEventParser = factory.create();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public void flatMap(String json, Collector<Pattern<IN, IN>> out) throws Exception {
		try {
			Optional<Pattern<IN, IN>> pattern = PatternConverter.buildPattern(objectMapper, json, cepEventParser);
			pattern.ifPresent(out::collect);
		} catch (Throwable t) {
			LOG.warn("Fail to parse pattern. {}", json, t);
		}
	}
}
