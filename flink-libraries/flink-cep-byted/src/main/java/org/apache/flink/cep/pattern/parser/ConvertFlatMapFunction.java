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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.EventParserCondition;
import org.apache.flink.cep.pattern.pojo.Event;
import org.apache.flink.cep.pattern.pojo.PatternBody;
import org.apache.flink.cep.pattern.pojo.PatternPojo;
import org.apache.flink.cep.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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

	@SuppressWarnings("unchecked")
	@VisibleForTesting
	public Pattern<IN, IN> buildPattern(PatternPojo pojo) {
		final Pattern<IN, IN> begin = (Pattern<IN, IN>) Pattern.begin(pojo.getBeginEvent().getId())
				.where(new EventParserCondition<>(cepEventParser.duplicate(), pojo.getBeginEvent().getConditions().get(0)));

		Pattern<IN, IN> compositePattern = begin;
		Event tempEvent = pojo.getBeginEvent();
		while (pojo.getEventAfter(tempEvent) != null) {
			final Event afterEvent = pojo.getEventAfter(tempEvent);
			final Event.ConnectionType connectionType = afterEvent.getConnection();

			Pattern<IN, IN> nextPattern;
			if (connectionType == Event.ConnectionType.FOLLOWED_BY) {
				nextPattern = compositePattern.followedBy(afterEvent.getId())
						.where(new EventParserCondition<>(cepEventParser.duplicate(), afterEvent.getConditions().get(0)));
			} else if (connectionType == Event.ConnectionType.NOT_FOLLOWED_BY) {
				nextPattern = compositePattern.notFollowedBy(afterEvent.getId())
						.where(new EventParserCondition<>(cepEventParser.duplicate(), afterEvent.getConditions().get(0)));
			} else {
				throw new UnsupportedOperationException(String.format("ConnectionType %s is not supported.", connectionType));
			}

			compositePattern = nextPattern;
			tempEvent = afterEvent;
		}

		// attributes
		Map<PatternBody.AttributeType, String> attributes = pojo.getPattern().getAttributes();
		for (Map.Entry<PatternBody.AttributeType, String> attr : attributes.entrySet()) {
			switch (attr.getKey()) {
				case WINDOW:
					compositePattern.within(Time.milliseconds(Long.parseLong(attr.getValue())));
					break;
				case ALLOW_SINGLE_PARTIAL_MATCH_PER_KEY:
					compositePattern.setAllowSinglePartialMatchPerKey(Boolean.parseBoolean(attr.getValue()));
					break;
				default:
					throw new UnsupportedOperationException(String.format("AttributeType %s is not supported.", attr.getKey()));
			}
		}

		compositePattern.setPatternId(pojo.getId());
		return compositePattern;
	}

	@Override
	public void flatMap(String json, Collector<Pattern<IN, IN>> out) throws Exception {
		final PatternPojo pojo = objectMapper.readValue(json, PatternPojo.class);
		if (!LegalPatternPojoChecker.isPatternPojoLegal(pojo)) {
			LOG.warn("{} is not legal, dropping...", pojo);
		}

		out.collect(buildPattern(pojo));
	}
}
