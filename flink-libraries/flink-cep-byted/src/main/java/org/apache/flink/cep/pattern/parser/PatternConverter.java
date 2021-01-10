/*
c * Licensed to the Apache Software Foundation (ASF) under one
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
import org.apache.flink.cep.pattern.conditions.EventParserCondition;
import org.apache.flink.cep.pattern.pojo.Event;
import org.apache.flink.cep.pattern.pojo.PatternBody;
import org.apache.flink.cep.pattern.pojo.PatternPojo;
import org.apache.flink.cep.time.Time;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.cep.utils.CEPUtils.generateUniqueId;

/**
 * PatternConverter.
 */
public class PatternConverter implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(PatternConverter.class);

	public static <IN> Optional<Pattern<IN, IN>> buildPattern(ObjectMapper objectMapper, String json, CepEventParser cepEventParser) throws IOException {
		final PatternPojo pojo = objectMapper.readValue(json, PatternPojo.class);
		if (!LegalPatternPojoChecker.isPatternPojoLegal(pojo)) {
			LOG.warn("{} is not legal, dropping...", pojo);
			return Optional.empty();
		} else {
			return Optional.of(buildPattern(pojo, cepEventParser));
		}
	}

	public static <IN> Pattern<IN, IN> buildPattern(PatternPojo pojo, CepEventParser cepEventParser) {
		String patternId = pojo.getId();
		int hash = pojo.hashCode();

		if (pojo.getStatus().equals(PatternPojo.StatusType.DISABLED)) {
			final Pattern<IN, IN> disabledPattern = Pattern.begin("ignored");
			disabledPattern.setPatternMeta(pojo.getId(), -1);
			disabledPattern.setDisabled(true);
			return disabledPattern;
		}

		// used to generate the unique state name
		int conditionPos = 0;

		final Pattern<IN, IN> begin = (Pattern<IN, IN>) Pattern.begin(pojo.getBeginEvent().getId())
				.where(new EventParserCondition<>(cepEventParser.duplicate(), pojo.getBeginEvent().getConditions(), generateUniqueId(patternId, hash) + "-" + conditionPos++));

		Pattern<IN, IN> compositePattern = begin;
		Event tempEvent = pojo.getBeginEvent();
		while (pojo.getEventAfter(tempEvent) != null) {
			final Event afterEvent = pojo.getEventAfter(tempEvent);
			final Event.ConnectionType connectionType = afterEvent.getConnection();

			Pattern<IN, IN> nextPattern;
			if (connectionType == Event.ConnectionType.FOLLOWED_BY) {
				nextPattern = compositePattern.followedBy(afterEvent.getId())
						.where(new EventParserCondition<>(cepEventParser.duplicate(), afterEvent.getConditions(), generateUniqueId(patternId, hash) + "-" + conditionPos++));
			} else if (connectionType == Event.ConnectionType.NOT_FOLLOWED_BY) {
				nextPattern = compositePattern.notFollowedBy(afterEvent.getId())
						.where(new EventParserCondition<>(cepEventParser.duplicate(), afterEvent.getConditions(), generateUniqueId(patternId, hash) + "-" + conditionPos++));
			} else {
				throw new UnsupportedOperationException(String.format("ConnectionType %s is not supported.", connectionType));
			}

			compositePattern = nextPattern;
			tempEvent = afterEvent;
		}

		// attributes, set a few default values here for most cases
		Map<PatternBody.AttributeType, String> attributes = pojo.getPattern().getAttributes();
		compositePattern.setAllowSinglePartialMatchPerKey(true);
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

		compositePattern.setPatternMeta(patternId, hash);
		LOG.info("Receive a new pattern json(id={}, hash={})", patternId, hash);
		return compositePattern;
	}
}
