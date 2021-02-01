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

package org.apache.flink.cep.pattern.pojo;

import org.apache.flink.cep.pattern.v2.ConditionGroup;
import org.apache.flink.cep.pattern.v2.LeafCondition;
import org.apache.flink.cep.pattern.v2.PatternPojoV2;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for conversion from json string to {@link PatternPojo}.
 */
public class JsonTransfromTest {

	@Test
	public void testConditionGroup() throws IOException {
		String json = "{\n" +
				"        \"id\": \"rule_xx\",\n" +
				"        \"pattern\": {\n" +
				"                \"events\": [{\n" +
				"                                \"id\": \"imp\",\n" +
				"                                \"conditionGroup\": {\n" +
				"                                                \"op\": \"or\",\n" +
				"                                                \"conditionGroups\": [{\n" +
				"                                                        \"op\": \"or\",\n" +
				"                                                        \"conditionGroups\": [],\n" +
				"                                                        \"conditions\": [{\n" +
				"                                                                \"key\": \"k1\",\n" +
				"                                                                \"op\": \"=\",\n" +
				"                                                                \"value\": \"v1\",\n" +
				"                                                                \"type\": \"string\",\n" +
				"                                                                \"aggregation\": \"none\",\n" +
				"                                                                \"filters\": []\n" +
				"                                                        }, {\n" +
				"                                                                \"key\": \"k2\",\n" +
				"                                                                \"op\": \"=\",\n" +
				"                                                                \"value\": \"v2\",\n" +
				"                                                                \"type\": \"string\",\n" +
				"                                                                \"aggregation\": \"none\",\n" +
				"                                                                \"filters\": []\n" +
				"                                                        }]\n" +
				"                                                }, {\n" +
				"                                                        \"op\": \"and\",\n" +
				"                                                        \"conditionGroups\": [],\n" +
				"                                                        \"conditions\": [{\n" +
				"                                                                \"key\": \"k3\",\n" +
				"                                                                \"op\": \"=\",\n" +
				"                                                                \"value\": \"v3\",\n" +
				"                                                                \"type\": \"string\",\n" +
				"                                                                \"aggregation\": \"none\",\n" +
				"                                                                \"filters\": []\n" +
				"                                                        }, {\n" +
				"                                                                \"key\": \"k4\",\n" +
				"                                                                \"op\": \"=\",\n" +
				"                                                                \"value\": \"v4\",\n" +
				"                                                                \"type\": \"string\",\n" +
				"                                                                \"aggregation\": \"none\",\n" +
				"                                                                \"filters\": []\n" +
				"                                                        }]\n" +
				"                                                }],\n" +
				"                                                \"conditions\": []\n" +
				"                                }        \n" +
				"                        }\n" +
				"                ]\n" +
				"        },\n" +
				"   \"version\": 2\n" +
				"}\n";
		System.out.println(json);
		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojoV2 pattern = (PatternPojoV2) objectMapper.readValue(json, AbstractPatternPojo.class);
		ConditionGroup group = (ConditionGroup) pattern.getEvents().get(0).getConditions().get(0);
		ConditionGroup subGroup1 = group.getGroups().get(0);
		ConditionGroup subGroup2 = group.getGroups().get(1);
		Assert.assertEquals(ConditionGroup.OpType.OR, group.getOp());
		Assert.assertEquals(ConditionGroup.OpType.OR, subGroup1.getOp());
		Assert.assertEquals(ConditionGroup.OpType.AND, subGroup2.getOp());
		Assert.assertEquals("k1", subGroup1.getConditions().get(0).getKey());
		Assert.assertEquals("v2", subGroup1.getConditions().get(1).getValue());
		Assert.assertEquals(LeafCondition.OpType.EQUAL, subGroup1.getConditions().get(1).getOp());
	}

	@Test
	public void testAggregationCondition() throws IOException {
		String json = "{\n" +
				"        \"id\": \"rule_xx\",\n" +
				"        \"pattern\": {\n" +
				"                \"events\": [{\n" +
				"                                \"id\": \"imp\",\n" +
				"                                \"conditions\": [{\n" +
				"                                        \"key\": \"amount\",\n" +
				"                                        \"op\": \">\",\n" +
				"                                        \"value\": \"1.0\",\n" +
				"                                        \"aggregation\": \"sum\",\n" +
				"                                        \"type\": \"long\",\n" +
				"                                        \"filters\": [{\n" +
				"                                                        \"key\": \"eventName\",\n" +
				"                                                        \"op\": \"=\",\n" +
				"                                                        \"value\": \"a\"\n" +
				"                                                },\n" +
				"                                                {\n" +
				"                                                        \"key\": \"id\",\n" +
				"                                                        \"op\": \"=\",\n" +
				"                                                        \"value\": \"1\"\n" +
				"                                                }\n" +
				"                                        ]\n" +
				"                                }]\n" +
				"                        },\n" +
				"                        {\n" +
				"                                \"id\": \"purchase\",\n" +
				"                                \"conditions\": [{\n" +
				"                                        \"key\": \"eventName\",\n" +
				"                                        \"op\": \"=\",\n" +
				"                                        \"value\": \"buy iterm\"\n" +
				"                                }, {\n" +
				"                                        \"key\": \"eventName\",\n" +
				"                                        \"op\": \"=\",\n" +
				"                                        \"value\": \"take iterm\"\n" +
				"                                }],\n" +
				"                                \"connection\": \"NOT_FOLLOWED_BY\",\n" +
				"                                \"after\": \"imp\"\n" +
				"                        }\n" +
				"                ]\n" +
				"        }\n" +
				"}";
		System.out.println(json);
		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojo pattern = objectMapper.readValue(json, PatternPojo.class);
		Assert.assertEquals(Condition.ValueType.LONG, pattern.getBeginEvent().getConditions().get(0).getType());
		Assert.assertEquals(Condition.AggregationType.SUM, pattern.getBeginEvent().getConditions().get(0).getAggregation());
		Assert.assertEquals(2, pattern.getBeginEvent().getConditions().get(0).getFilters().size());
	}

	@Test
	public void testNumberCompareCondition() throws IOException {
		String json = "{\n" +
				"\t\"id\": \"rule_xx\",\n" +
				"\t\"pattern\": {\n" +
				"\t\t\"events\": [{\n" +
				"\t\t\t\t\"id\": \"imp\",\n" +
				"\t\t\t\t\"conditions\": [{\n" +
				"\t\t\t\t\t\"key\": \"amount\",\n" +
				"\t\t\t\t\t\"op\": \">\",\n" +
				"\t\t\t\t\t\"value\": \"1.0\",\n" +
				"                    \"type\": \"long\"\n" +
				"\t\t\t\t}]\n" +
				"\t\t\t},\n" +
				"\t\t\t{\n" +
				"\t\t\t\t\"id\": \"purchase\",\n" +
				"\t\t\t\t\"conditions\": [{\n" +
				"\t\t\t\t\t\"key\": \"eventName\",\n" +
				"\t\t\t\t\t\"op\": \"=\",\n" +
				"\t\t\t\t\t\"value\": \"buy iterm\"\n" +
				"\t\t\t\t}, {\n" +
				"\t\t\t\t\t\"key\": \"eventName\",\n" +
				"\t\t\t\t\t\"op\": \"=\",\n" +
				"\t\t\t\t\t\"value\": \"take iterm\"\n" +
				"\t\t\t\t}],\n" +
				"\t\t\t\t\"connection\": \"NOT_FOLLOWED_BY\",\n" +
				"\t\t\t\t\"after\": \"imp\"\n" +
				"\t\t\t}\n" +
				"\t\t]\n" +
				"\t}\n" +
				"}\n";
		System.out.println(json);
		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojo pattern = objectMapper.readValue(json, PatternPojo.class);
		Assert.assertEquals(Condition.ValueType.LONG, pattern.getBeginEvent().getConditions().get(0).getType());
	}

	@Test
	public void testMultipleCondition() throws IOException {
		String json = "{\n" +
				"\t\"id\": \"rule_xx\",\n" +
				"\t\"pattern\": {\n" +
				"\t\t\"events\": [{\n" +
				"\t\t\t\t\"id\": \"imp\",\n" +
				"\t\t\t\t\"conditions\": [{\n" +
				"\t\t\t\t\t\"key\": \"eventName\",\n" +
				"\t\t\t\t\t\"op\": \"=\",\n" +
				"\t\t\t\t\t\"value\": \"see iterm\"\n" +
				"\t\t\t\t}]\n" +
				"\t\t\t},\n" +
				"\t\t\t{\n" +
				"\t\t\t\t\"id\": \"purchase\",\n" +
				"\t\t\t\t\"conditions\": [{\n" +
				"\t\t\t\t\t\"key\": \"eventName\",\n" +
				"\t\t\t\t\t\"op\": \"=\",\n" +
				"\t\t\t\t\t\"value\": \"buy iterm\"\n" +
				"\t\t\t\t}, {\n" +
				"\t\t\t\t\t\"key\": \"eventName\",\n" +
				"\t\t\t\t\t\"op\": \"=\",\n" +
				"\t\t\t\t\t\"value\": \"take iterm\"\n" +
				"\t\t\t\t}],\n" +
				"\t\t\t\t\"connection\": \"NOT_FOLLOWED_BY\",\n" +
				"\t\t\t\t\"after\": \"imp\"\n" +
				"\t\t\t}\n" +
				"\t\t],\n" +
				"\t\t\"attributes\": {\n" +
				"\t\t\t\"window\": 1000,\n" +
				"\t\t\t\"allowSinglePartialMatchPerKey\": true\n" +
				"\t\t}\n" +
				"\t}\n" +
				"}";

		System.out.println(json);
		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojo pattern = objectMapper.readValue(json, PatternPojo.class);
		Assert.assertEquals(1, pattern.getBeginEvent().getConditions().size());
		Assert.assertEquals(2, pattern.getEventAfter(pattern.getBeginEvent()).getConditions().size());
	}

	@Test
	public void testNotFollowedBy() throws IOException {
		String json = "{\n" +
				"\t\"id\": \"rule_xx\",\n" +
				"\t\"pattern\": {\n" +
				"\t\t\"events\": [{\n" +
				"\t\t\t\t\"id\": \"imp\",\n" +
				"\t\t\t\t\"conditions\": [{\n" +
				"\t\t\t\t\t\"key\": \"eventName\",\n" +
				"\t\t\t\t\t\"op\": \"=\",\n" +
				"\t\t\t\t\t\"value\": \"see iterm\"\n" +
				"\t\t\t\t}]\n" +
				"\t\t\t},\n" +
				"\t\t\t{\n" +
				"\t\t\t\t\"id\": \"purchase\",\n" +
				"\t\t\t\t\"conditions\": [{\n" +
				"\t\t\t\t\t\"key\": \"eventName\",\n" +
				"\t\t\t\t\t\"op\": \"=\",\n" +
				"\t\t\t\t\t\"value\": \"buy iterm\"\n" +
				"\t\t\t\t}],\n" +
				"\t\t\t\t\"connection\": \"NOT_FOLLOWED_BY\",\n" +
				"\t\t\t\t\"after\": \"imp\"\n" +
				"\t\t\t}\n" +
				"\t\t],\n" +
				"\t\t\"attributes\": {\n" +
				"\t\t\t\"window\": 1000,\n" +
				"\t\t\t\"allowSinglePartialMatchPerKey\": true\n" +
				"\t\t}\n" +
				"\t}\n" +
				"}";

		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojo pattern = objectMapper.readValue(json, PatternPojo.class);
		Assert.assertEquals("1000", pattern.getPattern().getAttributes().get(PatternBody.AttributeType.WINDOW));
		Assert.assertTrue(Boolean.parseBoolean(pattern.getPattern().getAttributes().get(PatternBody.AttributeType.ALLOW_SINGLE_PARTIAL_MATCH_PER_KEY)));
	}

	@Test
	public void testFollowedBy() throws IOException {
		String json = "{\n" +
				"    \"id\": \"rule_xx\",\n" +
				"    \"pattern\": {\n" +
				"        \"events\": [\n" +
				"            {\n" +
				"                \"id\": \"imp\",\n" +
				"                \"conditions\": [\n" +
				"                    {\n" +
				"                        \"key\": \"eventName\",\n" +
				"                        \"op\": \"=\",\n" +
				"                        \"value\": \"see iterm\"\n" +
				"                    }\n" +
				"                ]\n" +
				"            },\n" +
				"            {\n" +
				"                \"id\": \"purchase\",\n" +
				"                \"conditions\": [\n" +
				"                    {\n" +
				"                        \"key\": \"eventName\",\n" +
				"                        \"op\": \"=\",\n" +
				"                        \"value\": \"buy iterm\"\n" +
				"                    }\n" +
				"                ],\n" +
				"                \"connection\": \"FOLLOWED_BY\",\n" +
				"                \"after\": \"imp\"\n" +
				"            }\n" +
				"        ]\n" +
				"    }\n" +
				"}";

		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojo pattern = objectMapper.readValue(json, PatternPojo.class);

		Assert.assertEquals("rule_xx", pattern.getId());
		Assert.assertEquals(2, pattern.getPattern().getEvents().size());
		Assert.assertEquals(0, pattern.getPattern().getAttributes().size());
		Assert.assertEquals(Event.ConnectionType.FOLLOWED_BY, pattern.getPattern().getEvents().get(1).getConnection());
	}
}
