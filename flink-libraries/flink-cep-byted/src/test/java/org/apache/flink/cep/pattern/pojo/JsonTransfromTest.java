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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test for conversion from json string to {@link PatternPojo}.
 */
public class JsonTransfromTest {

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
				"\t\t\t\"window\": 1000\n" +
				"\t\t}\n" +
				"\t}\n" +
				"}";

		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojo pattern = objectMapper.readValue(json, PatternPojo.class);
		Assert.assertEquals("1000", pattern.getPattern().getAttributes().get(PatternBody.AttributeType.WINDOW));
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
				"        ],\n" +
				"        \"attributes\": {\n" +
				"        }\n" +
				"    }\n" +
				"}";

		ObjectMapper objectMapper = new ObjectMapper();
		PatternPojo pattern = objectMapper.readValue(json, PatternPojo.class);

		Assert.assertEquals("rule_xx", pattern.getId());
		Assert.assertEquals(2, pattern.getPattern().getEvents().size());
		Assert.assertEquals(Event.ConnectionType.FOLLOWED_BY, pattern.getPattern().getEvents().get(1).getConnection());
	}
}
