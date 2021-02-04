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

package org.apache.flink.cep.test;

import org.junit.Test;

/**
 * TestData for cep2.0, we may add more data in the future.
 */
public class TestData {

	@Test
	public void testPrint() {
		System.out.println(CONDITION_GROUP_PATTERN);
		System.out.println("---");
		System.out.println(COUNT_PATTERN_1);
	}

	public static final String PATTERN_DATA_1 = "{\n" +
			"    \"id\": \"test_pattern\",\n" +
			"    \"pattern\": {\n" +
			"        \"events\": [\n" +
			"            {\n" +
			"                \"id\": \"start\",\n" +
			"                \"conditions\": [\n" +
			"                    {\n" +
			"                        \"key\": \"name\",\n" +
			"                        \"op\": \"=\",\n" +
			"                        \"value\": \"start\"\n" +
			"                    }\n" +
			"                ]\n" +
			"            },\n" +
			"            {\n" +
			"                \"id\": \"middle\",\n" +
			"                \"connection\": \"FOLLOWED_BY\",\n" +
			"                \"after\": \"start\",\n" +
			"                \"conditions\": [\n" +
			"                    {\n" +
			"                        \"key\": \"name\",\n" +
			"                        \"op\": \"=\",\n" +
			"                        \"value\": \"middle\"\n" +
			"                    }\n" +
			"                ]\n" +
			"            },\n" +
			"            {\n" +
			"                \"id\": \"end\",\n" +
			"                \"connection\": \"FOLLOWED_BY\",\n" +
			"                \"after\": \"middle\",\n" +
			"                \"conditions\": [\n" +
			"                    {\n" +
			"                        \"key\": \"name\",\n" +
			"                        \"op\": \"=\",\n" +
			"                        \"value\": \"end\"\n" +
			"                    }\n" +
			"                ]\n" +
			"            }\n" +
			"        ],\n" +
			"        \"attributes\": {}\n" +
			"    }\n" +
			"}";

	public static final String SUM_PATTERN_1 = "{\n" +
			"        \"id\": \"test_agg\",\n" +
			"        \"pattern\": {\n" +
			"                \"events\": [{\n" +
			"                        \"id\": \"imp\",\n" +
			"                        \"conditions\": [{\n" +
			"                                \"key\": \"price\",\n" +
			"                                \"op\": \">\",\n" +
			"                                \"value\": \"5.0\",\n" +
			"                                \"aggregation\": \"sum\",\n" +
			"                                \"type\": \"double\",\n" +
			"                                \"filters\": [{\n" +
			"                                                \"key\": \"name\",\n" +
			"                                                \"op\": \"=\",\n" +
			"                                                \"value\": \"buy\"\n" +
			"                                        },\n" +
			"                                        {\n" +
			"                                                \"key\": \"id\",\n" +
			"                                                \"op\": \"=\",\n" +
			"                                                \"value\": \"1\"\n" +
			"                                        }\n" +
			"                                ]\n" +
			"                        }]\n" +
			"                }]\n" +
			"        }\n" +
			"}";

	public static final String COUNT_PATTERN_1 = "{\n" +
			"        \"id\": \"test_count\",\n" +
			"        \"pattern\": {\n" +
			"                \"events\": [{\n" +
			"                        \"id\": \"imp\",\n" +
			"                        \"conditions\": [{\n" +
			"                                \"key\": \"id\",\n" +
			"                                \"op\": \">\",\n" +
			"                                \"value\": \"2\",\n" +
			"                                \"aggregation\": \"count\",\n" +
			"                                \"type\": \"long\",\n" +
			"                                \"filters\": [{\n" +
			"                                                \"key\": \"name\",\n" +
			"                                                \"op\": \"=\",\n" +
			"                                                \"value\": \"buy\"\n" +
			"                                        },\n" +
			"                                        {\n" +
			"                                                \"key\": \"id\",\n" +
			"                                                \"op\": \"=\",\n" +
			"                                                \"value\": \"1\"\n" +
			"                                        }\n" +
			"                                ]\n" +
			"                        }]\n" +
			"                }]\n" +
			"        }\n" +
			"}";

	public static final String COUNT_PATTERN_2 = "{\n" +
			"        \"id\": \"test_agg\",\n" +
			"        \"pattern\": {\n" +
			"                \"events\": [{\n" +
			"                        \"id\": \"imp\",\n" +
			"                        \"conditions\": [{\n" +
			"                                \"key\": \"id\",\n" +
			"                                \"op\": \">\",\n" +
			"                                \"value\": \"1\",\n" +
			"                                \"aggregation\": \"count\",\n" +
			"                                \"type\": \"long\",\n" +
			"                                \"filters\": [{\n" +
			"                                                \"key\": \"name\",\n" +
			"                                                \"op\": \"=\",\n" +
			"                                                \"value\": \"imp\"\n" +
			"                                        },\n" +
			"                                        {\n" +
			"                                                \"key\": \"id\",\n" +
			"                                                \"op\": \"=\",\n" +
			"                                                \"value\": \"1\"\n" +
			"                                        }\n" +
			"                                ]\n" +
			"                        }]\n" +
			"                }]\n" +
			"        }\n" +
			"}";

	public static final String FOLLOWEDBY_PATTERN = "{\n" +
			"    \"id\": \"pattern_followedby\",\n" +
			"    \"pattern\": {\n" +
			"        \"events\": [\n" +
			"            {\n" +
			"                \"id\": \"start\",\n" +
			"                \"conditions\": [\n" +
			"                    {\n" +
			"                        \"key\": \"name\",\n" +
			"                        \"op\": \"=\",\n" +
			"                        \"value\": \"buy\"\n" +
			"                    }\n" +
			"                ]\n" +
			"            },\n" +
			"            {\n" +
			"                \"id\": \"middle\",\n" +
			"                \"connection\": \"FOLLOWED_BY\",\n" +
			"                \"after\": \"start\",\n" +
			"                \"conditions\": [\n" +
			"                    {\n" +
			"                        \"key\": \"name\",\n" +
			"                        \"op\": \"=\",\n" +
			"                        \"value\": \"middle\"\n" +
			"                    }\n" +
			"                ]\n" +
			"            }\n" +
			"        ],\n" +
			"        \"attributes\": {\n" +
			"                \"allowSinglePartialMatchPerKey\": true\n" +
			"        }\n" +
			"    }\n" +
			"}";

	public static final String CONDITION_GROUP_PATTERN = "{\n" +
			"        \"id\": \"rule_xx\",\n" +
			"        \"pattern\": {\n" +
			"                \"events\": [{\n" +
			"                                \"id\": \"imp\",\n" +
			"                                \"conditionGroup\": \n" +
			"                                        {\n" +
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
			"                                        }\n" +
			"                                \n" +
			"                        }\n" +
			"                ]\n" +
			"        },\n" +
			"        \"version\": 2\n" +
			"}\n";

	public static final String ILLEGAL_PATTERN_1 = "{\n" +
			"        \"id\": \"test_count\",\n" +
			"        \"pattern\": {\n" +
			"                \"events\": [{\n" +
			"                        \"id\": \"imp\",\n" +
			"                        \"conditions\": [{\n" +
			"                                \"key\": \"id\",\n" +
			"                                \"op\": \">\",\n" +
			"                                \"value\": \"2\",\n" +
			"                                \"type\": \"long\",\n" +
			"                                \"filters\": [{\n" +
			"                                                \"key\": \"name\",\n" +
			"                                                \"op\": \"=\",\n" +
			"                                                \"value\": \"buy\"\n" +
			"                                        },\n" +
			"                                        {\n" +
			"                                                \"key\": \"id\",\n" +
			"                                                \"op\": \"=\",\n" +
			"                                                \"value\": \"1\"\n" +
			"                                        }\n" +
			"                                ]\n" +
			"                        }]\n" +
			"                }]\n" +
			"        }\n" +
			"}\n";

	public static String disablePattern(String patternId) {
		return "{\n" +
				"        \"id\": \"" + patternId + "\",\n" +
				"        \"status\": \"disabled\"\n" +
				"}";
	}
}
