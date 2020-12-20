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

/**
 * TestData for cep2.0, we may add more data in the future.
 */
public class TestData {

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
}
