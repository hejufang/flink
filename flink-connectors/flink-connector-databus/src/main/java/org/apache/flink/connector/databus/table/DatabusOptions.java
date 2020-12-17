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

package org.apache.flink.connector.databus.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configs describe the Databus Connector.
 */
public class DatabusOptions {
	public static final ConfigOption<String> CHANNEL = ConfigOptions
		.key("channel")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Name of databus channel.");
	public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS = ConfigOptions
		.key("sink.buffer-flush.max-rows")
		.intType()
		.defaultValue(1000)
		.withDescription("Optional. The max size of buffered records before flush. Can be set to zero to disable it.");
	public static final ConfigOption<Integer> SINK_MAX_BUFFER_BYTES = ConfigOptions
		.key("sink.max-buffer-bytes")
		.intType()
		.defaultValue(33554432)
		.withDescription("Optional. Max buffer size when databus agent is not available. " +
			"See https://doc.bytedance.net/docs/1019/1253/12694/#%E6%8E%A5%E5%8F%A3%E8%A1%8C%E4%B8%BA%E5%AE%9A%E4%B9%89 " +
			"for more detail");
	public static final ConfigOption<Boolean> SINK_NEED_RESPONSE = ConfigOptions
		.key("sink.need-response")
		.booleanType()
		.defaultValue(true)
		.withDescription("Optional. Flag indicating whether need response." +
			"See https://doc.bytedance.net/docs/1019/1253/12694/#%E6%8E%A5%E5%8F%A3%E8%A1%8C%E4%B8%BA%E5%AE%9A%E4%B9%89 " +
			"for more detail");
}
