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

package org.apache.flink.runtime.configuration;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for HDFS.
 */
public class HdfsConfigOptions {
	/**
	 * HDFS client block write retries.
	 */
	public static final ConfigOption<Integer> HDFS_CLIENT_BLOCK_WRITE_RETRIES =
			key("dfs.client.block.write.retries")
					.defaultValue(3)
					.withDescription("HDFS client block write retries");

	public static final ConfigOption<String> HDFS_DEFAULT_FS =
			key("fs.defaultFS")
					.defaultValue("")
					.withDescription("HDFS defaultFS");

	public static final ConfigOption<Integer> HDFS_SOCKET_WRITE_TIMEOUT =
			key("dfs.datanode.socket.write.timeout")
					.defaultValue(15000)
					.withDescription("HDFS socket write timeout");

	public static final ConfigOption<String> HDFS_VIP_IP_PORT =
		key("dfs.vip.ipPort")
			.defaultValue("10.8.13.12:65212")
			.withDescription("HDFS vip ip + port");
}