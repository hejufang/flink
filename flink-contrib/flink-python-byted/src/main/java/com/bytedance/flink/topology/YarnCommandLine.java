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

package com.bytedance.flink.topology;

import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.program.ClusterClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Yarn command line.
 */
public class YarnCommandLine extends CliFrontend {
	private static final Logger LOG = LoggerFactory.getLogger(YarnCommandLine.class);

	public YarnCommandLine() throws Exception {
	}

	public YarnCommandLine(String configDir) throws Exception {
		super(configDir);
	}

	public ClusterClient createClusterClient(String[] args) {
		LOG.info("Create cluster client, where args = {}", Arrays.asList(args));
		args = setJobName(args);
		RunOptions options;
		try {
			options = CliFrontendParser.parseRunCommand(args);
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}

		if (options.getJarFilePath() == null) {
			throw new RuntimeException(new CliArgsException("The program JAR file was not specified."));
		}

		ClusterClient client = null;
		try {
			client = createClient(args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return client;
	}
}
