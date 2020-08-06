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

package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.CHECKPOINT_ANALYZE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CHECKPOINT_ID;

/**
 * Command line options for the SAVEPOINT command.
 */
public class CheckpointOptions extends CommandLineOptions {

	private final String[] args;
	private int checkpointID;
	private boolean isAnalyzation;
	private String metadataPath;

	public CheckpointOptions(CommandLine line) {
		super(line);
		args = line.getArgs();
		if (line.hasOption(CHECKPOINT_ID.getOpt())) {
			checkpointID = Integer.parseInt(line.getOptionValue(CHECKPOINT_ID.getOpt()));
		} else {
			checkpointID = -1;
		}

		if (line.hasOption(CHECKPOINT_ANALYZE_OPTION.getOpt())) {
			isAnalyzation = true;
			metadataPath = line.getOptionValue(CHECKPOINT_ANALYZE_OPTION.getOpt());
		}
	}

	public String[] getArgs() {
		return args == null ? new String[0] : args;
	}

	public int getCheckpointID() {
		return checkpointID;
	}

	public boolean isAnalyzation() {
		return isAnalyzation;
	}

	public String getMetadataPath() {
		return metadataPath;
	}
}
