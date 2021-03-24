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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.org.apache.commons.cli.CommandLine;

import java.util.Arrays;

import static org.apache.flink.client.cli.CliFrontendParser.ARGS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CHECKPOINT_ANALYZE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CHECKPOINT_ID;
import static org.apache.flink.client.cli.CliFrontendParser.CHECKPOINT_VERIFY_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.JAR_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PARALLELISM_OPTION;

/**
 * Command line options for the SAVEPOINT command.
 */
public class CheckpointOptions extends CommandLineOptions {

	private final String[] args;
	private int checkpointID;
	private boolean isAnalyzation;
	private String metadataPath;
	private final String[] programArgs;

	//---------------------------------------------------
	// Args for checkpoint verification at client
	//---------------------------------------------------
	private boolean isVerification;
	private String jarFilePath;
	private String entryPointClass;
	private int parallelism;

	public CheckpointOptions(CommandLine line) throws CliArgsException {
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

		if (line.hasOption(CHECKPOINT_VERIFY_OPTION.getOpt())) {
			isVerification = true;
			this.entryPointClass = line.hasOption(CLASS_OPTION.getOpt()) ?
				line.getOptionValue(CLASS_OPTION.getOpt()) : null;
			Preconditions.checkNotNull(this.entryPointClass, "Command checkpoint verify must set main class path with -c");
			this.jarFilePath = line.hasOption(JAR_OPTION.getOpt()) ?
				line.getOptionValue(JAR_OPTION.getOpt()) : null;
			Preconditions.checkNotNull(this.jarFilePath, "Command checkpoint verify must set jar file path with -j");

			if (line.hasOption(PARALLELISM_OPTION.getOpt())) {
				String parString = line.getOptionValue(PARALLELISM_OPTION.getOpt());
				try {
					parallelism = Integer.parseInt(parString);
					if (parallelism <= 0) {
						throw new NumberFormatException();
					}
				}
				catch (NumberFormatException e) {
					throw new CliArgsException("The parallelism must be a positive number: " + parString);
				}
			}
			else {
				parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
			}
		}

		this.programArgs = extractProgramArgs(line);
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

	public boolean isVerification() {
		return isVerification;
	}

	public String getJarFilePath() {
		return jarFilePath;
	}

	public String getEntryPointClassName() {
		return entryPointClass;
	}

	public int getParallelism() {
		return parallelism;
	}

	public String[] getProgramArgs() {
		return programArgs;
	}

	protected String[] extractProgramArgs(CommandLine line) {
		String[] args = line.hasOption(ARGS_OPTION.getOpt()) ?
			line.getOptionValues(ARGS_OPTION.getOpt()) :
			line.getArgs();

		if (args.length > 0 && !line.hasOption(JAR_OPTION.getOpt())) {
			jarFilePath = args[0];
			args = Arrays.copyOfRange(args, 1, args.length);
		}

		return args;
	}
}
