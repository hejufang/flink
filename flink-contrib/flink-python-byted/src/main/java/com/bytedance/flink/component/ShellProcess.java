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

package com.bytedance.flink.component;

import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.exception.NoOutputException;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.pojo.ShellMessage;
import com.bytedance.flink.serialize.MessagePackSerializer;
import com.bytedance.flink.serialize.MessageSerializable;
import com.bytedance.flink.utils.CoreDumpUtils;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.apache.hadoop.mapreduce.util.ProcessTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Shell process.
 * */
public class ShellProcess implements Serializable {
	public static final Logger LOG = LoggerFactory.getLogger(ShellProcess.class);
	public static final String SET_SID_COMMAND = "setsid";
	public static Logger shellLog;

	private Process subprocess;
	private InputStream subprocessErrorStream;
	private String[] command;
	private Map<String, String> environment;
	private MessageSerializable serializer;
	private Number pid;
	private String taskName;
	private boolean killProcessGroup = true;
	private volatile boolean abnormal = false;

	public ShellProcess(String[] command) {
		this.command = command;
	}

	public Number launch(RuntimeConfig config) {
		LOG.info("Launch ShellProcess with RuntimeConfig: {}", config);
		killProcessGroup = (boolean) config.getOrDefault(Constants.IS_KILL_PROCESS_GROUP_KEY,
			Constants.IS_KILL_PROCESS_GROUP_VAL);
		// In order to be compatible with old configuration.
		int killProcessGroupOld = Integer.valueOf(
			config.getOrDefault(Constants.IS_KILL_PROCESS_GROUP_OLD_KEY, -1).toString());
		if (killProcessGroupOld > 0) {
			killProcessGroup = true;
		}
		environment = config.getEnvironment();

		if (killProcessGroup) {
			ArrayList<String> newCommand = new ArrayList<String>(command.length + 1);
			newCommand.add(SET_SID_COMMAND);
			Collections.addAll(newCommand, command);
			command = newCommand.toArray(new String[0]);
		}
		if (command[0].equals("bash")) {
			command[0] = "/bin/bash";
		}
		LOG.info("Executing " + Arrays.toString(command));

		ProcessBuilder builder = new ProcessBuilder(command);

		// Add resource dir and pyFlink to PYTHONPATH.
		List<String> pythonPathList = new ArrayList<>();
		pythonPathList.add(Constants.PYTHONPATH_VAL);
		String resourceDir = EnvironmentInitUtils.getResourceDir(config);
		pythonPathList.add(resourceDir);
		if (environment.containsKey(Constants.PYTHONPATH_KEY)) {
			pythonPathList.add(environment.get(Constants.PYTHONPATH_KEY));
		}
		String pythonPath = String.join(":", pythonPathList);
		environment.put(Constants.PYTHONPATH_KEY, pythonPath);
		builder.environment().putAll(environment);
		LOG.debug("Launch process with environment: {}", builder.environment());

		builder.directory(new File(resourceDir));
		builder.redirectError(ProcessBuilder.Redirect.INHERIT);
		taskName = config.getTaskName();
		shellLog = LoggerFactory.getLogger(taskName);
		serializer = new MessagePackSerializer(config.getIgnoreMismatchedMsg());

		try {
			subprocess = builder.start();
			subprocessErrorStream = subprocess.getErrorStream();
			serializer.initialize(subprocess.getOutputStream(), subprocess.getInputStream());
			this.pid = serializer.connect(config);
			LOG.info(String.format("Process launched: %s killProcessGroup: %s", pid, killProcessGroup));
		} catch (IOException e) {
			throw new RuntimeException(
				"Error when launching multilang subprocess\n"
					+ getErrorsString(), e);
		} catch (Throwable e) {
			throw new RuntimeException(getErrorsString() + "\n", e);
		}
		return this.pid;
	}

	public String getErrorsString() {
		if (subprocessErrorStream != null) {
			try {
				StringBuilder sb = new StringBuilder();
				while (subprocessErrorStream.available() > 0) {
					int bufferSize = subprocessErrorStream.available();
					byte[] errorReadingBuffer = new byte[bufferSize];
					subprocessErrorStream.read(errorReadingBuffer, 0, bufferSize);
					sb.append(new String(errorReadingBuffer));
				}
				return sb.toString();
			} catch (IOException e) {
				return "(Unable to capture error stream)";
			}
		} else {
			return "";
		}
	}

	public ShellMessage readShellMsg() throws IOException {
		try {
			return serializer.readShellMsg();
		} catch (NoOutputException e) {
			abnormal = true;
			throw new RuntimeException(e + getErrorsString() + "\n");
		}

	}

	public void writeShellMsg(ShellMessage msg) throws IOException {
		serializer.writeShellMsg(msg);
		logErrorStream();
	}

	// Log any info sent on the error stream
	public void logErrorStream() {
		String errorString = getErrorsString();
		if (errorString != null && !errorString.isEmpty()) {
			shellLog.info(errorString);
		}
	}

	public void destroy() {
		if (this.pid == null) {
			return;
		}
		String pid = String.valueOf(this.pid.longValue());
		String coreMsg = CoreDumpUtils.checkCoreDump(pid);
		if (coreMsg != null) {
			LOG.error(coreMsg);
		}
		if (killProcessGroup) {
			LOG.info("Component " + taskName + " killing process " + pid + " using processTree");
			ProcessTree.destroy(String.valueOf(pid), 1000, true, false);
		} else {
			LOG.info("Killing process " + pid);
			subprocess.destroy();
		}
	}

	/**
	 * @return exit code of the process if process is terminated, -1 if process is not started or terminated.
	 */
	public int getExitCode() {
		try {
			return subprocess != null ? subprocess.exitValue() : -1;
		} catch (IllegalThreadStateException e) {
			return -1;
		}
	}

	public String getProcessInfoString() {
		return String.format("pid:%s, name:%s", pid, taskName);
	}

	public String getProcessTerminationInfoString() {
		return String.format(" exitCode:%s, errorString:%s ", getExitCode(), getErrorsString());
	}

	public boolean isAlive() {
		return !this.abnormal && this.subprocess.isAlive();
	}
}
