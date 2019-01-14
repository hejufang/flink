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

import com.bytedance.flink.collector.SpoutCollector;
import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.pojo.ShellMessage;
import com.bytedance.flink.pojo.SpoutInfo;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Shell spout.
 */
public class ShellSpout implements Spout {
	private static final Logger LOG = LoggerFactory.getLogger(ShellProcess.class);

	private SpoutInfo spoutInfo;
	private ShellProcess shellProcess;
	private ShellMessage requestMsg;
	private SpoutCollector spoutCollector;
	private Number subPid;
	private Integer subTaskId;
	private volatile boolean isRunning = true;
	private volatile boolean isSuspended = false;
	private volatile boolean isDead = false;
	private transient Thread spoutReader;

	/**
	 * Pending messages that has not been emit.
	 */
	private volatile LinkedBlockingQueue<ShellMessage> pendingMsgs;
	/**
	 * Error occurred when communicating with shellspout process.
	 */
	private volatile Throwable exception;

	public ShellSpout(SpoutInfo spoutInfo) {
		this.spoutInfo = spoutInfo;
		requestMsg = new ShellMessage();
		requestMsg.setCommand(Constants.NEXT);
		requestMsg.setTuple(new ArrayList<>());
	}

	@Override
	public void open(RuntimeConfig runtimeConfig, SpoutCollector spoutCollector) {
		LOG.info("spoutInfo = {}", spoutInfo);
		LOG.info("runtimeConfig = {}", runtimeConfig);
		runtimeConfig.addAbsentArgs(spoutInfo.getArgs());
		runtimeConfig.put(Constants.INDEX, runtimeConfig.getSubTaskId());
		// Overwirte yaml which interferes with your ability to see the configuration.
		runtimeConfig.put(Constants.TOPOLOGY_YAML, "...");
		spoutReader = new Thread(new SpoutReaderRunnable());
		subTaskId = runtimeConfig.getSubTaskId();
		int maxPendding = (int) spoutInfo.getArgs().getOrDefault(Constants.MAX_SPOUT_PENDING_KEY,
				Constants.MAX_SPOUT_PENDING_VAL);
		pendingMsgs = new LinkedBlockingQueue<>(maxPendding);
		this.spoutCollector = spoutCollector;
		fillPartitionInfo(runtimeConfig, spoutInfo.getArgs());
		LOG.info("subTaskId = {}", subTaskId);
		Map<String, Object> thisArgs = new HashMap<>();
		thisArgs.putAll(spoutInfo.getArgs());
		thisArgs.put(Constants.SUB_TASK_ID, subTaskId);
		String logFile = EnvironmentInitUtils.getLogFile();
		if (logFile != null) {
			thisArgs.put(Constants.LOG_FILE, logFile);
			runtimeConfig.put(Constants.LOG_FILE, logFile);
		}
		boolean dontWriteBytecode =
			(boolean) spoutInfo.getArgs().getOrDefault(Constants.DONT_WRITE_BYTECODE, true);
		thisArgs.put(Constants.LOCAL_FAILOVER,
			runtimeConfig.getOrDefault(Constants.LOCAL_FAILOVER, false));
		String[] cmd = EnvironmentInitUtils.buildShellCommand(spoutInfo.getInterpreter(),
			spoutInfo.getScript(), thisArgs, dontWriteBytecode);
		shellProcess = new ShellProcess(cmd);
		subPid = shellProcess.launch(runtimeConfig);
		LOG.info("Launched subprocess with pid " + subPid);
		spoutReader.start();

	}

	@Override
	public void nextTuple() {
		if (exception != null) {
			throw new RuntimeException(exception);
		}

		if (isSuspended || !isRunning) {
			return;
		}

		try {
			ShellMessage shellMsg = pendingMsgs.poll(1, TimeUnit.SECONDS);
			if (shellMsg != null) {
				handleEmit(shellMsg);
			}
		} catch (InterruptedException e) {
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		isRunning = false;
		if (spoutReader != null) {
			spoutReader.interrupt();
		}
		if (shellProcess != null) {
			shellProcess.destroy();
		}
	}

	public SpoutInfo getSpoutInfo() {
		return spoutInfo;
	}

	private void handleEmit(ShellMessage shellMsg) throws IOException {
		List<Object> tuple = shellMsg.getTuple();
		spoutCollector.emit(tuple);
	}

	private void handleLog(ShellMessage shellMsg) throws IOException {
		String log = shellMsg.getMessage();
		LOG.info("Get log from python: {}", log);
	}

	private void handleError(ShellMessage shellMsg) throws IOException {
		String log = shellMsg.getMessage();
		LOG.error("Get Error log from python: {}", log);
	}

	public void fillPartitionInfo(RuntimeConfig runtimeConfig, Map<String, Object> conf) {
		int modNum = spoutInfo.getTotalPartition() % spoutInfo.getParallelism();
		int partPerThread = spoutInfo.getTotalPartition() / spoutInfo.getParallelism();
		int newIndex = -1;
		int consumedPartitionNum = 1;
		// if spout is multi_kafka_spout, use taskIndex as spout index
		if (spoutInfo.getScript().contains(Constants.MULTI_KAFKA_SPOUT_RUNNER_FLAG)) {
			newIndex = subTaskId;
		} else if (!spoutInfo.isThreadNumDeterminedByPartition()) {
			newIndex = calcStartIndex(subTaskId, partPerThread, modNum);
			consumedPartitionNum = calcPartitionNum(subTaskId, partPerThread, modNum);
		} else if (spoutInfo.isPartitionRangeConfigured()) {
			// if kafka partition range has been configured
			List<Integer> partitionList = spoutInfo.getPartitionList();
			newIndex = partitionList.get(subTaskId);
		} else {
			newIndex = subTaskId;
		}

		LOG.info("Adjust index to {}", newIndex);
		conf.put(Constants.INDEX, newIndex);
		runtimeConfig.put(Constants.INDEX, newIndex);

		LOG.info("Set consumedPartitionNum = {}", consumedPartitionNum);
		conf.put(Constants.CONSUMED_PARTITION, consumedPartitionNum);
		runtimeConfig.put(Constants.CONSUMED_PARTITION, consumedPartitionNum);
	}

	private int calcStartIndex(int idx, int perThread, int modNum) {
		int offset = Math.min(idx, modNum);
		return idx * perThread + offset;
	}

	private int calcPartitionNum(int idx, int perThread, int modNum) {
		return idx < modNum ? perThread + 1 : perThread;
	}

	public void attach(SpoutCollector spoutCollector) throws Exception {
		if (shellProcess.isAlive() && !isDead) {
			this.spoutCollector = spoutCollector;
			this.isSuspended = false;
			LOG.info("attach spout, pid:{}", subPid);
		} else {
			throw new Exception("subprogress died");
		}
	}

	private void die(Throwable exception) {
		LOG.info("Spout {}-{} is going to die", spoutInfo.getName(), subTaskId);
		isRunning = false;
		isDead = true;
		String processInfo = shellProcess.getProcessInfoString()
			+ shellProcess.getProcessTerminationInfoString();
		this.exception = new RuntimeException(processInfo, exception);
		String message = "Halting process: ShellSpout died.";
		LOG.error(message, exception);
		shellProcess.destroy();
	}

	public void suspend() {
		this.isSuspended = true;
		LOG.info("suspend spout, pid:{}", subPid);
	}

	/**
	 * Read msg from shellspout progress asyncly.
	 */
	private class SpoutReaderRunnable implements Runnable {
		@Override
		public void run() {
			while (isRunning) {
				try {
					shellProcess.writeShellMsg(requestMsg);
					while (isRunning) {
						ShellMessage shellMsg = shellProcess.readShellMsg();
						String command = shellMsg.getCommand();
						if (command == null) {
							throw new IllegalArgumentException("Command not found in spout message: " + command);
						}

						if (Constants.SYNC.equals(command)) {
							break;
						} else if (Constants.EMIT.contains(command)) {
							pendingMsgs.put(shellMsg);
						} else if (Constants.LOG.equals(command)) {
							handleLog(shellMsg);
						} else if (Constants.ERROR.equals(command)) {
							handleError(shellMsg);
						} else {
							throw new RuntimeException("Unknown command received: " + command);
						}
					}
				} catch (InterruptedException e) {
					LOG.info("SpoutReaderRunnable Interrupted");
					isRunning = false;
					return;
				} catch (Throwable e) {
					LOG.error("Error occurred when read meg from python {}", subPid, e);
					die(e);
					return;
				}
			}
		}
	}
}
