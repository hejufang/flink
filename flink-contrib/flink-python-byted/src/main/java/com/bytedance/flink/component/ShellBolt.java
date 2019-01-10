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

import org.apache.flink.api.java.tuple.Tuple;

import com.bytedance.flink.collector.BoltCollector;
import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.BoltInfo;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.pojo.ShellMessage;
import com.bytedance.flink.utils.CommonUtils;
import com.bytedance.flink.utils.CoreDumpUtils;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Shell bolt.
 */
public class ShellBolt implements Bolt {
	private static final Logger LOG = LoggerFactory.getLogger(ShellBolt.class);

	private BoltInfo boltInfo;
	private ShellProcess shellProcess;
	private BoltCollector boltCollector;
	private LinkedBlockingQueue<ShellMessage> pendingWrites;
	private String[] command;
	private Number subPid;
	private Integer subTaskId;
	private transient Thread readerThread;
	private transient Thread writerThread;
	private volatile boolean isRunning = true;
	private volatile boolean isSuspended = false;
	private volatile boolean isDead = false;
	private volatile Throwable exception;

	public ShellBolt(BoltInfo boltInfo) {
		this.boltInfo = boltInfo;
	}

	@Override
	public void open(RuntimeConfig runtimeConfig, BoltCollector boltCollector) {
		LOG.info("boltInfo = {}", boltInfo);
		LOG.info("runtimeConfig = {}", runtimeConfig);
		subTaskId = runtimeConfig.getSubTaskId();

		runtimeConfig.addAbsentArgs(boltInfo.getArgs());
		// Overwirte TOPOLOGY_YAML which interferes with your ability to see the configuration.
		runtimeConfig.put(Constants.TOPOLOGY_YAML, "...");
		this.boltCollector = boltCollector;
		int maxPendding = (int) boltInfo.getArgs().getOrDefault(Constants.MAX_BOLT_PENDING,
			Constants.MAX_BOLT_PENDING_DEFAULT);
		pendingWrites = new LinkedBlockingQueue<>(maxPendding);
		Map<String, Object> shellArgs = new HashMap<>();
		String logFile = EnvironmentInitUtils.getLogFile();
		if (logFile != null) {
			shellArgs.put(Constants.LOG_FILE, logFile);
			runtimeConfig.put(Constants.LOG_FILE, logFile);
		}
		shellArgs.putAll(boltInfo.getArgs());
		runtimeConfig.put(Constants.SUB_TASK_ID, subTaskId);
		boolean dontWriteBytecode =
			(boolean) boltInfo.getArgs().getOrDefault(Constants.DONT_WRITE_BYTECODE, true);
		command = EnvironmentInitUtils.buildShellCommand(boltInfo.getInterpreter(),
			boltInfo.getScript(), shellArgs, dontWriteBytecode);
		shellProcess = new ShellProcess(command);
		subPid = shellProcess.launch(runtimeConfig);
		LOG.info("Launched subprocess with pid " + subPid);
		startReaderAndWriter();
	}

	public void execute(Tuple tuple) {
		List<Object> value = CommonUtils.tupleToList(tuple);
		execute(value);
	}

	@Override
	public void execute(List<Object> value) {
		if (!isRunning || isSuspended) {
			throw new RuntimeException("Invalid bolt state, running:" + isRunning + ", sudpend:" + isSuspended);
		}

		ShellMessage msg = new ShellMessage();
		msg.setTuple(value);
		while (isRunning && !isSuspended) {
			if (exception != null) {
				throw new RuntimeException(exception);
			}
			try {
				if (pendingWrites.offer(msg, 1, TimeUnit.SECONDS)) {
					return;
				}
			} catch (InterruptedException e) {
				String processInfo = shellProcess.getProcessInfoString()
					+ shellProcess.getProcessTerminationInfoString();
				throw new RuntimeException("Error during multilang processing " + processInfo, e);
			}
		}
	}

	@Override
	public void close() {
		isRunning = false;
		if (writerThread != null) {
			writerThread.interrupt();
		}
		if (readerThread != null) {
			readerThread.interrupt();
		}
		if (shellProcess != null) {
			shellProcess.destroy();
		}
	}

	@Override
	public BoltInfo getBoltInfo() {
		return boltInfo;
	}

	public void startReaderAndWriter() {
		// reader
		readerThread = new Thread(new BoltReaderRunnable());
		readerThread.start();

		// writer
		writerThread = new Thread(new BoltWriterRunnable());
		writerThread.start();
	}

	private void handleEmit(ShellMessage shellMsg) throws InterruptedException {
		boltCollector.emit(shellMsg.getTuple());
	}

	private void handleLog(ShellMessage shellMsg) throws IOException {
		String log = shellMsg.getMessage();
		LOG.info("Get log from python: {}", log);
	}

	private void handleError(ShellMessage shellMsg) throws IOException {
		String log = shellMsg.getMessage();
		LOG.error("Get Error log from python: {}", log);
	}

	private void die(Throwable exception) {
		this.isDead = true;
		String processInfo = shellProcess.getProcessInfoString() + shellProcess.getProcessTerminationInfoString();
		this.exception = new RuntimeException(processInfo, exception);
		String message = String.format("Halting process: ShellBolt died. Command: %s, ProcessInfo %s",
			Arrays.toString(command), processInfo);
		String pid = String.valueOf(subPid);
		String coreMsg = CoreDumpUtils.checkCoreDump(pid);
		if (coreMsg != null) {
			message = coreMsg + " " + message;
		}
		LOG.error(message, exception);
	}

	public void suspend() {
		this.isSuspended = true;
		LOG.info("suspend bolt, pid:{}", subPid);
	}

	public void attach(BoltCollector boltCollector) throws Exception {
		LOG.info("Try to attach bolt pid:{}", subPid);
		if (shellProcess.isAlive() && !this.isDead) {
			// If process still alive, update _collector (used to emit element to flink frame)
			this.boltCollector = boltCollector;
			this.isSuspended = false;
			LOG.info("attach bolt, pid:{}", subPid);
		} else {
			throw new Exception("subprogress died");
		}
	}

	@Override
	public String toString() {
		return "ShellBolt{" +
			"subPid=" + subPid +
			", isRunning=" + isRunning +
			", isSuspended=" + isSuspended +
			", isDead=" + isDead +
			'}';
	}

	private class BoltReaderRunnable implements Runnable {
		public void run() {
			while (isRunning) {
				try {
					ShellMessage shellMsg = shellProcess.readShellMsg();
					String command = shellMsg.getCommand();
					if (command == null) {
						throw new IllegalArgumentException("Command not found in bolt message: " + shellMsg);
					} else if (Constants.EMIT.equals(command)) {
						handleEmit(shellMsg);
					} else if (Constants.LOG.equals(command)) {
						handleLog(shellMsg);
					} else if (Constants.ERROR.equals(command)) {
						handleError(shellMsg);
					} else {
						LOG.error("Unknown command {}", command);
					}
				} catch (IOException | IllegalArgumentException e) {
					LOG.error("Error occurred when get msg from python {}", subPid, e);
					die(e);
					return;
				} catch (Throwable t) {
					// Exception from flink-frame, ignore it and suspend, then waiting to be attached or exit
					LOG.error(shellProcess.getProcessInfoString(), t);
					isSuspended = true;
					while (isRunning && isSuspended) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							if (isRunning) {
								die(e);
							}
							return;
						}
					}
				}
			}
		}
	}

	private class BoltWriterRunnable implements Runnable {
		public void run() {
			while (isRunning) {
				try {
					Object write = pendingWrites.poll(1, SECONDS);
					if (write != null) {
						shellProcess.writeShellMsg((ShellMessage) write);
					}
				} catch (InterruptedException e) {
					LOG.warn("InterruptedException occurred", e);
				} catch (Throwable t) {
					LOG.error("Error occurred when write msg to python {}", subPid, t);
					die(t);
					return;
				}
			}
		}
	}
}
