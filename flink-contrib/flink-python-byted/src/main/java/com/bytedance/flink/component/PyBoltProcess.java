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

import org.apache.flink.runtime.pyflink.PYFlinkProgress;

import com.bytedance.flink.pojo.RuntimeConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * PYFlink spout process.
 */
public class PyBoltProcess implements PYFlinkProgress {
	private static final Logger LOG = LoggerFactory.getLogger(PyBoltProcess.class);

	private final RuntimeConfig runtimeConfig;
	private final String name;
	private final int taskId;
	private final Bolt bolt;
	private volatile boolean inuse;
	private volatile long idleStartTime;

	public PyBoltProcess(RuntimeConfig runtimeConfig, String name, int taskId, Bolt bolt) {
		this.runtimeConfig = runtimeConfig;
		this.name = name;
		this.taskId = taskId;
		this.bolt = bolt;
		this.inuse = true;
	}

	public RuntimeConfig getRuntimeConfig() {
		return runtimeConfig;
	}

	public String getName() {
		return name;
	}

	public int getTaskId() {
		return taskId;
	}

	public Bolt getBolt() {
		return bolt;
	}

	@Override
	public void clear() {
		bolt.close();
		String pidDir = runtimeConfig.getPidDir();
		try {
			FileUtils.deleteDirectory(new File(pidDir));
		} catch (IOException e) {
			LOG.error("Error when clear path.", e);
		}
	}

	@Override
	public boolean isInUse() {
		return this.inuse;
	}

	@Override
	public long getIdleDuration() {
		if (inuse) {
			return 0;
		}

		return System.currentTimeMillis() - this.idleStartTime;
	}

	public void markInUse() {
		this.inuse = true;
		this.idleStartTime = -1;
	}

	public void markUnUse() {
		this.inuse = false;
		this.idleStartTime = System.currentTimeMillis();
	}

	@Override
	public String toString() {
		return "PYFlinkBoltProgress{" +
			"name='" + name + '\'' +
			", taskId=" + taskId +
			", bolt=" + bolt +
			", inuse=" + inuse +
			'}';
	}
}
