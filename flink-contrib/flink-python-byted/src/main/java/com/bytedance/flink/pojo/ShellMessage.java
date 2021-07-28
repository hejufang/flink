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

package com.bytedance.flink.pojo;

import java.io.Serializable;
import java.util.List;

/**
 * Shell message pojo which carries data between java and python.
 */
public class ShellMessage implements Serializable {
	private String command;
	private List<Object> tuple;
	private String message;

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public List<Object> getTuple() {
		return tuple;
	}

	public void setTuple(List<Object> tuple) {
		this.tuple = tuple;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "ShellMessage{" +
			"command='" + command + '\'' +
			", tuple=" + tuple +
			", message='" + message + '\'' +
			'}';
	}
}
