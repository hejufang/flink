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

package com.bytedance.flink.serialize;

import com.bytedance.flink.exception.NoOutputException;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.pojo.ShellMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface for message serializer.
 */
public interface MessageSerializable {

	/**
	 * This method sets the input and output streams of the serializer.
	 *
	 * @param processIn  output stream to non-JVM component
	 * @param processOut input stream from non-JVM component
	 */
	void initialize(OutputStream processIn, InputStream processOut);

	/**
	 * This method transmits the config to the non-JVM process and receives its pid.
	 *
	 * @param runtimeConfig time configuration
	 * @return process pid
	 */
	Number connect(RuntimeConfig runtimeConfig) throws IOException;

	/**
	 * This method receives a shell message from the non-JVM process.
	 *
	 * @return shell message
	 */
	ShellMessage readShellMsg() throws IOException, NoOutputException;

	/**
	 * This method sends a message to a non-JVM bolt process.
	 *
	 * @param msg bolt message
	 */
	void writeShellMsg(ShellMessage msg) throws IOException;
}
