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

package org.apache.flink.runtime.socket;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Objects;

/**
 * Socket && Rest address information.
 */
public class SocketRestLeaderAddress {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final String baseRestUrl;
	private final String socketAddress;
	private final int socketPort;

	public SocketRestLeaderAddress() {
		this(null, null, 0);
	}

	public SocketRestLeaderAddress(String baseRestUrl, String socketAddress, int socketPort) {
		this.baseRestUrl = baseRestUrl;
		this.socketAddress = socketAddress;
		this.socketPort = socketPort;
	}

	public String getBaseRestUrl() {
		return baseRestUrl;
	}

	public String getSocketAddress() {
		return socketAddress;
	}

	public int getSocketPort() {
		return socketPort;
	}

	public String toJson() throws IOException {
		return OBJECT_MAPPER.writeValueAsString(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SocketRestLeaderAddress that = (SocketRestLeaderAddress) o;
		return socketPort == that.socketPort && Objects.equals(baseRestUrl, that.baseRestUrl) && Objects.equals(socketAddress, that.socketAddress);
	}

	@Override
	public int hashCode() {
		return Objects.hash(baseRestUrl, socketAddress, socketPort);
	}

	public static SocketRestLeaderAddress fromJson(String json) throws IOException {
		return OBJECT_MAPPER.readValue(json, SocketRestLeaderAddress.class);
	}
}
