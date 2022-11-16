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

package org.apache.flink.runtime.io.network;

import java.util.Objects;

/**
 * The class to represent a network address containing host name and port.
 */
public class NetworkAddress {

	private final String hostName;
	private final int port;

	public NetworkAddress(String hostName, int port) {
		this.hostName = hostName;
		this.port = port;
	}

	public String getHostName() {
		return hostName;
	}

	public int getPort() {
		return port;
	}

	@Override
	public String toString() {
		return hostName + ":" + port;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(hostName) + Integer.hashCode(port);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof NetworkAddress)) {
			return false;
		}
		if (this == obj) {
			return true;
		}
		NetworkAddress that = (NetworkAddress) obj;
		return port == that.port && Objects.equals(hostName, that.hostName);
	}
}
