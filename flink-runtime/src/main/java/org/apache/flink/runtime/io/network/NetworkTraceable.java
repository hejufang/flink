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

import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

/**
 * This interface defines some methods that can help trace the network problem, such as the source and destination of a connection.
 */
public interface NetworkTraceable {

	@Nullable
	NetworkAddress getRemoteAddress();

	@Nullable
	static NetworkAddress parseFromString(String socketAddress) {
		if (StringUtils.isNullOrWhitespaceOnly(socketAddress)) {
			return null;
		}
		String hostName = extractHostNameOrIP(socketAddress);
		int port = extractPort(socketAddress);
		return new NetworkAddress(hostName, port);
	}

	static String extractHostNameOrIP(String socketAddress) {
		if (socketAddress.startsWith("/")) {
			// for form like /fdbd:dc03:ff:100:53b3:6d47:8366:32e8:14593, will return fdbd:dc03:ff:100:53b3:6d47:8366:32e8,
			// this will be happened when NS can't find the host name of this ip.
			int portPos = socketAddress.lastIndexOf(':');
			if (portPos != -1) {
				return socketAddress.substring(1, portPos);
			}
		} else {
			// for form like test-host/fdbd:dc03:ff:100:53b3:6d47:8366:32e8:14593, will return test-host
			int pos = socketAddress.indexOf('/');
			if (pos != -1) {
				return socketAddress.substring(0, pos);
			}
			// for form like test-host:14593, will return test-host,
			// this will be happened when NS can't find the ip of with this host name (usually in tests).
			int portPos = socketAddress.lastIndexOf(':');
			if (portPos != -1) {
				return socketAddress.substring(0, portPos);
			}
		}
		// should not be reached, means parsing failed
		return socketAddress;
	}

	static int extractPort(String socketAddress) {
		int portPos = socketAddress.lastIndexOf(':');
		if (portPos != -1) {
			String portString = socketAddress.substring(portPos + 1);
			return Integer.parseInt(portString);
		}
		return -1;
	}

}
