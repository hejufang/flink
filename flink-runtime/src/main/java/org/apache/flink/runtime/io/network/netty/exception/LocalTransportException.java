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

package org.apache.flink.runtime.io.network.netty.exception;

import org.apache.flink.runtime.io.network.NetworkAddress;
import org.apache.flink.runtime.io.network.NetworkTraceable;

import java.net.SocketAddress;

/**
 * This class represents a Transport layer exception that may be caused by the local endpoint.
 */
public class LocalTransportException extends AbstractTransportException {

	private static final long serialVersionUID = 2366708881288640674L;

	private SocketAddress remoteAddress;

	public LocalTransportException(String message, SocketAddress address, SocketAddress remoteAddress) {
		super(message, address);
		this.remoteAddress = remoteAddress;
	}

	public LocalTransportException(String message, SocketAddress address, SocketAddress remoteAddress, Throwable cause) {
		super(message, address, cause);
		this.remoteAddress = remoteAddress;
	}

	@Override
	public NetworkAddress getRemoteAddress() {
		if (remoteAddress == null) {
			return null;
		}
		return NetworkTraceable.parseFromString(remoteAddress.toString());
	}
}
