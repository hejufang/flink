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

package org.apache.flink.connector.rpc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Copy from java_arch / base-group repo.
 */
public final class RequestIDUtil {

	private static final Logger LOG = LoggerFactory.getLogger(RequestIDUtil.class);
	private static final String PATTERN_FORMAT = "yyyyMMddHHmmss%sSSS", DEFAULT_IP = "000000000000";
	private static final String LOCAL_IP = getLocalIP();
	private static final String REAL_PATTERN = String.format(PATTERN_FORMAT, LOCAL_IP);
	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(REAL_PATTERN);
	private static final int RANDOM_MAX = 0x1000;
	private static final String HEX_STRING_FORMATTER = "%03X";

	public static String generateRequestID() {
		return LocalDateTime.now().format(FORMATTER) + getRandomHexString();
	}

	static String getRandomHexString() {
		return String.format(HEX_STRING_FORMATTER, ThreadLocalRandom.current().nextInt(RANDOM_MAX)).toUpperCase();
	}

	private static <T> Stream<T> enumerationAsStream(Enumeration<T> e) {
		return StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.ORDERED) {
			public boolean tryAdvance(Consumer<? super T> action) {
				if (e.hasMoreElements()) {
					action.accept(e.nextElement());
					return true;
				}
				return false;
			}

			public void forEachRemaining(Consumer<? super T> action) {
				while (e.hasMoreElements()) {
					action.accept(e.nextElement());
				}
			}
		}, false);
	}

	static String getLocalIP() {
		try {
			List<NetworkInterface> interfaces = enumerationAsStream(NetworkInterface.getNetworkInterfaces())
				.collect(Collectors.toList());
			Collections.reverse(interfaces);
			for (NetworkInterface inf : interfaces) {
				for (InterfaceAddress interfaceAddress : inf.getInterfaceAddresses()) {
					InetAddress addr = interfaceAddress.getAddress();
					if (!addr.isLoopbackAddress()) {
						byte[] bytes = addr.getAddress();
						if (null != bytes && bytes.length == 4) {
							return formatIPv4Bytes(bytes);
						}
					}
				}
			}
		} catch (SocketException e) {
			LOG.warn("get local ip failure.", e);
		}
		return DEFAULT_IP;
	}

	static String formatIPv4Bytes(byte[] bytes)  {
		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < 4; ++i) {
			buf.append(Byte.toUnsignedInt(bytes[i]));
			for (int j = 0, z = 3 * (i + 1) - buf.length(); j < z; ++j) {
				buf.insert(3 * i, '0');
			}
		}
		return buf.toString();
	}

	private RequestIDUtil() {
		String fmt = "%s is not allowed to be constructed.";
		throw new UnsupportedOperationException(String.format(fmt, this.getClass().getName()));
	}
}

