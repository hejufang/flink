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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Disk utils.
 */
public class DiskUtils {
	private static final Logger LOG = LoggerFactory.getLogger(DiskUtils.class);

	public static final String REMOTE_SSD_PREFIX = "/opt/iscsi/yarn";

	private static final String DISK_PREFIX = "/data";

	private static final String[] COMMAND_OUTPUT = new String[] {"NAME", "ROTA", "SIZE", "MOUNTPOINT"};
	private static final String[] COMMAND = new String[] {"lsblk", "-P", "-o", String.join(",", COMMAND_OUTPUT)};

	private static final List<DiskInfo> diskInfos = new ArrayList<>();

	static {
		try {
			final Process p = Runtime.getRuntime().exec(COMMAND);

			try (InputStream is = p.getInputStream()) {
				final BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String line;
				while ((line = br.readLine()) != null) {
					final Optional<DiskInfo> diskInfo = parseLine(line);
					diskInfo.ifPresent(diskInfos::add);
				}
			}
		} catch (IOException e) {
			LOG.warn("Fail to execute command {}.", String.join(" ", COMMAND));
		}
	}

	public static List<String> ssdMounts() {
		return diskInfos.stream()
			.filter(x -> x.rota.equals("0"))
			.map(x -> parseDataDirectory(x.mountpoint))
			.collect(Collectors.toList());
	}

	@VisibleForTesting
	public static String parseDataDirectory(String mountpoint) {
		if (mountpoint.startsWith(DISK_PREFIX)) {
			String[] elements = mountpoint.split("/");
			return "/" + elements[1];
		} else {
			return mountpoint;
		}
	}

	@VisibleForTesting
	public static Optional<DiskInfo> parseLine(String line) {
		final String[] elements = line.split(" ");
		if (elements.length != COMMAND_OUTPUT.length) {
			LOG.warn("Unable to parse disk info string: {}", line);
		} else {
			final Optional<DiskInfo> diskInfoOpt = parseDiskInfo(elements);
			return diskInfoOpt.map(diskInfo -> {
				if (!diskInfo.mountpoint.isEmpty() && !diskInfo.mountpoint.equals("/")) {
					return diskInfo;
				} else {
					return null;
				}
			});
		}
		return Optional.empty();
	}

	private static Optional<DiskInfo> parseDiskInfo(String[] elements) {
		final List<String> infos = Arrays.stream(elements)
			.map(DiskUtils::parseElementValue)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
		if (infos.size() == elements.length) {
			return Optional.of(new DiskInfo(infos.get(0), infos.get(1), infos.get(2), infos.get(3)));
		}
		return Optional.empty();
	}

	private static String parseElementValue(String element) {
		final String[] pair = element.split("=");
		if (pair.length == 2) {
			final String value = pair[1];
			return value.replaceAll("\"", "");
		}
		LOG.warn("Fail to parse element {}.", element);
		return null;
	}

	/**
	 * DiskInfo.
	 */
	public static class DiskInfo {
		public final String name;
		public final String rota;
		public final String size;
		public final String mountpoint;

		DiskInfo(String name, String rota, String size, String mountpoint) {
			this.name = name;
			this.rota = rota;
			this.size = size;
			this.mountpoint = mountpoint;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof DiskInfo) {
				final DiskInfo that = (DiskInfo) obj;
				return name.equals(that.name)
					&& rota.equals(that.rota)
					&& size.equals(that.size)
					&& mountpoint.equals(that.mountpoint);
			} else {
				return false;
			}
		}
	}
}
