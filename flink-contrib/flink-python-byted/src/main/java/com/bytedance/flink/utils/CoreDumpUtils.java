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

package com.bytedance.flink.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Core dump checking utils.
 */
public class CoreDumpUtils {
	private static final String CORE_DUMP_LOG_FILE = "/opt/tiger/cores/dumps.log";
	private static final Logger LOG = LoggerFactory.getLogger(CoreDumpUtils.class);
	private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	/**
	 * Check whether there is a core dump record of given process.
	 * We check the core dump log file reversely for better performance.
	 * */
	public static String checkCoreDump(String pid) {
		File file = new File(CORE_DUMP_LOG_FILE);
		if (!file.exists()) {
			LOG.info("{} not exist.", CORE_DUMP_LOG_FILE);
			return null;
		}
		String date = DF.format(new Date());
		String pattern = String.format("%s.*PID=%s[\\r|\\n|\\D]+.*", date, pid);
		LOG.info("Try to find core dump info of pid: {} in {}, where pattern is {}",
			pid, file.getAbsolutePath(), pattern);
		String coreMsg = null;
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(CORE_DUMP_LOG_FILE, "r")) {
			long len = randomAccessFile.length();
			long nextEnd = len - 1;
			char ch;
			int count = 0;
			int maxLineCount = 200;
			while (nextEnd >= 0 && count <= maxLineCount) {
				ch = (char) randomAccessFile.read();
				if (ch == '\n') {
					count++;
					String line = randomAccessFile.readLine();
					coreMsg = checkLine(line, pid, pattern);
					if (coreMsg != null) {
						return coreMsg;
					}
				}
				randomAccessFile.seek(nextEnd);
				if (nextEnd == 0) {
					count++;
					String line = randomAccessFile.readLine();
					coreMsg = checkLine(line, pid, pattern);
					if (coreMsg != null) {
						return coreMsg;
					}
				}
				nextEnd--;
			}
		} catch (FileNotFoundException e) {
			LOG.info("File not found exception while checking core dump.", e);
		} catch (IOException e) {
			LOG.info("I/O exception while checking core dump.", e);
		}
		return coreMsg;
	}

	public static String checkLine(String line, String pid, String pattern) {
		String coreMsg = null;
		if (line == null || pid == null) {
			return coreMsg;
		}
		// To mark this is the end of this line for regular expression matching.
		line += "\n";
		try {
			boolean match = line.matches(pattern);
			if (match) {
				if (line.contains("ONLY_NOTIFY")) {
					coreMsg = String.format("Core dump occurred where pid = %s, " +
						"but did not generated a core file, only notified.", pid);
				} else if (line.contains("DUMP")) {
					coreMsg = String.format("Core dump occurred where pid = %s, " +
						"and generated a core file.", pid);
				} else {
					coreMsg = String.format("Core dump occurred where pid = %s," +
						" but we do not known if there is a core file generated. " +
						"Please check file %s for more information", pid, CORE_DUMP_LOG_FILE);
				}
			}
		} catch (Exception e) {
			LOG.info("Exception while checking core log line.", e);
		}
		return coreMsg;
	}
}
