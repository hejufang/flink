/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;

/**
 * LoggerHelper.
 */
public class LoggerHelper {

	private static final String LOG_SEC_MARK_ENABLE_KEY = "log.sec.mark.enabled";

	private static boolean secMarksEnabled = Boolean.parseBoolean(System.getProperty(LOG_SEC_MARK_ENABLE_KEY));

	private LoggerHelper() {
	}

	public static void setSecMarksEnabled(boolean enabled) {
		secMarksEnabled = enabled;
	}

	public static boolean getSecMarksEnabled() {
		return secMarksEnabled;
	}

	public static String secMark(Object key, Object value) {
		return secMarksEnabled ? "{{" + key + "=" + value + "}}" : formatMessage(value);
	}

	private static String formatMessage(Object value) {
		return value instanceof Throwable ? Throwables.getStackTraceAsString((Throwable) value) : value.toString();
	}
}