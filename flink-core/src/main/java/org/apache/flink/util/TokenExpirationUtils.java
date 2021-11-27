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

package org.apache.flink.util;

import javax.annotation.Nullable;

/**
 * Utils for handling token expiration exceptions.
 */
public class TokenExpirationUtils {

	public static boolean isTokenProblemInTraces(Throwable throwable){
		Throwable t = throwable;
		while (t != null) {
			if (t instanceof SerializedThrowable) {
				// SerializedThrowable will compress stringify the cause exception
				final SerializedThrowable st = (SerializedThrowable) t;
				final String message = st.getMessage();
				final String trace = st.getFullStringifiedStackTrace();
				if (isStringTokenExpired(message) || isStringTokenExpired(trace)) {
					return true;
				}
			} else {
				final String message = t.getMessage();
				if (isStringTokenExpired(message)) {
					return true;
				}
			}
			t = t.getCause();
		}
		return false;
	}

	private static boolean isStringTokenExpired(@Nullable String message) {
		if (message == null) {
			return false;
		} else if (message.contains("InfSecSException")) {
			return true;
		} else if (message.contains("token") && message.contains("expire")) {
			return true;
		} else {
			return false;
		}
	}
}
