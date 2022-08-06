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

package com.google.protobuf;

/** This class is to access internal method in protobuf package. */
public class ProtobufInternalUtils {
	/** convert underscore name to camel name. */
	public static String underScoreToCamelCase(String name, boolean capNext) {
		return toCamelCase(name, capNext);
	}

	// This is copied from com.google.protobuf.SchemaUtil#toCamelCase.
	// We copy it because it's not available in 3.7.0 yet.
	static String toCamelCase(String name, boolean capNext) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < name.length(); ++i) {
			char c = name.charAt(i);
			if ('a' <= c && c <= 'z') {
				if (capNext) {
					sb.append((char) (c + -32));
				} else {
					sb.append(c);
				}

				capNext = false;
			} else if ('A' <= c && c <= 'Z') {
				if (i == 0 && !capNext) {
					sb.append((char) (c - -32));
				} else {
					sb.append(c);
				}

				capNext = false;
			} else if ('0' <= c && c <= '9') {
				sb.append(c);
				capNext = true;
			} else {
				capNext = true;
			}
		}

		return sb.toString();
	}
}
