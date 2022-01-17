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

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.byted.org.byted.infsec.client.Identity;
import org.apache.flink.shaded.byted.org.byted.infsec.client.InfSecException;
import org.apache.flink.shaded.byted.org.byted.infsec.client.SecTokenC;

/** Utils for handling security.*/
public class SecUtil {
	public static Identity getIdentityFromToken() {
		try {
			String token = getGDPRToken(false);
			return SecTokenC.parseToken(token);
		} catch (InfSecException e) {
			throw new FlinkRuntimeException("Failed to parse the gdpr token!", e);
		}
	}

	public static String getGDPRToken(boolean forceUpdate) {
		try {
			return SecTokenC.getToken(forceUpdate);
		} catch (InfSecException e) {
			throw new FlinkRuntimeException("Failed to get the gdpr token!", e);
		}
	}
}
