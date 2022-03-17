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

import org.byted.security.ztijwthelper.LegacyIdentity;
import org.byted.security.ztijwthelper.ZTIJwtHelper;
import org.byted.security.ztijwthelper.ZtiJwtException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils for handling security.*/
public class SecUtil {
	private static final Logger LOG = LoggerFactory.getLogger(SecUtil.class);

	public static LegacyIdentity getIdentityFromToken() {
		try {
			String token = getGDPROrJWTToken();
			// decode the claims in the token string to get formatted zero trust identity
			LegacyIdentity zti = ZTIJwtHelper.decodeGDPRorJwtSVID(token);
			return zti;
		} catch (ZtiJwtException e) {
			throw new FlinkRuntimeException("Failed to parse the gdpr token!", e);
		}
	}

	public static String getGDPROrJWTToken() {
		try {
			// fetch a JWT-SVID token string
			String tokenString = ZTIJwtHelper.getJwtSVID();
			return tokenString;
		} catch (ZtiJwtException e) {
			LOG.warn("Failed to get the GDPR/JWT-SVID token!", e);
		}
		return null;
	}
}
