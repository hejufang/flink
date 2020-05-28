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

package org.apache.flink.streaming.connectors.elasticsearch7.util;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.byted.org.byted.infsec.client.InfSecException;
import org.apache.flink.shaded.byted.org.byted.infsec.client.SecTokenC;

import com.bytedance.search.consul.Discovery;
import com.bytedance.search.consul.EndPoint;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Ported from code.byted.org:aladdin/es_flink.
 */
public class ESHttpRoutePlanner implements HttpRoutePlanner {

	private static final Discovery CONSUL = new Discovery();

	private final boolean enableGdpr;

	public ESHttpRoutePlanner(boolean enableGdpr) {
		this.enableGdpr = enableGdpr;
	}

	@Override
	public HttpRoute determineRoute(HttpHost host, HttpRequest request, HttpContext context) throws HttpException {
		Objects.requireNonNull(request, "Request cannot be null");
		Objects.requireNonNull(host, "host cannot be null");

		HttpClientContext clientContext = HttpClientContext.adapt(context);
		RequestConfig config = clientContext.getRequestConfig();
		InetAddress local = config.getLocalAddress();
		HttpHost proxy = config.getProxy();

		HttpHost target = host;
		if (host.getSchemeName().equals("psm")) {
			String hostName = host.getHostName();

			String authInfo = null;
			int userInfoIdx = hostName.indexOf('@');
			if (userInfoIdx >= 0) {
				authInfo = hostName.substring(0, userInfoIdx);
				hostName = hostName.substring(userInfoIdx + 1);
			}

			if (authInfo != null) {
				authInfo = Base64.encodeBase64String(authInfo.getBytes(StandardCharsets.US_ASCII));
				request.addHeader(new BasicHeader("Authorization", "Basic ".concat(authInfo)));
			}

			if (enableGdpr) {
				String gdprToken = getGDPRToken();
				request.addHeader(new BasicHeader("Gdpr-Token", gdprToken));
			}

			String psm = hostName, cluster = "default";
			int clusterIdx = hostName.indexOf('$');
			if (clusterIdx > 0) {
				psm = hostName.substring(0, clusterIdx);
				cluster = hostName.substring(clusterIdx + 1);
			}
			EndPoint endPoint = CONSUL.lookup(psm).FilterCluster(cluster).GetOne();
			if (endPoint == null) {
				throw new HttpException("No available nodes");
			}
			target = new HttpHost(endPoint.getHost(), endPoint.getPort(), "http");
		}

		boolean secure = target.getSchemeName().equalsIgnoreCase("https");
		return proxy == null ? new HttpRoute(target, local, secure) : new HttpRoute(target, local, proxy, secure);
	}

	private String getGDPRToken() {
		try {
			String token = SecTokenC.getToken(false);
			if (token != null && !token.isEmpty()) {
				return token;
			} else {
				throw new FlinkRuntimeException("Enabled ByteES GDPR, however getting blank token from the container.");
			}
		} catch (InfSecException e) {
			throw new FlinkRuntimeException("Enabled ByteES GDPR, however exception occurred while getting token " +
				"from the container.", e);
		}
	}
}
