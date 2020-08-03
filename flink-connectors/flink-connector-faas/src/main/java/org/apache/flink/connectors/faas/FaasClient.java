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

package org.apache.flink.connectors.faas;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connectors.faas.utils.FaasUtils;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.httpclient.org.apache.http.HttpEntity;
import org.apache.flink.shaded.httpclient.org.apache.http.HttpResponse;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.HttpGet;
import org.apache.flink.shaded.httpclient.org.apache.http.concurrent.FutureCallback;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.flink.shaded.httpclient.org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * faas client.
 */
public class FaasClient {
	private static final Logger LOG = LoggerFactory.getLogger(FaasClient.class);

	private final String[] fieldNames;
	private final TypeInformation<?>[] fieldTypes;
	private final FaasLookupOptions faasLookupOptions;
	private final String[] lookupKeys;

	private CloseableHttpAsyncClient asyncHttpClient;
	private String urlTemplate;
	private JsonRowDeserializationSchema deserializationSchema;

	public FaasClient(
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes,
			FaasLookupOptions faasLookupOptions,
			String[] lookupKeys) {
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.faasLookupOptions = faasLookupOptions;
		this.lookupKeys = lookupKeys;
	}

	public void open() {
		// init asyncHttpClient
		IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
			.setConnectTimeout(faasLookupOptions.getConnectionTimeoutMs())
			.build();
		asyncHttpClient = HttpAsyncClients.custom()
			.setMaxConnTotal(faasLookupOptions.getMaxConnections())
			.setMaxConnPerRoute(faasLookupOptions.getMaxConnections())
			.setDefaultIOReactorConfig(ioReactorConfig)
			.build();
		asyncHttpClient.start();
		// init url template
		String url = faasLookupOptions.getUrl().trim();
		if (!url.endsWith("?")) {
			url += "?";
		}
		List<String> params = new ArrayList<>();
		for (String lookupKey : lookupKeys) {
			params.add(lookupKey + "=%s");
		}
		urlTemplate = url + String.join("&", params);
		// init deserializationSchema
		TypeInformation<Row> typeInfo = new RowTypeInfo(fieldTypes, fieldNames);
		deserializationSchema = new JsonRowDeserializationSchema.Builder(typeInfo).build();
	}

	public String buildRequestUrl(Object... keys) {
		return String.format(urlTemplate, keys);
	}

	/**
	 * There are three different situations of this function:
	 * 1. fail: then we call 'result.completeExceptionally to collect Exception'.
	 * 2. retry: then we call 'retryAsyncRequest' to do retry.
	 * 3. success: then we call 'result.complete' to collect the result.
	 */
	public void doAsyncRequest(
			String requestUrl,
			CompletableFuture<Collection<Row>> result,
			Cache<String, List<Row>> cache,
			int retry) {
		HttpGet httpGet = new HttpGet(requestUrl);
		asyncHttpClient.execute(httpGet, new FutureCallback<HttpResponse>() {
			@Override
			public void completed(HttpResponse response) {
				// if EntityUtils.toString failed, we can't get responseBody, so set a default one
				String responseBody = "Illegal response body";
				try {
					int statusCode = response.getStatusLine().getStatusCode();
					if (statusCode != 200) {
						result.completeExceptionally(new FlinkRuntimeException(String.format("Failed to parse "
							+ "faas response '%s', status code of faas response must be 200", response.toString())));
						return;
					}
					HttpEntity entity = response.getEntity();
					responseBody = EntityUtils.toString(entity);
					List<Row> responseList = FaasUtils.parseJSONString(responseBody, deserializationSchema);
					if (responseList == null) {
						result.completeExceptionally(new FlinkRuntimeException(String.format(
							"response body '%s' is not a json array, please check it", responseBody)));
						return;
					}
					if (cache != null) {
						cache.put(requestUrl, responseList);
					}
					result.complete(responseList);
				} catch (IOException e) {
					if (retry == faasLookupOptions.getMaxRetryTimes()) {
						result.completeExceptionally(new FlinkRuntimeException(String.format(
							"Failed to parse faas response '%s', "
								+ "please check the format of faas response body", responseBody)));
					} else {
						retryAsyncRequest(requestUrl, result, cache, retry);
					}
				}
			}

			@Override
			public void failed(Exception e) {
				if (retry == faasLookupOptions.getMaxRetryTimes()) {
					result.completeExceptionally(new FlinkRuntimeException("faas async request failed", e));
				} else {
					retryAsyncRequest(requestUrl, result, cache, retry);
				}
			}

			@Override
			public void cancelled() {
				result.completeExceptionally(new FlinkRuntimeException("faas async request is cancelled"));
			}
		});
	}

	public void close() throws IOException {
		if (asyncHttpClient != null) {
			asyncHttpClient.close();
		}
	}

	private void retryAsyncRequest(
			String requestUrl,
			CompletableFuture<Collection<Row>> result,
			Cache<String, List<Row>> cache,
			int retry) {
		LOG.warn(String.format("Faas execute read error, retry times = %d", retry));
		try {
			Thread.sleep(1000 * retry);
		} catch (InterruptedException e1) {
			result.completeExceptionally(new FlinkRuntimeException(e1));
			return;
		}
		doAsyncRequest(requestUrl, result, cache, ++retry);
	}
}
