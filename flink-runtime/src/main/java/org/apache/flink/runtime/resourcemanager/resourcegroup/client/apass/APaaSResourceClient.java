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

package org.apache.flink.runtime.resourcemanager.resourcegroup.client.apass;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfo;
import org.apache.flink.runtime.resourcemanager.resourcegroup.ResourceInfoAwareListener;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClient;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClientUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.httpclient.org.apache.http.client.config.RequestConfig;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.HttpPost;
import org.apache.flink.shaded.httpclient.org.apache.http.entity.StringEntity;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.client.HttpClients;
import org.apache.flink.shaded.httpclient.org.apache.http.util.EntityUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link ResourceClient} used for APaaS business. In business at APaaS, {@link ResourceInfo} was obtained from
 * rds metadata service.
 */
public class APaaSResourceClient implements ResourceClient {
	private static final Logger log = LoggerFactory.getLogger(APaaSResourceClient.class);

	private static final ObjectMapper MAPPER =
		new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private static final Object lock = new Object();

	private static final RequestConfig requestConfig = RequestConfig.custom()
		.setSocketTimeout(5000)
		.setConnectTimeout(5000)
		.setConnectionRequestTimeout(5000)
		.build();

	private final String clusterName;

	private final String restHost;

	private final int restPort;

	private final String serviceURLPrefix;

	private final long queryInterval;

	private final ScheduledExecutorService executorService;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private final CloseableHttpClient httpClient;

	private int currentVersion = 0;

	private List<ResourceInfoAwareListener> resourceInfoAwareListeners = new ArrayList<>();

	private List<ResourceInfo> resourceInfos = new ArrayList<>();

	public APaaSResourceClient(Configuration configuration) throws FlinkException {
		try {
			InetAddress address = InetAddress.getLocalHost();
			this.restHost = address.getHostAddress();

			this.httpClient = HttpClients.custom().
				setDefaultRequestConfig(RequestConfig.copy(requestConfig).build()).build();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new FlinkException("Init APaaSResourceClient catch exception");
		}

		this.restPort = Integer.parseInt(configuration.getString(RestOptions.BIND_PORT));
		this.clusterName = Preconditions.checkNotNull(configuration.getValue(ResourceManagerOptions.RESOURCE_CLUSTER_NAME));
		this.queryInterval = configuration.getLong(ResourceManagerOptions.RESOURCE_GROUP_QUERY_INTERVAL);
		this.serviceURLPrefix = Preconditions.checkNotNull(configuration.getString(ResourceManagerOptions.RESOURCE_GROUP_CLIENT_DOMAIN_URL));
		this.executorService = 	Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("resource-group-handler"));
	}

	@Override
	public void start() {
		if (running.compareAndSet(false, true)) {
			log.debug("aPaaS ResourceClient started");
			// Regularly query the full ResourceGroupInfo of current cluster. If there is an update, then update the
			// ResourceInfo and notify all the ResourceInfoAwareListener.
			this.executorService.scheduleAtFixedRate(
				() -> {
					try {
						log.debug("Start to query ResourceGroupInfo in scheduledExecutor");
						QueryResourceInfoResponse response = queryResourceGroupInfo();
						if (response != null && response.code == 0) {
							int newVersion = response.result.version;
							if (newVersion >= this.currentVersion) {
								// update resource info if needed.
								List<ResourceInfo> newResourceInfos = ResourceClientUtils.convertToResourceInfo(response.result.rgInfos);
								updateResourceInfos(newResourceInfos);

								this.currentVersion = newVersion;
							}
						}

						// Subscribe to the changes of the RDS metadata.
						subscribeResourceGroupInfo(this.restHost, this.restPort);
					} catch (Exception e) {
						e.printStackTrace();
						log.warn("Catch exception in scheduledExecutor, exception: {}", e.toString());
					}
				},
				0, this.queryInterval, TimeUnit.MINUTES);
		}
	}

	@Override
	public List<ResourceInfo> fetchResourceInfos() {
		return this.resourceInfos;
	}

	@Override
	public synchronized void updateResourceInfos(List<ResourceInfo> newResourceInfos) {
		List<ResourceInfo> oldResourceInfos = this.fetchResourceInfos();

		log.debug("Update ResourceInfo oldResourceInfos: {}, newResourceInfos: {}", oldResourceInfos, newResourceInfos);
		// Determine whether ResourceInfo has changed
		if (resourceInfoChanged(oldResourceInfos, newResourceInfos)) {
			this.resourceInfos = newResourceInfos;

			try {
				notifyResourceInfoChange(newResourceInfos);
			} catch (Exception e) {
				log.warn("Notify resourceInfo changed failed, exception: {}", e.getMessage());
			}
		}
	}

	@Override
	public synchronized void registerResourceInfoChangeListener(ResourceInfoAwareListener listener) {
			resourceInfoAwareListeners.add(listener);
	}

	@Override
	public void close() throws Exception {
		this.httpClient.close();
		ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.executorService);
	}

	public int getCurrentVersion() {
		return this.currentVersion;
	}

	public void setCurrentVersion(int version) {
		this.currentVersion = version;
	}

	public QueryResourceInfoResponse queryResourceGroupInfo() {
		String url = this.serviceURLPrefix + "/htap/RGGet";

		try {
			QueryResourceInfoRequest request = new QueryResourceInfoRequest();
			request.setInstanceName(this.clusterName);

			CloseableHttpResponse response = sendPost(url, MAPPER.writeValueAsString(request));
			if (response.getStatusLine().getStatusCode() != 200) {
				log.warn("Request aPaaS metadata service failed, http code: {}", response.getStatusLine().getStatusCode());
				return null;
			}

			String resStr = EntityUtils.toString(response.getEntity(), "UTF-8");
			QueryResourceInfoResponse queryResourceInfoResponse = MAPPER.readValue(
				resStr.getBytes(),
				QueryResourceInfoResponse.class);

			if (queryResourceInfoResponse.code != 0) {
				log.warn("Request aPaaS metadata service failed, request_id: {}, code: {}",
					queryResourceInfoResponse.requestID, queryResourceInfoResponse.code);
				return null;
			}

			return queryResourceInfoResponse;
		} catch (Exception e) {
			throw new IllegalStateException("Request aPaaS metadata service failed, catch IOException", e);
		}
	}

	public boolean subscribeResourceGroupInfo(String host, int port) {
		String url = this.serviceURLPrefix + "/htap/RGSubscribe";
		try {
			SubscribeResourceInfoRequest request = new SubscribeResourceInfoRequest();
			request.setInstanceName(this.clusterName);
			request.setHost(host);
			request.setPort(port);
			request.setToken("test_token");
			CloseableHttpResponse response = sendPost(url, MAPPER.writeValueAsString(request));
			if (response.getStatusLine().getStatusCode() != 200) {
				log.warn("Request aPaaS metadata service failed, http code: {}", response.getStatusLine().getStatusCode());
				return false;
			}

			String resStr = EntityUtils.toString(response.getEntity(), "UTF-8");
			SubscribeResourceInfoResponse subscribeResourceInfoResponse = MAPPER.readValue(
				resStr.getBytes(),
				SubscribeResourceInfoResponse.class);

			if (subscribeResourceInfoResponse.code != 0) {
				log.warn("Request aPaaS metadata service failed, request_id: {}, code: {}",
					subscribeResourceInfoResponse.requestID, subscribeResourceInfoResponse.code);
				return false;
			}

			log.debug("Subscribe ResourceGroup change success");
			return true;
		} catch (Exception e) {
			throw new IllegalStateException("Request aPaaS metadata service failed, catch IOException", e);
		}
	}

	public boolean resourceInfoChanged(List<ResourceInfo> oldResourceInfos, List<ResourceInfo> newResourceInfos) {
		if (oldResourceInfos.size() != newResourceInfos.size()) {
			return true;
		}

		for (ResourceInfo newResourceInfo: newResourceInfos) {
			if (newResourceInfo.getResourceType() == ResourceInfo.ResourceType.SHARED) {
				continue;
			}

			boolean foundFlag = false;
			for (ResourceInfo oldResourceInfo: oldResourceInfos) {
				if (oldResourceInfo.getResourceType() != ResourceInfo.ResourceType.SHARED &&
					oldResourceInfo.getId().equals(newResourceInfo.getId())) {
					foundFlag = true;
					BigDecimal oldApCPUCores = BigDecimal.valueOf(oldResourceInfo.getApCPUCores());
					BigDecimal newApCPUCores = BigDecimal.valueOf(newResourceInfo.getApCPUCores());
					BigDecimal oldApScalableCPUCores = BigDecimal.valueOf(oldResourceInfo.getApScalableCPUCores());
					BigDecimal newApScalableCPUCores = BigDecimal.valueOf(newResourceInfo.getApScalableCPUCores());
					if (oldApCPUCores.compareTo(newApCPUCores) != 0 || oldApScalableCPUCores.compareTo(newApScalableCPUCores) != 0) {
						return true;
					}
				}
			}

			if (!foundFlag) {
				return true;
			}
		}

		return false;
	}

	private void notifyResourceInfoChange(List<ResourceInfo> newResourceInfos) throws Exception {
		for (ResourceInfoAwareListener listener: this.resourceInfoAwareListeners) {
			listener.onResourceInfosChanged(newResourceInfos);
		}
	}

	private CloseableHttpResponse sendPost(String url, String requestBody) throws IOException {
		HttpPost httpPost = new HttpPost(url);
		httpPost.addHeader("Content-Type", "application/json");

		StringEntity entity = new StringEntity(requestBody, "UTF-8");
		httpPost.setEntity(entity);
		httpPost.setConfig(RequestConfig.copy(requestConfig).build());

		return this.httpClient.execute(httpPost);
	}

	/**
	 * Request body of fetch resource info interface.
	 */
	public static class QueryResourceInfoRequest {
		private static final String FIELD_INSTANCE_NAME = "instance_name";

		@JsonProperty(FIELD_INSTANCE_NAME)
		public String instanceName;

		public void setInstanceName(String instanceName) {
			this.instanceName = instanceName;
		}
	}

	/**
	 * Response body of fetch resource info interface.
	 */
	@JsonInclude(JsonInclude.Include.NON_NULL)
	public static class QueryResourceInfoResponse {
		private static final String FIELD_REQUEST_ID = "request_id";

		private static final String FIELD_CODE = "code";

		private static final String FIELD_RESULT = "result";

		@JsonProperty(FIELD_REQUEST_ID)
		public final String requestID;

		@JsonProperty(FIELD_CODE)
		public final int code;

		@JsonProperty(FIELD_RESULT)
		public final Result result;

		@JsonCreator
		public QueryResourceInfoResponse(
			@JsonProperty(FIELD_REQUEST_ID) String requestID,
			@JsonProperty(FIELD_CODE) int code,
			@JsonProperty(FIELD_RESULT) Result result
		) {
			this.requestID = requestID;
			this.code = code;
			this.result = result;
		}

		/**
		 * Result part in QueryResourceInfoResponse.
		 */
		public static class Result {

			private static final String  FIELD_TOTAL_NUM = "total_num";
			private static final String FIELD_VERSION = "version";

			private static final String FIELD_RG_INFO = "rg";

			@JsonProperty(FIELD_TOTAL_NUM)
			public final int totalNum;

			@JsonProperty(FIELD_VERSION)
			public final int version;

			@JsonProperty(FIELD_RG_INFO)
			public Collection<APaaSResourceGroupInfo> rgInfos;

			@JsonCreator
			public Result(
				@JsonProperty(FIELD_TOTAL_NUM) int totalNum,
				@JsonProperty(FIELD_VERSION) int version,
				@JsonProperty(FIELD_RG_INFO) Collection<APaaSResourceGroupInfo> rgInfos
			) {
				this.totalNum = totalNum;
				this.version = version;

				if (rgInfos == null) {
					this.rgInfos = Collections.emptyList();
				} else {
					this.rgInfos = rgInfos;
				}
			}
		}
	}

	/**
	 * Request body of subscribe resource info interface.
	 */
	public static class SubscribeResourceInfoRequest {
		private static final String FIELD_INSTANCE_NAME = "instance_name";

		private static final String FIELD_HOST = "host";

		private static final String FIELD_PORT = "port";

		private static final String FIELD_TOKEN = "token";

		@JsonProperty(FIELD_INSTANCE_NAME)
		public String instanceName;

		@JsonProperty(FIELD_HOST)
		public String host;

		@JsonProperty(FIELD_PORT)
		public int port;

		@JsonProperty(FIELD_TOKEN)
		public String token;

		public void setInstanceName(String instanceName) {
			this.instanceName = instanceName;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public void setToken(String token){
			this.token = token;
		}
	}

	/**
	 * Response body of subscribe resource info interface.
	 */
	@JsonInclude(JsonInclude.Include.NON_NULL)
	public static class SubscribeResourceInfoResponse {
		private static final String FIELD_REQUEST_ID = "request_id";

		private static final String FIELD_CODE = "code";

		private static final String FIELD_MESSAGE = "message";


		@JsonProperty(FIELD_REQUEST_ID)
		public final String requestID;

		@JsonProperty(FIELD_CODE)
		public final int code;

		@JsonProperty(FIELD_MESSAGE)
		public final String message;

		@JsonCreator
		public SubscribeResourceInfoResponse(
			@JsonProperty(FIELD_REQUEST_ID) String requestID,
			@JsonProperty(FIELD_CODE) int code,
			@JsonProperty(FIELD_MESSAGE) String message
		) {
			this.requestID = requestID;
			this.code = code;
			this.message = message;
		}
	}
}
