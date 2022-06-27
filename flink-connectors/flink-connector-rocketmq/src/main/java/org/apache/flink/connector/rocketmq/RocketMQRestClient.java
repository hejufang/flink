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

package org.apache.flink.connector.rocketmq;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RetryManager;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.PropertyNamingStrategy;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * RocketMQRestClient.
 */
public class RocketMQRestClient implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQRestClient.class);

	private static final String QUERY_TOPIC_ID_API = "/secret/topic/info/id";
	private static final String REGISTER_CONSUMER_GROUP_API = "/secret/create/consumer";
	private static final String DEFAULT_API_SERVER_BASE_URL = "https://mq.byted.org/api";
	private static final MediaType REQUEST_BODY_JSON = MediaType.parse("application/json; charset=utf-8");
	private static final int TIMEOUT = 3;
	public static final ObjectMapper MAPPER = new ObjectMapper();
	private final OkHttpClient httpClient;
	private final String serverBaseUrl;
	private final String apiSecretKey;
	private final String apiSecretUser;
	private final String region;
	private final int retryTimes;
	private final int retryInitTimeMs;

	static {
		MAPPER.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
	}

	public RocketMQRestClient(String region, String owner, int retryTimes, int retryInitTimeMs) {
		this.httpClient = new OkHttpClient.Builder()
			.connectTimeout(TIMEOUT, TimeUnit.SECONDS)
			.writeTimeout(TIMEOUT, TimeUnit.SECONDS)
			.readTimeout(TIMEOUT, TimeUnit.SECONDS)
			.build();
		this.region = region;
		this.apiSecretUser = owner;
		this.retryTimes = retryTimes;
		this.retryInitTimeMs = retryInitTimeMs;
		Map<String, String> configMap = GlobalConfiguration.loadConfiguration().toMap();
		this.apiSecretKey = configMap.get("rmq.secret_key");
		this.serverBaseUrl = configMap.getOrDefault(
			String.format("rmq.server_url.%s", region.toLowerCase()), DEFAULT_API_SERVER_BASE_URL);

		LOG.info("RocketMQRestClient config: serverBaseUrl: {}, region: {}", serverBaseUrl, region);
	}

	public void registerToToolbox(String cluster, String topic, String group) {
		Tuple2<Boolean, Integer> queryResult = queryTopicId(cluster, topic);
		if (queryResult.f0) {
			int topicId = queryResult.f1;
			LOG.info("topic {} id : {}", topic, topicId);
			registerConsumerGroup(topicId, group);
		} else {
			LOG.info("topic {} does not exist", topic);
		}
	}

	private Tuple2<Boolean, Integer> queryTopicId(String cluster, String topic) {
		LOG.info("RocketMQRestClient queryTopicId: region: {}, cluster: {}, topic: {}", this.region, topic, cluster);
		QueryTopicIdRequest queryTopicIdRequest = new QueryTopicIdRequest();
		queryTopicIdRequest.setRegion(this.region);
		queryTopicIdRequest.setTopicName(topic);
		queryTopicIdRequest.setClusterName(cluster);

		Map<String, String> params = new HashMap<>();
		params.put("secret_key", apiSecretKey);
		params.put("secret_user", apiSecretUser);
		try {
			RestResponse<HashMap<String, Object>> resp = sendRequest(
				RestMethod.POST,
				QUERY_TOPIC_ID_API,
				params,
				RocketMQRestClient.serialize(queryTopicIdRequest),
				RocketMQRestClient.MAPPER.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class));

			if (resp.getErrorCode() == 0) {
				return Tuple2.of((Boolean) resp.getData().get("exist"), (Integer) resp.getData().get("topic_id"));
			} else {
				LOG.warn("Query topic id error: {}", resp.getErrorMessage());
				return Tuple2.of(false, -1);
			}
		} catch (Exception e) {
			LOG.warn("Query topic id error: {}", e.getMessage());
			return Tuple2.of(false, -1);
		}
	}

	private void registerConsumerGroup(int topicId, String group) {
		LOG.info("RocketMQRestClient registerConsumerGroup: topicId: {}, group: {}", topicId, group);
		RegisterConsumerGroupRequest registerRequest = new RegisterConsumerGroupRequest();
		registerRequest.setGroupName(group);
		registerRequest.setTopicId(topicId);
		registerRequest.setOwner(apiSecretUser);
		registerRequest.setEnableAlarm(0);
		registerRequest.setAlarmThreshold(0);
		registerRequest.setEnableB2cAlarm(0);
		registerRequest.setB2cAlarmThreshold(0);

		Map<String, String> params = new HashMap<>();
		params.put("secret_key", apiSecretKey);
		params.put("secret_user", apiSecretUser);

		try {
			RestResponse<Object> resp = sendRequest(
				RestMethod.POST,
				REGISTER_CONSUMER_GROUP_API,
				params,
				RocketMQRestClient.serialize(registerRequest),
				RocketMQRestClient.MAPPER.getTypeFactory().constructType(Object.class));

			if (resp.getErrorCode() == 0) {
				LOG.info("Register consumer group succeed");
			} else {
				LOG.warn("Register consumer group failed: {}", resp.getErrorMessage());
			}
		} catch (Exception e) {
			LOG.warn("Register consumer group exception : {}", e.getMessage());
		}
	}

	private <T> RestResponse<T> sendRequest(
			RestMethod method,
			String uri,
			Map<String, String> params,
			String payload,
			JavaType type) {
		RetryManager.Strategy retryStrategy = RetryManager.createStrategy(RetryManager.StrategyType.FIXED_DELAY.name(),
			retryTimes,
			retryInitTimeMs);

		List<RestResponse<T>> resultList = new ArrayList<>();
		RetryManager.retry(() -> {
			Request request = prepareRequest(method, uri, params, payload);
			Response response = this.httpClient.newCall(request).execute();
			if (response.isSuccessful()) {
				resultList.add(parseResponse(response, type));
			} else {
				RestResponse<T> result = new RestResponse<>();
				result.setErrorCode(response.code());
				result.setErrorMessage(response.message());
				resultList.add(result);
			}
		}, retryStrategy);
		return resultList.get(0);
	}

	public void close() {
		this.httpClient.dispatcher().executorService().shutdown();
		this.httpClient.connectionPool().evictAll();
	}

	private Request prepareRequest(RestMethod method, String uri, Map<String, String> params, String payload) {
		HttpUrl.Builder builder = Preconditions.checkNotNull(HttpUrl.parse(serverBaseUrl + uri)).newBuilder();

		for (Map.Entry<String, String> param : params.entrySet()) {
			builder.addQueryParameter(param.getKey(), param.getValue());
		}

		RequestBody requestBody = null;
		if (method == RestMethod.POST) {
			requestBody = RequestBody.create(REQUEST_BODY_JSON, payload);
		}

		Request.Builder requestBuilder = new Request.Builder()
			.url(builder.build())
			.method(method.name(), requestBody);
		return requestBuilder.build();
	}

	private static String serialize(Object obj) throws JsonProcessingException {
		return MAPPER.writeValueAsString(obj);
	}

	private <T> RestResponse<T> parseResponse(Response response, JavaType type) throws Exception {
		JavaType javaType = MAPPER.getTypeFactory().constructParametricType(RestResponse.class, new JavaType[]{type});
		ResponseBody body = response.body();
		if (body == null) {
			return null;
		} else {
			return MAPPER.readValue(body.bytes(), javaType);
		}
	}

	private enum RestMethod {
		POST;

		RestMethod() {
		}
	}

	/**
	 * QueryTopicIdRequest.
	 */
	public static class QueryTopicIdRequest {
		private String region;
		private String topicName;
		private String clusterName;

		public QueryTopicIdRequest() {
		}

		public String getRegion() {
			return region;
		}

		public void setRegion(String region) {
			this.region = region;
		}

		public String getTopicName() {
			return topicName;
		}

		public void setTopicName(String topicName) {
			this.topicName = topicName;
		}

		public String getClusterName() {
			return clusterName;
		}

		public void setClusterName(String clusterName) {
			this.clusterName = clusterName;
		}
	}

	/**
	 * RegisterConsumerGroupRequest.
	 */
	public static class RegisterConsumerGroupRequest {
		private String groupName;
		private int topicId;
		private int type;
		private String owner;
		private int enableAlarm;
		private int alarmThreshold;
		private int enableB2cAlarm;
		private int b2cAlarmThreshold;

		public RegisterConsumerGroupRequest() {
		}

		public String getGroupName() {
			return groupName;
		}

		public void setGroupName(String groupName) {
			this.groupName = groupName;
		}

		public int getTopicId() {
			return topicId;
		}

		public void setTopicId(int topicId) {
			this.topicId = topicId;
		}

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}

		public String getOwner() {
			return owner;
		}

		public void setOwner(String owner) {
			this.owner = owner;
		}

		public int getEnableAlarm() {
			return enableAlarm;
		}

		public void setEnableAlarm(int enableAlarm) {
			this.enableAlarm = enableAlarm;
		}

		public int getAlarmThreshold() {
			return alarmThreshold;
		}

		public void setAlarmThreshold(int alarmThreshold) {
			this.alarmThreshold = alarmThreshold;
		}

		public int getEnableB2cAlarm() {
			return enableB2cAlarm;
		}

		public void setEnableB2cAlarm(int enableB2cAlarm) {
			this.enableB2cAlarm = enableB2cAlarm;
		}

		public int getB2cAlarmThreshold() {
			return b2cAlarmThreshold;
		}

		public void setB2cAlarmThreshold(int b2cAlarmThreshold) {
			this.b2cAlarmThreshold = b2cAlarmThreshold;
		}
	}

	private static class RestResponse<T> implements Serializable {
		private static final long serialVersionUID = 1;

		private int errorCode;
		private String errorMessage;
		private T data;

		public RestResponse() {
		}

		public int getErrorCode() {
			return errorCode;
		}

		public void setErrorCode(int errorCode) {
			this.errorCode = errorCode;
		}

		public String getErrorMessage() {
			return errorMessage;
		}

		public void setErrorMessage(String errorMessage) {
			this.errorMessage = errorMessage;
		}

		public T getData() {
			return data;
		}

		public void setData(T data) {
			this.data = data;
		}
	}
}
