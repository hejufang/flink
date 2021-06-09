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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.IgnoringFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryCommonFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FAILURE_HANDLER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.FAILURE_REQUEST_MAX_RETRIES_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.PASSWORD_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.URI;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.USERNAME_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.USER_DEFINED_PARAMS;

/**
 * Accessor methods to elasticsearch options.
 */
@Internal
class ElasticsearchConfiguration {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchConfiguration.class);

	protected final ReadableConfig config;
	private final ClassLoader classLoader;

	ElasticsearchConfiguration(ReadableConfig config, ClassLoader classLoader) {
		this.config = config;
		this.classLoader = classLoader;
	}

	public ActionRequestFailureHandler getFailureHandler() {
		final ActionRequestFailureHandler failureHandler;
		String value = config.get(FAILURE_HANDLER_OPTION);
		switch (value.toUpperCase()) {
			case "FAIL":
				failureHandler = new NoOpFailureHandler();
				break;
			case "IGNORE":
				failureHandler = new IgnoringFailureHandler();
				break;
			case "RETRY-REJECTED":
				failureHandler = new RetryRejectedExecutionFailureHandler();
				break;
			case "RETRY-COMMON":
				failureHandler = new RetryCommonFailureHandler();
				break;
			default:
				try {
					Class<?> failureHandlerClass = Class.forName(value, false, classLoader);
					try {
						// user can define a custom ActionRequestFailureHandler with a constructor like:
						// public MyFailureHandler(Map<String, String> params) {
						//     ...
						// }
						// this is really a hack, however, some use cases need this.
						// user can config USER_DEFINED_PARAMS to pass any string they want, then we will
						// pass it to the customized ActionRequestFailureHandler.
						Constructor<?> constructor = failureHandlerClass.getDeclaredConstructor(Map.class);
						Map<String, String> userDefinedConfigs = new HashMap<>();
						if (config.getOptional(USER_DEFINED_PARAMS).isPresent()) {
							String params = config.get(USER_DEFINED_PARAMS);
							userDefinedConfigs.put(USER_DEFINED_PARAMS.key(), params);
							for (FallbackKey legacyKey : USER_DEFINED_PARAMS.fallbackKeys()) {
								userDefinedConfigs.put(legacyKey.getKey(), params);
							}

							String uri = config.get(URI);
							if (uri != null) {
								userDefinedConfigs.put("connector.uri", uri);
							}
						}
						return (ActionRequestFailureHandler) constructor.newInstance(userDefinedConfigs);
					} catch (NoSuchMethodException e) {
						failureHandler = (ActionRequestFailureHandler) InstantiationUtil.instantiate(failureHandlerClass);
					} catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
						LOG.warn("Found custom ActionRequestFailureHandler " +
							failureHandlerClass.getCanonicalName() +
							" with single param of Map type, however cannot instantiate," +
							" trying default no parameter constructor.", e);
						failureHandler = (ActionRequestFailureHandler) InstantiationUtil.instantiate(failureHandlerClass);
					}
				} catch (ClassNotFoundException e) {
					throw new ValidationException("Could not instantiate the failure handler class: " + value, e);
				}
				break;
		}
		return failureHandler;
	}

	public String getDocumentType() {
		return config.get(ElasticsearchOptions.DOCUMENT_TYPE_OPTION);
	}

	public int getBulkFlushMaxActions() {
		int maxActions = config.get(ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION);
		// convert 0 to -1, because Elasticsearch client use -1 to disable this configuration.
		return maxActions == 0 ? -1 : maxActions;
	}

	public long getBulkFlushMaxByteSize() {
		long maxSize = config.get(ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION).getBytes();
		// convert 0 to -1, because Elasticsearch client use -1 to disable this configuration.
		return maxSize == 0 ? -1 : maxSize;
	}

	public long getBulkFlushInterval() {
		long interval = config.get(BULK_FLUSH_INTERVAL_OPTION).toMillis();
		// convert 0 to -1, because Elasticsearch client use -1 to disable this configuration.
		return interval == 0 ? -1 : interval;
	}

	public Optional<String> getUsername() {
		return config.getOptional(USERNAME_OPTION);
	}

	public Optional<String> getPassword() {
		return config.getOptional(PASSWORD_OPTION);
	}

	public boolean isBulkFlushBackoffEnabled() {
		return config.get(BULK_FLUSH_BACKOFF_TYPE_OPTION) != ElasticsearchOptions.BackOffType.DISABLED;
	}

	public Optional<ElasticsearchSinkBase.FlushBackoffType> getBulkFlushBackoffType() {
		switch (config.get(BULK_FLUSH_BACKOFF_TYPE_OPTION)) {
			case CONSTANT:
				return Optional.of(ElasticsearchSinkBase.FlushBackoffType.CONSTANT);
			case EXPONENTIAL:
				return Optional.of(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);
			default:
				return Optional.empty();
		}
	}

	public Optional<Integer> getBulkFlushBackoffRetries() {
		return config.getOptional(BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION);
	}

	public Optional<Long> getBulkFlushBackoffDelay() {
		return config.getOptional(BULK_FLUSH_BACKOFF_DELAY_OPTION).map(Duration::toMillis);
	}

	public boolean isDisableFlushOnCheckpoint() {
		return !config.get(ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION);
	}

	public String getIndex() {
		return config.get(ElasticsearchOptions.INDEX_OPTION);
	}

	public String getKeyDelimiter() {
		return config.get(ElasticsearchOptions.KEY_DELIMITER_OPTION);
	}

	public Optional<String> getPathPrefix() {
		return config.getOptional(ElasticsearchOptions.CONNECTION_PATH_PREFIX);
	}

	public int getFailureRequestMaxRetries() {
		return config.get(FAILURE_REQUEST_MAX_RETRIES_OPTION);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ElasticsearchConfiguration that = (ElasticsearchConfiguration) o;
		return Objects.equals(config, that.config) &&
			Objects.equals(classLoader, that.classLoader);
	}

	@Override
	public int hashCode() {
		return Objects.hash(config, classLoader);
	}
}
