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

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.bytedance.commons.consul.Discovery;
import com.bytedance.commons.consul.ServiceNode;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_DELAY;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_ENABLED;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_RETRIES;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_TYPE;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_INTERVAL;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_MAX_ACTIONS;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_MAX_SIZE;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.CONSUL;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.DISABLE_FLUSH_ON_CHECKPOINT;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.ENABLE_PASSWORD_CONFIG;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.HTTP_SCHEMA;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.PASSWORD;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.REST_MAX_RETRY_TIMEOUT;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.REST_PATH_PREFIX;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.USERNAME;

/**
 * Version-specific upsert table sink for Elasticsearch 6.
 */
@Internal
public class Elasticsearch6UpsertTableSink extends ElasticsearchUpsertTableSinkBase {

	@VisibleForTesting
	static final RequestFactory UPDATE_REQUEST_FACTORY =
		new Elasticsearch6RequestFactory();

	private long globalRateLimit = -1;

	public Elasticsearch6UpsertTableSink(
			boolean isAppendOnly,
			TableSchema schema,
			List<Host> hosts,
			String index,
			String docType,
			String keyDelimiter,
			String keyNullLiteral,
			SerializationSchema<Row> serializationSchema,
			XContentType contentType,
			ActionRequestFailureHandler failureHandler,
			Map<SinkOption, String> sinkOptions,
			int[] keyFieldIndices,
			long globalRateLimit) {
		this(
			isAppendOnly,
			schema,
			hosts,
			index,
			docType,
			keyDelimiter,
			keyNullLiteral,
			serializationSchema,
			contentType,
			failureHandler,
			sinkOptions,
			keyFieldIndices
		);
		this.globalRateLimit = globalRateLimit;
	}

	public Elasticsearch6UpsertTableSink(
		boolean isAppendOnly,
		TableSchema schema,
		List<Host> hosts,
		String index,
		String docType,
		String keyDelimiter,
		String keyNullLiteral,
		SerializationSchema<Row> serializationSchema,
		XContentType contentType,
		ActionRequestFailureHandler failureHandler,
		Map<SinkOption, String> sinkOptions,
		int[] keyFieldIndices) {

		super(
			isAppendOnly,
			schema,
			hosts,
			index,
			docType,
			keyDelimiter,
			keyNullLiteral,
			serializationSchema,
			contentType,
			failureHandler,
			sinkOptions,
			UPDATE_REQUEST_FACTORY,
			keyFieldIndices);
	}

	@Override
	protected ElasticsearchUpsertTableSinkBase copy(
		boolean isAppendOnly,
		TableSchema schema,
		List<Host> hosts,
		String index,
		String docType,
		String keyDelimiter,
		String keyNullLiteral,
		SerializationSchema<Row> serializationSchema,
		XContentType contentType,
		ActionRequestFailureHandler failureHandler,
		Map<SinkOption, String> sinkOptions,
		RequestFactory requestFactory,
		int[] keyFieldIndices) {

		return new Elasticsearch6UpsertTableSink(
			isAppendOnly,
			schema,
			hosts,
			index,
			docType,
			keyDelimiter,
			keyNullLiteral,
			serializationSchema,
			contentType,
			failureHandler,
			sinkOptions,
			keyFieldIndices,
			globalRateLimit);
	}

	@Override
	protected SinkFunction<Tuple2<Boolean, Row>> createSinkFunction(
			List<Host> hosts,
			ActionRequestFailureHandler failureHandler,
			Map<SinkOption, String> sinkOptions,
			ElasticsearchUpsertSinkFunction upsertSinkFunction) {

		List<HttpHost> httpHosts = new ArrayList<>();

		String consul = sinkOptions.get(CONSUL);
		String httpSchema = sinkOptions.getOrDefault(HTTP_SCHEMA, "http");

		if (consul == null) {
			httpHosts = hosts.stream()
				.map((host) -> new HttpHost(host.hostname, host.port, host.protocol))
				.collect(Collectors.toList());
		}
		else {
			List<ServiceNode> serverNodeList = new Discovery().translateOne(consul);
			if (serverNodeList == null || serverNodeList.size() < 1) {
				throw new RuntimeException("No host for consul");
			}
			for (int i = 0; i < serverNodeList.size(); i++) {
				ServiceNode serviceNode = serverNodeList.get(i);
				String host = serviceNode.getHost();
				int port = serviceNode.getPort();
				httpHosts.add(new HttpHost(host, port, httpSchema));
			}
		}

		final ElasticsearchSink.Builder<Tuple2<Boolean, Row>> builder = createBuilder(upsertSinkFunction, httpHosts);

		builder.setFailureHandler(failureHandler);

		Optional.ofNullable(sinkOptions.get(BULK_FLUSH_MAX_ACTIONS))
			.ifPresent(v -> builder.setBulkFlushMaxActions(Integer.valueOf(v)));

		Optional.ofNullable(sinkOptions.get(BULK_FLUSH_MAX_SIZE))
			.ifPresent(v -> builder.setBulkFlushMaxSizeMb(MemorySize.parse(v).getMebiBytes()));

		Optional.ofNullable(sinkOptions.get(BULK_FLUSH_INTERVAL))
			.ifPresent(v -> builder.setBulkFlushInterval(Long.valueOf(v)));

		Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_ENABLED))
			.ifPresent(v -> builder.setBulkFlushBackoff(Boolean.valueOf(v)));

		Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_TYPE))
			.ifPresent(v -> builder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.valueOf(v)));

		Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_RETRIES))
			.ifPresent(v -> builder.setBulkFlushBackoffRetries(Integer.valueOf(v)));

		Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_DELAY))
			.ifPresent(v -> builder.setBulkFlushBackoffDelay(Long.valueOf(v)));

		DefaultRestClientFactory restClientFactory = new DefaultRestClientFactory(
			Optional.ofNullable(sinkOptions.get(REST_MAX_RETRY_TIMEOUT))
				.map(Integer::valueOf)
				.orElse(null),
			sinkOptions.get(REST_PATH_PREFIX));

		Optional.ofNullable(sinkOptions.get(ENABLE_PASSWORD_CONFIG))
			.ifPresent(v -> {
				if (Boolean.valueOf(v)) {
					restClientFactory.configPasswordSettings(
						Boolean.valueOf(sinkOptions.get(ENABLE_PASSWORD_CONFIG)),
						sinkOptions.get(USERNAME),
						sinkOptions.get(PASSWORD)
					);
				}
			});

		builder.setRestClientFactory(restClientFactory);

		if (globalRateLimit > 0) {
			builder.setGlobalRateLimit(globalRateLimit);
		}

		final ElasticsearchSink<Tuple2<Boolean, Row>> sink = builder.build();

		Optional.ofNullable(sinkOptions.get(DISABLE_FLUSH_ON_CHECKPOINT))
			.ifPresent(v -> {
				if (Boolean.valueOf(v)) {
					sink.disableFlushOnCheckpoint();
				}
			});

		return sink;
	}

	@VisibleForTesting
	ElasticsearchSink.Builder<Tuple2<Boolean, Row>> createBuilder(
			ElasticsearchUpsertSinkFunction upsertSinkFunction,
			List<HttpHost> httpHosts) {
		return new ElasticsearchSink.Builder<>(httpHosts, upsertSinkFunction);
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	/**
	 * Serializable {@link RestClientFactory} used by the sink.
	 */
	@VisibleForTesting
	static class DefaultRestClientFactory implements RestClientFactory {

		private Integer maxRetryTimeout;
		private String pathPrefix;

		/**
		 * password settings.
		 */
		private boolean enablePasswordConfig;
		private String userName;
		private String password;

		public DefaultRestClientFactory(@Nullable Integer maxRetryTimeout, @Nullable String pathPrefix) {
			this.maxRetryTimeout = maxRetryTimeout;
			this.pathPrefix = pathPrefix;
		}

		public void configPasswordSettings(boolean enablePasswordConfig, String userName, String password) {
			this.enablePasswordConfig = enablePasswordConfig;
			this.userName = userName;
			this.password = password;
		}

		@Override
		public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
			if (maxRetryTimeout != null) {
				restClientBuilder.setMaxRetryTimeoutMillis(maxRetryTimeout);
			}
			if (pathPrefix != null) {
				restClientBuilder.setPathPrefix(pathPrefix);
			}

			if (enablePasswordConfig) {
				try {
					SSLContext sslContext = SSLContext.getInstance("TLS");
					final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
					credentialsProvider.setCredentials(AuthScope.ANY,
						new UsernamePasswordCredentials(userName, password));
					restClientBuilder.setHttpClientConfigCallback(httpClientBuilder ->
						httpClientBuilder
							.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)
							.setDefaultCredentialsProvider(credentialsProvider)
							.setSSLContext(sslContext));
				} catch (NoSuchAlgorithmException e) {
					throw new RuntimeException(e);
				}
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			DefaultRestClientFactory that = (DefaultRestClientFactory) o;
			return Objects.equals(maxRetryTimeout, that.maxRetryTimeout) &&
				Objects.equals(pathPrefix, that.pathPrefix);
		}

		@Override
		public int hashCode() {
			return Objects.hash(
				maxRetryTimeout,
				pathPrefix);
		}
	}

	/**
	 * Version-specific creation of {@link org.elasticsearch.action.ActionRequest}s used by the sink.
	 */
	private static class Elasticsearch6RequestFactory implements RequestFactory {

		@Override
		public UpdateRequest createUpdateRequest(
				String index,
				String docType,
				String key,
				XContentType contentType,
				byte[] document) {
			return new UpdateRequest(index, docType, key)
				.doc(document, contentType)
				.upsert(document, contentType);
		}

		@Override
		public IndexRequest createIndexRequest(
				String index,
				String docType,
				XContentType contentType,
				byte[] document) {
			return new IndexRequest(index, docType)
				.source(document, contentType);
		}

		@Override
		public DeleteRequest createDeleteRequest(String index, String docType, String key) {
			return new DeleteRequest(index, docType, key);
		}
	}
}
