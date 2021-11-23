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

package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.byted.org.byted.infsec.client.InfSecException;
import org.apache.flink.shaded.byted.org.byted.infsec.client.SecTokenC;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
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
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.CONNECT_TIMEOUT;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.DISABLE_FLUSH_ON_CHECKPOINT;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.REST_PATH_PREFIX;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.SOCKET_TIMEOUT;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.URI;

/**
 * Version-specific upsert table sink for Elasticsearch 7.
 */
@Internal
public class Elasticsearch7UpsertTableSink extends ElasticsearchUpsertTableSinkBase {

	private static final String BYTEES_GDPR_HEADER_KEY = "Gdpr-Token";

	@VisibleForTesting
	static final RequestFactory UPDATE_REQUEST_FACTORY =
		new Elasticsearch7RequestFactory();

	private long globalRateLimit = -1;

	public Elasticsearch7UpsertTableSink(
			boolean isAppendOnly,
			TableSchema schema,
			List<Host> hosts,
			String index,
			String keyDelimiter,
			String keyNullLiteral,
			SerializationSchema<Row> serializationSchema,
			XContentType contentType,
			ActionRequestFailureHandler failureHandler,
			Map<SinkOption, String> sinkOptions,
			int[] keyFieldIndices,
			long globalRateLimit,
			int parallelism,
			boolean byteEsMode,
			boolean ignoreInvalidData) {
		this(
			isAppendOnly,
			schema,
			hosts,
			index,
			keyDelimiter,
			keyNullLiteral,
			serializationSchema,
			contentType,
			failureHandler,
			sinkOptions,
			keyFieldIndices,
			parallelism,
			byteEsMode,
			ignoreInvalidData
		);
		this.globalRateLimit = globalRateLimit;
	}

	public Elasticsearch7UpsertTableSink(
			boolean isAppendOnly,
			TableSchema schema,
			List<Host> hosts,
			String index,
			String keyDelimiter,
			String keyNullLiteral,
			SerializationSchema<Row> serializationSchema,
			XContentType contentType,
			ActionRequestFailureHandler failureHandler,
			Map<SinkOption, String> sinkOptions,
			int[] keyFieldIndices,
			int parallelism,
			boolean byteEsMode,
			boolean ignoreInvalidData) {

		super(
			isAppendOnly,
			schema,
			hosts,
			index,
			"",
			keyDelimiter,
			keyNullLiteral,
			serializationSchema,
			contentType,
			failureHandler,
			sinkOptions,
			UPDATE_REQUEST_FACTORY,
			keyFieldIndices,
			parallelism,
			byteEsMode,
			ignoreInvalidData);
	}

	@VisibleForTesting
	Elasticsearch7UpsertTableSink(
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
		int parallelism) {

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
			keyFieldIndices,
			parallelism);
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
			int[] keyFieldIndices,
			int parallelism,
			boolean byteEsMode) {

		return new Elasticsearch7UpsertTableSink(
			isAppendOnly,
			schema,
			hosts,
			index,
			keyDelimiter,
			keyNullLiteral,
			serializationSchema,
			contentType,
			failureHandler,
			sinkOptions,
			keyFieldIndices,
			globalRateLimit,
			parallelism,
			byteEsMode,
			ignoreInvalidData);

	}

	@Override
	protected SinkFunction<Tuple2<Boolean, Row>> createSinkFunction(
			List<Host> hosts,
			ActionRequestFailureHandler failureHandler,
			Map<SinkOption, String> sinkOptions,
			ElasticsearchUpsertSinkFunction upsertSinkFunction) {

		Optional<String> psm = Optional.ofNullable(sinkOptions.get(URI));

		ElasticsearchSink.Builder<Tuple2<Boolean, Row>> builder;
		// if we support psm to lookup hosts, we need to fake a schema://host:port
		// to ensure underlying RestHighLevelClient not to complain about it.
		// psm example: psm://byte.es.flink_connector_test.service.lq$data
		if (psm.isPresent()) {
			try {
				URI uri = new URI(psm.get());
				String host = uri.getAuthority();
				if (uri.getHost() != null) {
					host = uri.getHost();
				}
				final List<HttpHost> httpHosts = Collections.singletonList(
					new HttpHost(host, uri.getPort(), uri.getScheme()));
				builder = createBuilder(upsertSinkFunction, httpHosts);
			} catch (URISyntaxException e) {
				throw new TableException(e.getMessage());
			}
		} else {
			final List<HttpHost> httpHosts = hosts.stream()
				.map((host) -> new HttpHost(host.hostname, host.port, host.protocol))
				.collect(Collectors.toList());
			builder = createBuilder(upsertSinkFunction, httpHosts);
		}

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

		Optional.ofNullable(sinkOptions.get(CONNECT_TIMEOUT))
			.ifPresent(v -> builder.setConnectTimeout(Integer.parseInt(v)));

		Optional.ofNullable(sinkOptions.get(SOCKET_TIMEOUT))
			.ifPresent(v -> builder.setSocketTimeout(Integer.parseInt(v)));

		if (psm.isPresent()) {

			builder.setRestClientFactory(new RoutedRestClientFactory(sinkOptions.get(REST_PATH_PREFIX)));
		} else {
			builder.setRestClientFactory(new DefaultRestClientFactory(sinkOptions.get(REST_PATH_PREFIX)));
		}

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

		private String pathPrefix;

		public DefaultRestClientFactory(@Nullable String pathPrefix) {
			this.pathPrefix = pathPrefix;
		}

		@Override
		public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
			if (pathPrefix != null) {
				restClientBuilder.setPathPrefix(pathPrefix);
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
			return Objects.equals(pathPrefix, that.pathPrefix);
		}

		@Override
		public int hashCode() {
			return Objects.hash(pathPrefix);
		}
	}

	/**
	 * A routed RestClientFactory.
	 */
	static class RoutedRestClientFactory implements RestClientFactory {

		private static final long serialVersionUID = 1L;

		private final String pathPrefix;

		public RoutedRestClientFactory(@Nullable String pathPrefix) {
			this.pathPrefix = pathPrefix;
		}

		@Override
		public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
			if (pathPrefix != null) {
				restClientBuilder.setPathPrefix(pathPrefix);
			}
			// add GDPR header
			restClientBuilder.setDefaultHeaders(new Header[]{new BasicHeader(BYTEES_GDPR_HEADER_KEY, getGdprToken())});
		}

		private String getGdprToken() {
			try {
				String token = SecTokenC.getToken(false);
				if (token != null && !token.isEmpty()) {
					return token;
				}
			} catch (InfSecException e) {
				throw new FlinkRuntimeException(e);
			}
			return null;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			RoutedRestClientFactory that = (RoutedRestClientFactory) o;
			return Objects.equals(pathPrefix, that.pathPrefix);
		}

		@Override
		public int hashCode() {
			return Objects.hash(pathPrefix);
		}
	}

	/**
	 * Version-specific creation of {@link org.elasticsearch.action.ActionRequest}s used by the sink.
	 */
	private static class Elasticsearch7RequestFactory implements RequestFactory {

		private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7RequestFactory.class);

		@Override
		public UpdateRequest createUpdateRequest(
				String index,
				String docType,
				String key,
				XContentType contentType,
				byte[] document) {
			return new UpdateRequest(index, key)
				.doc(document, contentType)
				.upsert(document, contentType);
		}

		@Override
		public IndexRequest createIndexRequest(
				String index,
				String docType,
				XContentType contentType,
				byte[] document) {
			return new IndexRequest(index)
				.source(document, contentType);
		}

		@Override
		public DeleteRequest createDeleteRequest(String index, String docType, String key) {
			return new DeleteRequest(index, key);
		}

		@Override
		public void addCustomRequestToIndexer(
				String doc,
				RequestIndexer indexer,
				long version,
				String routing,
				String index,
				String id,
				String opType) {
			if (opType == null || index == null) {
				LOG.error("Invalid OpType: {} or index {}", opType, index);
				return;
			}
			switch (opType.toLowerCase()) {
				case "delete":
					DeleteRequest deleteRequest = new DeleteRequest().index(index).id(id);
					if (version > 0) {
						deleteRequest.version(version).versionType(VersionType.EXTERNAL_GTE);
					}
					if (!org.elasticsearch.common.Strings.isEmpty(routing)) {
						deleteRequest.routing(routing);
					}
					if (deleteRequest.validate() != null) {
						LOG.error("Construct DeleteRequest error for value: {}", doc);
						return;
					}
					indexer.add(deleteRequest);
					break;
				case "index":
					IndexRequest indexRequest = new IndexRequest().index(index).id(id);
					indexRequest.source(doc, XContentType.JSON);
					if (version > 0) {
						indexRequest.version(version).versionType(VersionType.EXTERNAL_GTE);
					}
					if (!org.elasticsearch.common.Strings.isEmpty(routing)) {
						indexRequest.routing(routing);
					}
					if (indexRequest.validate() != null) {
						LOG.error("Construct IndexRequest error for value: {}", doc);
						return;
					}
					indexer.add(indexRequest);
					break;
				case "create":
					IndexRequest createRequest = new IndexRequest().index(index).id(id);
					createRequest.create(true);
					createRequest.source(doc, XContentType.JSON);
					if (version > 0) {
						createRequest.version(version).versionType(VersionType.EXTERNAL_GTE);
					}
					if (!org.elasticsearch.common.Strings.isEmpty(routing)) {
						createRequest.routing(routing);
					}
					if (createRequest.validate() != null) {
						LOG.error("Construct CreateRequest error for value: {}", doc);
						return;
					}
					indexer.add(createRequest);
					break;
				case "update":
					UpdateRequest updateRequest = new UpdateRequest().index(index).id(id);
					updateRequest.doc(doc, XContentType.JSON);
					if (!org.elasticsearch.common.Strings.isEmpty(routing)) {
						updateRequest.routing(routing);
					}
					if (updateRequest.validate() != null) {
						LOG.error("Construct UpdateRequest error for value: {}", doc);
						return;
					}
					indexer.add(updateRequest);
					break;
				case "upsert":
					UpdateRequest upsertRequest = new UpdateRequest().index(index).id(id);
					upsertRequest.doc(doc, XContentType.JSON);
					upsertRequest.docAsUpsert(true);
					if (!org.elasticsearch.common.Strings.isEmpty(routing)) {
						upsertRequest.routing(routing);
					}
					if (upsertRequest.validate() != null) {
						LOG.error("Construct UpsertRequest error for value: {}", doc);
						return;
					}
					indexer.add(upsertRequest);
					break;
			}
		}
	}
}
