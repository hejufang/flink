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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.byted.org.byted.infsec.client.InfSecException;
import org.apache.flink.shaded.byted.org.byted.infsec.client.SecTokenC;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.BYTE_ES_MODE_ENABLED;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.CONNECTION_PATH_PREFIX;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.IGNORE_INVALID_DATA;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.URI;
import static org.apache.flink.table.factories.FactoryUtil.SINK_IGNORE_DELETE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ElasticsearchSink} from a logical
 * description.
 */
@Internal
final class Elasticsearch7DynamicSink implements DynamicTableSink {

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7DynamicSink.class);
	private static final String BYTEES_GDPR_HEADER_KEY = "Gdpr-Token";
	private static final String DATA_PALACE_GDPR_HEADER_KEY = "authorization";

	@VisibleForTesting
	static final Elasticsearch7RequestFactory REQUEST_FACTORY = new Elasticsearch7DynamicSink.Elasticsearch7RequestFactory();

	private final EncodingFormat<SerializationSchema<RowData>> format;
	private final TableSchema schema;
	private final Elasticsearch7Configuration config;

	public Elasticsearch7DynamicSink(
			EncodingFormat<SerializationSchema<RowData>> format,
			Elasticsearch7Configuration config,
			TableSchema schema) {
		this(format, config, schema, (ElasticsearchSink.Builder::new));
	}

	//--------------------------------------------------------------
	// Hack to make configuration testing possible.
	//
	// The code in this block should never be used outside of tests.
	// Having a way to inject a builder we can assert the builder in
	// the test. We can not assert everything though, e.g. it is not
	// possible to assert flushing on checkpoint, as it is configured
	// on the sink itself.
	//--------------------------------------------------------------

	private final ElasticSearchBuilderProvider builderProvider;

	@FunctionalInterface
	interface ElasticSearchBuilderProvider {
		ElasticsearchSink.Builder<RowData> createBuilder(
			List<HttpHost> httpHosts,
			RowElasticsearchSinkFunction upsertSinkFunction);
	}

	Elasticsearch7DynamicSink(
			EncodingFormat<SerializationSchema<RowData>> format,
			Elasticsearch7Configuration config,
			TableSchema schema,
			ElasticSearchBuilderProvider builderProvider) {
		this.format = format;
		this.schema = schema;
		this.config = config;
		this.builderProvider = builderProvider;
	}

	//--------------------------------------------------------------
	// End of hack to make configuration testing possible
	//--------------------------------------------------------------

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (RowKind kind : requestedMode.getContainedKinds()) {
			if (kind != RowKind.UPDATE_BEFORE) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	@Override
	public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
		return () -> {
			SerializationSchema<RowData> format = this.format.createRuntimeEncoder(context, schema.toRowDataType());

			ByteESHandler handler = null;
			if (config.config.get(BYTE_ES_MODE_ENABLED)) {
				ByteESHandler.Builder byteESHandlerBuilder = ByteESHandler.builder();

				for (int i = 0; i < schema.getFieldCount(); ++i) {
					String fieldName = schema.getFieldName(i).get();
					DataType fieldType = schema.getFieldDataType(i).get();
					if (fieldName.equalsIgnoreCase(ByteESHandler.VERSION)) {
						byteESHandlerBuilder.setVersionGetter(RowData.createFieldGetter(fieldType.getLogicalType(), i));
						// Validate version field type. Can only be INT/BIGINT/SMALLINT/TINYINT.
						LogicalTypeRoot typeRoot = fieldType.getLogicalType().getTypeRoot();
						if (typeRoot == DECIMAL || !typeRoot.getFamilies().contains(LogicalTypeFamily.EXACT_NUMERIC)) {
							throw new TableException("_version field for ES sink should be integer types like int/bigint.");
						}
					} else if (fieldName.equalsIgnoreCase(ByteESHandler.ROUTING)) {
						byteESHandlerBuilder.setRoutingIndex(i);
						// Validate routing field type. Can only be VARCHAR/CHAR.
						LogicalTypeRoot typeRoot = fieldType.getLogicalType().getTypeRoot();
						if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
							throw new TableException("_routing field for ES sink should be varchar/char type.");
						}
					} else if (fieldName.equalsIgnoreCase(ByteESHandler.INDEX)) {
						byteESHandlerBuilder.setIndexIndex(i);
						// Validate index field type Can only be VARCHAR/CHAR.
						LogicalTypeRoot typeRoot = fieldType.getLogicalType().getTypeRoot();
						if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
							throw new TableException("_index field for ES sink should be varchar/char type.");
						}
					} else if (fieldName.equalsIgnoreCase(ByteESHandler.ID)) {
						byteESHandlerBuilder.setIdIndex(i);
						// Validate index field type Can only be VARCHAR/CHAR.
						LogicalTypeRoot typeRoot = fieldType.getLogicalType().getTypeRoot();
						if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
							throw new TableException("_id field for ES sink should be varchar/char type.");
						}
					} else if (fieldName.equalsIgnoreCase(ByteESHandler.OP_TYPE)) {
						byteESHandlerBuilder.setOpTypeIndex(i);
						// Validate index field type Can only be VARCHAR/CHAR.
						LogicalTypeRoot typeRoot = fieldType.getLogicalType().getTypeRoot();
						if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
							throw new TableException("_opType field for ES sink should be varchar/char type.");
						}
					} else if (fieldName.equalsIgnoreCase(ByteESHandler.SOURCE)) {
						byteESHandlerBuilder.setSourceIndex(i);
						// Validate index field type Can only be VARCHAR/CHAR.
						LogicalTypeRoot typeRoot = fieldType.getLogicalType().getTypeRoot();
						if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
							throw new TableException("_source field for ES sink should be varchar/char type.");
						}
					}
				}

				byteESHandlerBuilder.setIgnoreInvalidData(config.config.get(IGNORE_INVALID_DATA));

				handler = byteESHandlerBuilder.build();
			}

			final RowElasticsearchSinkFunction upsertFunction =
				new RowElasticsearchSinkFunction(
					IndexGeneratorFactory.createIndexGenerator(config.getIndex(), schema),
					null, // this is deprecated in es 7+
					format,
					XContentType.JSON,
					REQUEST_FACTORY,
					KeyExtractor.createKeyExtractor(schema, config.getKeyDelimiter()),
					handler,
					config.config.get(SINK_IGNORE_DELETE)
				);

			final ElasticsearchSink.Builder<RowData> builder = builderProvider.createBuilder(
				config.getHosts(),
				upsertFunction);

			Optional<Long> rate = config.getRateLimitNum();
			if (rate.isPresent()) {
				FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
				rateLimiter.setRate(rate.get());
				builder.setRateLimiter(rateLimiter);
			}

			builder.setFailureHandler(config.getFailureHandler());
			builder.setBulkFlushMaxActions(config.getBulkFlushMaxActions());
			builder.setBulkFlushMaxSizeMb((int) (config.getBulkFlushMaxByteSize() >> 20));
			builder.setBulkFlushInterval(config.getBulkFlushInterval());
			builder.setBulkFlushBackoff(config.isBulkFlushBackoffEnabled());
			builder.setParallelism(config.getParallelism());
			builder.setFailureRequestMaxRetries(config.getFailureRequestMaxRetries());
			config.getBulkFlushBackoffType().ifPresent(builder::setBulkFlushBackoffType);
			config.getBulkFlushBackoffRetries().ifPresent(builder::setBulkFlushBackoffRetries);
			config.getBulkFlushBackoffDelay().ifPresent(builder::setBulkFlushBackoffDelay);
			builder.setConnectTimeout(config.getConnectTimeout());
			builder.setSocketTimeout(config.getSocketTimeout());

			// we must overwrite the default factory which is defined with a lambda because of a bug
			// in shading lambda serialization shading see FLINK-18006
			if (config.getUsername().isPresent()) {
				builder.setRestClientFactory(new AuthRestClientFactory(
					config.getPathPrefix().orElse(null),
					config.getUsername().get(),
					config.getPassword().get()));
			} else if (config.config.getOptional(URI).isPresent()) {
				String prefix = config.config.get(CONNECTION_PATH_PREFIX);
				builder.setRestClientFactory(new RoutedRestClientFactory(
					prefix,
					config.getConnectTimeout(),
					config.getSocketTimeout()));
			} else {
				String prefix = config.config.get(CONNECTION_PATH_PREFIX);
				builder.setRestClientFactory(new DefaultRestClientFactory(
					prefix,
					config.getConnectTimeout(),
					config.getSocketTimeout()));
			}

			final ElasticsearchSink<RowData> sink = builder.build();

			if (config.isDisableFlushOnCheckpoint()) {
				sink.disableFlushOnCheckpoint();
			}

			return sink;
		};
	}

	@Override
	public DynamicTableSink copy() {
		return this;
	}

	@Override
	public String asSummaryString() {
		return "Elasticsearch7";
	}

	/**
	 * Serializable {@link RestClientFactory} used by the sink.
	 */
	@VisibleForTesting
	static class DefaultRestClientFactory implements RestClientFactory {

		private final String pathPrefix;
		private final int connectTimeoutMs;
		private final int socketTimeoutMs;

		public DefaultRestClientFactory(
				@Nullable String pathPrefix,
				int connectTimeoutMs,
				int socketTimeoutMs) {
			this.pathPrefix = pathPrefix;
			this.connectTimeoutMs = connectTimeoutMs;
			this.socketTimeoutMs = socketTimeoutMs;
		}

		public DefaultRestClientFactory(@Nullable String pathPrefix) {
			this(pathPrefix, -1, -1);
		}

		@Override
		public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
			if (pathPrefix != null) {
				restClientBuilder.setPathPrefix(pathPrefix);
			}
			// add dynamic GDPR header
			restClientBuilder.setHttpClientConfigCallback(httpClientBuilder ->
				httpClientBuilder.addInterceptorLast((HttpRequestInterceptor) (httpRequest, httpContext) -> {
					final String gdprToken = getGdprToken();
					httpRequest.addHeader(BYTEES_GDPR_HEADER_KEY, gdprToken);
					httpRequest.addHeader(DATA_PALACE_GDPR_HEADER_KEY, gdprToken);
				}));
			restClientBuilder.setRequestConfigCallback(requestConfigBuilder ->
				requestConfigBuilder.setConnectTimeout(connectTimeoutMs).setSocketTimeout(socketTimeoutMs));
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
	 * Serializable {@link RestClientFactory} used by the sink which enable authentication.
	 */
	@VisibleForTesting
	static class AuthRestClientFactory implements RestClientFactory {

		private static final long serialVersionUID = 1L;

		private final String pathPrefix;
		private final String username;
		private final String password;
		private transient CredentialsProvider credentialsProvider;

		public AuthRestClientFactory(@Nullable String pathPrefix, String username, String password) {
			this.pathPrefix = pathPrefix;
			this.password = password;
			this.username = username;
		}

		@Override
		public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
			if (pathPrefix != null) {
				restClientBuilder.setPathPrefix(pathPrefix);
			}
			if (credentialsProvider == null) {
				credentialsProvider = new BasicCredentialsProvider();
				credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
			}
			restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
				httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			AuthRestClientFactory that = (AuthRestClientFactory) o;
			return Objects.equals(pathPrefix, that.pathPrefix) &&
				Objects.equals(username, that.username) &&
				Objects.equals(password, that.password);
		}

		@Override
		public int hashCode() {
			return Objects.hash(pathPrefix, password, username);
		}
	}

	/**
	 * A routed RestClientFactory.
	 */
	static class RoutedRestClientFactory implements RestClientFactory {

		private static final long serialVersionUID = 1L;

		private final String pathPrefix;
		private final int connectTimeoutMs;
		private final int socketTimeoutMs;

		public RoutedRestClientFactory(
				@Nullable String pathPrefix,
				int connectTimeoutMs,
				int socketTimeoutMs) {
			this.pathPrefix = pathPrefix;
			this.connectTimeoutMs = connectTimeoutMs;
			this.socketTimeoutMs = socketTimeoutMs;
		}

		@Override
		public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
			if (pathPrefix != null) {
				restClientBuilder.setPathPrefix(pathPrefix);
			}
			// add dynamic GDPR header
			restClientBuilder.setHttpClientConfigCallback(httpClientBuilder ->
				httpClientBuilder.addInterceptorLast((HttpRequestInterceptor) (httpRequest, httpContext) -> {
					final String gdprToken = getGdprToken();
					httpRequest.addHeader(BYTEES_GDPR_HEADER_KEY, gdprToken);
					httpRequest.addHeader(DATA_PALACE_GDPR_HEADER_KEY, gdprToken);
			}));
			restClientBuilder.setRequestConfigCallback(requestConfigBuilder ->
				requestConfigBuilder.setConnectTimeout(connectTimeoutMs).setSocketTimeout(socketTimeoutMs));
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

	private static String getGdprToken() {
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
				String key,
				XContentType contentType,
				byte[] document) {
			return new IndexRequest(index)
				.id(key)
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

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Elasticsearch7DynamicSink that = (Elasticsearch7DynamicSink) o;
		return Objects.equals(format, that.format) &&
			Objects.equals(schema, that.schema) &&
			Objects.equals(config, that.config) &&
			Objects.equals(builderProvider, that.builderProvider);
	}

	@Override
	public int hashCode() {
		return Objects.hash(format, schema, config, builderProvider);
	}
}
