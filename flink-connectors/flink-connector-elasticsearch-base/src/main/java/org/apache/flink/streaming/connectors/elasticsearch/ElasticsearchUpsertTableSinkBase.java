/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.typeutils.TypeCheckUtils;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;

/**
 * A version-agnostic Elasticsearch {@link UpsertStreamTableSink}.
 */
@Internal
public abstract class ElasticsearchUpsertTableSinkBase implements UpsertStreamTableSink<Row> {

	/** The property name for version of a record. */
	public static final String VERSION = "_version";

	/** The property name for routing of a record. */
	public static final String ROUTING = "_routing";

	/** The property name for index of a record. */
	public static final String INDEX = "_index";

	/** The property name for id of a record. */
	public static final String ID = "_id";

	/** The property name for opType of a record. */
	public static final String OP_TYPE = "_opType";

	/** The property name for source of a record. */
	public static final String SOURCE = "_source";

	/** Flag that indicates that only inserts are accepted. */
	private final boolean isAppendOnly;

	/** Schema of the table. */
	private final TableSchema schema;

	/** Version-agnostic hosts configuration. */
	private final List<Host> hosts;

	/** Default index for all requests. */
	private final String index;

	/** Default document type for all requests. */
	private final String docType;

	/** Delimiter for composite keys. */
	private final String keyDelimiter;

	/** String literal for null keys. */
	private final String keyNullLiteral;

	/** Serialization schema used for the document. */
	private final SerializationSchema<Row> serializationSchema;

	/** Content type describing the serialization schema. */
	private final XContentType contentType;

	/** Failure handler for failing {@link ActionRequest}s. */
	private final ActionRequestFailureHandler failureHandler;

	/**
	 * Map of optional configuration parameters for the Elasticsearch sink. The config is
	 * internal and can change at any time.
	 */
	private final Map<SinkOption, String> sinkOptions;

	/**
	 * Version-agnostic creation of {@link ActionRequest}s.
	 */
	private final RequestFactory requestFactory;

	/** Key field indices determined by the query. */
	private int[] keyFieldIndices;

	/** Whether to use ByteEs mode. */
	private final boolean byteEsMode;

	public ElasticsearchUpsertTableSinkBase(
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
			boolean byteEsMode) {

		this.isAppendOnly = isAppendOnly;
		this.schema = Preconditions.checkNotNull(schema);
		this.hosts = hosts;
		this.index = Preconditions.checkNotNull(index);
		this.keyDelimiter = Preconditions.checkNotNull(keyDelimiter);
		this.keyNullLiteral = Preconditions.checkNotNull(keyNullLiteral);
		this.docType = Preconditions.checkNotNull(docType);
		this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
		this.contentType = Preconditions.checkNotNull(contentType);
		this.failureHandler = Preconditions.checkNotNull(failureHandler);
		this.sinkOptions = Preconditions.checkNotNull(sinkOptions);
		this.requestFactory = Preconditions.checkNotNull(requestFactory);
		this.keyFieldIndices = keyFieldIndices;
		this.byteEsMode = byteEsMode;
	}

	public ElasticsearchUpsertTableSinkBase(
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
			requestFactory,
			keyFieldIndices,
			false
		);
	}

	@Override
	public void setKeyFields(String[] keyNames) {
		//user had defined the key fields indices
		if (this.keyFieldIndices.length > 0) {
			return;
		}

		if (keyNames == null) {
			this.keyFieldIndices = new int[0];
			return;
		}

		final String[] fieldNames = getFieldNames();
		final int[] keyFieldIndices = new int[keyNames.length];
		for (int i = 0; i < keyNames.length; i++) {
			keyFieldIndices[i] = -1;
			for (int j = 0; j < fieldNames.length; j++) {
				if (keyNames[i].equals(fieldNames[j])) {
					keyFieldIndices[i] = j;
					break;
				}
			}
			if (keyFieldIndices[i] == -1) {
				throw new RuntimeException("Invalid key fields: " + Arrays.toString(keyNames));
			}
		}

		validateKeyTypes(keyFieldIndices);

		this.keyFieldIndices = keyFieldIndices;
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		if (this.isAppendOnly && !isAppendOnly) {
			throw new ValidationException(
				"The given query is not supported by this sink because the sink is configured to " +
				"operate in append mode only. Thus, it only support insertions (no queries " +
				"with updating results).");
		}
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return schema.toRowType();
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		// get _index, _version, _routing, _id, _opType, _source index
		int indexIndex = -1;
		int versionIndex = -1;
		int routingIndex = -1;
		int idIndex = -1;
		int opTypeIndex = -1;
		int sourceIndex = -1;
		for (int i = 0; i < schema.getFieldCount(); ++i) {
			String fieldName = schema.getFieldName(i).orElseThrow(() -> new TableException("Won't get here."));
			if (fieldName.equalsIgnoreCase(VERSION)) {
				versionIndex = i;

				// Validate version field type. Can only be INT/BIGINT/SMALLINT/TINYINT.
				DataType dataType = schema.getFieldDataType(i).get();
				LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
				if (typeRoot == DECIMAL || !typeRoot.getFamilies().contains(LogicalTypeFamily.EXACT_NUMERIC)) {
					throw new TableException("_version field for ES sink should be integer types like int/bigint.");
				}
			} else if (fieldName.equalsIgnoreCase(ROUTING)) {
				routingIndex = i;

				// Validate routing field type. Can only be VARCHAR/CHAR.
				DataType dataType = schema.getFieldDataType(i).get();
				LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
				if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
					throw new TableException("_routing field for ES sink should be varchar/char type.");
				}
			} else if (fieldName.equalsIgnoreCase(INDEX)) {
				indexIndex = i;

				// Validate index field type Can only be VARCHAR/CHAR.
				DataType dataType = schema.getFieldDataType(i).get();
				LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
				if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
					throw new TableException("_index field for ES sink should be varchar/char type.");
				}
			} else if (fieldName.equalsIgnoreCase(ID)) {
				idIndex = i;

				// Validate index field type Can only be VARCHAR/CHAR.
				DataType dataType = schema.getFieldDataType(i).get();
				LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
				if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
					throw new TableException("_id field for ES sink should be varchar/char type.");
				}
			} else if (fieldName.equalsIgnoreCase(OP_TYPE)) {
				opTypeIndex = i;

				// Validate index field type Can only be VARCHAR/CHAR.
				DataType dataType = schema.getFieldDataType(i).get();
				LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
				if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
					throw new TableException("_opType field for ES sink should be varchar/char type.");
				}
			} else if (fieldName.equalsIgnoreCase(SOURCE)) {
				sourceIndex = i;

				// Validate index field type Can only be VARCHAR/CHAR.
				DataType dataType = schema.getFieldDataType(i).get();
				LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
				if (!typeRoot.getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
					throw new TableException("_source field for ES sink should be varchar/char type.");
				}
			}
		}

		final ElasticsearchUpsertSinkFunction upsertFunction =
			new ElasticsearchUpsertSinkFunction(
				index,
				docType,
				keyDelimiter,
				keyNullLiteral,
				serializationSchema,
				contentType,
				requestFactory,
				keyFieldIndices,
				versionIndex,
				routingIndex,
				indexIndex,
				idIndex,
				opTypeIndex,
				sourceIndex,
				byteEsMode);
		final SinkFunction<Tuple2<Boolean, Row>> sinkFunction = createSinkFunction(
			hosts,
			failureHandler,
			sinkOptions,
			upsertFunction);
		return dataStream.addSink(sinkFunction)
			.setParallelism(dataStream.getParallelism())
			.name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
		return Types.TUPLE(Types.BOOLEAN, getRecordType());
	}

	@Override
	public String[] getFieldNames() {
		return schema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return schema.getFieldTypes();
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
				"Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
				"But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		}
		return copy(
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
			requestFactory,
			keyFieldIndices,
			byteEsMode);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ElasticsearchUpsertTableSinkBase that = (ElasticsearchUpsertTableSinkBase) o;
		return Objects.equals(isAppendOnly, that.isAppendOnly) &&
			Objects.equals(schema, that.schema) &&
			Objects.equals(hosts, that.hosts) &&
			Objects.equals(index, that.index) &&
			Objects.equals(docType, that.docType) &&
			Objects.equals(keyDelimiter, that.keyDelimiter) &&
			Objects.equals(keyNullLiteral, that.keyNullLiteral) &&
			Objects.equals(serializationSchema, that.serializationSchema) &&
			Objects.equals(contentType, that.contentType) &&
			Objects.equals(failureHandler, that.failureHandler) &&
			Objects.equals(sinkOptions, that.sinkOptions);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
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
			sinkOptions);
	}

	// --------------------------------------------------------------------------------------------
	// For version-specific implementations
	// --------------------------------------------------------------------------------------------

	protected abstract ElasticsearchUpsertTableSinkBase copy(
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
		boolean byteEsMode);

	protected abstract SinkFunction<Tuple2<Boolean, Row>> createSinkFunction(
		List<Host> hosts,
		ActionRequestFailureHandler failureHandler,
		Map<SinkOption, String> sinkOptions,
		ElasticsearchUpsertSinkFunction upsertFunction);

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Validate the types that are used for conversion to string.
	 */
	private void validateKeyTypes(int[] keyFieldIndices) {
		final TypeInformation<?>[] types = getFieldTypes();
		for (int keyFieldIndex : keyFieldIndices) {
			final TypeInformation<?> type = types[keyFieldIndex];
			if (!TypeCheckUtils.isSimpleStringRepresentation(type)) {
				throw new ValidationException(
					"Only simple types that can be safely converted into a string representation " +
						"can be used as keys. But was: " + type);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	/**
	 * Keys for optional parameterization of the sink.
	 */
	public enum SinkOption {
		DISABLE_FLUSH_ON_CHECKPOINT,
		BULK_FLUSH_MAX_ACTIONS,
		BULK_FLUSH_MAX_SIZE,
		BULK_FLUSH_INTERVAL,
		BULK_FLUSH_BACKOFF_ENABLED,
		BULK_FLUSH_BACKOFF_TYPE,
		BULK_FLUSH_BACKOFF_RETRIES,
		BULK_FLUSH_BACKOFF_DELAY,
		REST_MAX_RETRY_TIMEOUT,
		REST_PATH_PREFIX,

		/**
		 * support bytedance consul.
		 */
		CONSUL,
		HTTP_SCHEMA,

		/**
		 * support password config.
		 */
		ENABLE_PASSWORD_CONFIG,
		USERNAME,
		PASSWORD,

		/**
		 * support to lookup es hosts via psm.
		 */
		URI
	}

	/**
	 * Entity for describing a host of Elasticsearch.
	 */
	public static class Host {
		public final String hostname;
		public final int port;
		public final String protocol;

		public Host(String hostname, int port, String protocol) {
			this.hostname = hostname;
			this.port = port;
			this.protocol = protocol;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Host host = (Host) o;
			return port == host.port &&
				Objects.equals(hostname, host.hostname) &&
				Objects.equals(protocol, host.protocol);
		}

		@Override
		public int hashCode() {
			return Objects.hash(
				hostname,
				port,
				protocol);
		}
	}

	/**
	 * For version-agnostic creating of {@link ActionRequest}s.
	 */
	public interface RequestFactory extends Serializable {

		/**
		 * Creates an update request to be added to a {@link RequestIndexer}.
		 * Note: the type field has been deprecated since Elasticsearch 7.x and it would not take any effort.
		 */
		UpdateRequest createUpdateRequest(
			String index,
			String docType,
			String key,
			XContentType contentType,
			byte[] document);

		/**
		 * Creates an index request to be added to a {@link RequestIndexer}.
		 * Note: the type field has been deprecated since Elasticsearch 7.x and it would not take any effort.
		 */
		IndexRequest createIndexRequest(
			String index,
			String docType,
			XContentType contentType,
			byte[] document);

		/**
		 * Creates a delete request to be added to a {@link RequestIndexer}.
		 * Note: the type field has been deprecated since Elasticsearch 7.x and it would not take any effort.
		 */
		DeleteRequest createDeleteRequest(
			String index,
			String docType,
			String key);

		/**
		 * Custom request, used for ByteES.
		 */
		default void addCustomRequestToIndexer(
				String doc,
				RequestIndexer indexer,
				long version,
				String routing,
				String index,
				String id,
				String opType) {
			throw new UnsupportedOperationException("Need to be implemented.");
		}
	}

	/**
	 * Sink function for converting upserts into Elasticsearch {@link ActionRequest}s.
	 */
	public static class ElasticsearchUpsertSinkFunction implements ElasticsearchSinkFunction<Tuple2<Boolean, Row>> {

		private static final long serialVersionUID = 1L;

		private final String index;
		private final String docType;
		private final String keyDelimiter;
		private final String keyNullLiteral;
		private final SerializationSchema<Row> serializationSchema;
		private final XContentType contentType;
		private final RequestFactory requestFactory;
		private final int[] keyFieldIndices;
		private final int versionIndex;
		private final int routingIndex;
		private final int indexIndex;
		private final int idIndex;
		private final int opTypeIndex;
		private final int sourceIndex;
		private final boolean byteEsMode;

		public ElasticsearchUpsertSinkFunction(
				String index,
				String docType,
				String keyDelimiter,
				String keyNullLiteral,
				SerializationSchema<Row> serializationSchema,
				XContentType contentType,
				RequestFactory requestFactory,
				int[] keyFieldIndices,
				int versionIndex,
				int routingIndex,
				int indexIndex,
				int idIndex,
				int opTypeIndex,
				int sourceIndex,
				boolean byteEsMode) {

			this.index = Preconditions.checkNotNull(index);
			this.docType = Preconditions.checkNotNull(docType);
			this.keyDelimiter = Preconditions.checkNotNull(keyDelimiter);
			this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
			this.contentType = Preconditions.checkNotNull(contentType);
			this.keyFieldIndices = Preconditions.checkNotNull(keyFieldIndices);
			this.requestFactory = Preconditions.checkNotNull(requestFactory);
			this.keyNullLiteral = Preconditions.checkNotNull(keyNullLiteral);
			this.versionIndex = versionIndex;
			this.routingIndex = routingIndex;
			this.indexIndex = indexIndex;
			this.idIndex = idIndex;
			this.opTypeIndex = opTypeIndex;
			this.sourceIndex = sourceIndex;
			this.byteEsMode = byteEsMode;
		}

		@Override
		public void process(Tuple2<Boolean, Row> element, RuntimeContext ctx, RequestIndexer indexer) {
			if (byteEsMode) {
				final Row record = element.f1;
				long version = -1;
				String routing = null;
				String index = null;
				String id = null;
				String opType = null;
				String source = null;

				for (int i = 0; i < record.getArity(); ++i) {
					if (i == versionIndex) {
						Object versionColumn = record.getField(i);
						if (versionColumn != null) {
							version = Long.parseLong(versionColumn.toString());
						}
					} else if (i == routingIndex) {
						routing = (String) record.getField(i);
					} else if (i == indexIndex) {
						index = (String) record.getField(i);
					} else if (i == idIndex) {
						id = (String) record.getField(i);
					} else if (i == opTypeIndex) {
						opType = (String) record.getField(i);
					} else if (i == sourceIndex) {
						source = (String) record.getField(i);
					}
				}

				requestFactory.addCustomRequestToIndexer(source, indexer, version, routing, index, id, opType);
			} else {
				if (element.f0) {
					processUpsert(element.f1, indexer);
				} else {
					processDelete(element.f1, indexer);
				}
			}
		}

		private void processUpsert(Row row, RequestIndexer indexer) {
			final byte[] document = serializationSchema.serialize(row);
			if (keyFieldIndices.length == 0) {
				final IndexRequest indexRequest = requestFactory.createIndexRequest(
					index,
					docType,
					contentType,
					document);
				indexer.add(indexRequest);
			} else {
				final String key = createKey(row);
				final UpdateRequest updateRequest = requestFactory.createUpdateRequest(
					index,
					docType,
					key,
					contentType,
					document);
				indexer.add(updateRequest);
			}
		}

		private void processDelete(Row row, RequestIndexer indexer) {
			final String key = createKey(row);
			final DeleteRequest deleteRequest = requestFactory.createDeleteRequest(
				index,
				docType,
				key);
			indexer.add(deleteRequest);
		}

		private String createKey(Row row) {
			final StringBuilder builder = new StringBuilder();
			for (int i = 0; i < keyFieldIndices.length; i++) {
				final int keyFieldIndex = keyFieldIndices[i];
				if (i > 0) {
					builder.append(keyDelimiter);
				}
				final Object value = row.getField(keyFieldIndex);
				if (value == null) {
					builder.append(keyNullLiteral);
				} else {
					builder.append(value.toString());
				}
			}
			return builder.toString();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ElasticsearchUpsertSinkFunction that = (ElasticsearchUpsertSinkFunction) o;
			return Objects.equals(index, that.index) &&
				Objects.equals(docType, that.docType) &&
				Objects.equals(keyDelimiter, that.keyDelimiter) &&
				Objects.equals(keyNullLiteral, that.keyNullLiteral) &&
				Objects.equals(serializationSchema, that.serializationSchema) &&
				contentType == that.contentType &&
				Objects.equals(requestFactory, that.requestFactory) &&
				Arrays.equals(keyFieldIndices, that.keyFieldIndices);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(
				index,
				docType,
				keyDelimiter,
				keyNullLiteral,
				serializationSchema,
				contentType,
				requestFactory);
			result = 31 * result + Arrays.hashCode(keyFieldIndices);
			return result;
		}
	}
}
