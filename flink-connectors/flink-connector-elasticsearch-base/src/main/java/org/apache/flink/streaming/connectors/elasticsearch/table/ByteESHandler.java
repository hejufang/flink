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

import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * A special handler for ByteES, it could extract some config from record, e.t. index/routing/version/id...
 */
public class ByteESHandler implements Serializable {
	public static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ByteESHandler.class);

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

	private final RowData.FieldGetter versionGetter;
	private final int routingIndex;
	private final int indexIndex;
	private final int idIndex;
	private final int opTypeIndex;
	private final int sourceIndex;
	private final boolean ignoreInvalidData;

	private ByteESHandler(
			RowData.FieldGetter versionGetter,
			int routingIndex,
			int indexIndex,
			int idIndex,
			int opTypeIndex,
			int sourceIndex,
			boolean ignoreInvalidData) {
		this.versionGetter = versionGetter;
		this.routingIndex = routingIndex;
		this.indexIndex = indexIndex;
		this.idIndex = idIndex;
		this.opTypeIndex = opTypeIndex;
		this.sourceIndex = sourceIndex;
		this.ignoreInvalidData = ignoreInvalidData;
	}

	public void handle(RowData row, RequestIndexer indexer, RequestFactory requestFactory) {
		long version = -1;
		if (versionGetter != null) {
			Object v = versionGetter.getFieldOrNull(row);
			if (v != null) {
				version = Long.parseLong(v.toString());
			}
		}
		String routing = getStringField(row, routingIndex);
		String index = getStringField(row, indexIndex);
		String id = getStringField(row, idIndex);
		String opType = getStringField(row, opTypeIndex);
		String source = getStringField(row, sourceIndex);

		if (index == null || opType == null) {
			if (ignoreInvalidData) {
				LOG.error("Receive invalid data, doc id {}, opType {}, indexer {}", id, opType, index);
			} else {
				throw new RuntimeException(String.format(
					"Invalid data find, doc id %s, opType %s, indexer %s", id, opType, index));
			}
		}
		requestFactory.addCustomRequestToIndexer(source, indexer, version, routing, index, id, opType);
	}

	private String getStringField(RowData row, int index) {
		if (row.isNullAt(index)) {
			return null;
		} else {
			return row.getString(index).toString();
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link ByteESHandler}.
	 */
	public static class Builder {

		private RowData.FieldGetter versionGetter = null;
		private int routingIndex = -1;
		private int indexIndex = -1;
		private int idIndex = -1;
		private int opTypeIndex = -1;
		private int sourceIndex = -1;
		private boolean ignoreInvalidData = false;

		private Builder() {
			// private constructor.
		}

		public Builder setVersionGetter(RowData.FieldGetter versionGetter) {
			this.versionGetter = versionGetter;
			return this;
		}

		public Builder setRoutingIndex(int routingIndex) {
			this.routingIndex = routingIndex;
			return this;
		}

		public Builder setIndexIndex(int indexIndex) {
			this.indexIndex = indexIndex;
			return this;
		}

		public Builder setIdIndex(int idIndex) {
			this.idIndex = idIndex;
			return this;
		}

		public Builder setOpTypeIndex(int opTypeIndex) {
			this.opTypeIndex = opTypeIndex;
			return this;
		}

		public Builder setSourceIndex(int sourceIndex) {
			this.sourceIndex = sourceIndex;
			return this;
		}

		public Builder setIgnoreInvalidData(boolean ignoreInvalidData) {
			this.ignoreInvalidData = ignoreInvalidData;
			return this;
		}

		public ByteESHandler build() {
			return new ByteESHandler(
				versionGetter,
				routingIndex,
				indexIndex,
				idIndex,
				opTypeIndex,
				sourceIndex,
				ignoreInvalidData);
		}
	}

}
