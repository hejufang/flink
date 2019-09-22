/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.Base64;

/**
 * newline delimited table sink.
 */
public class NewLineDelimitedTableSink implements AppendStreamTableSink<Row> {

	public static final String BASE64_CODEC = "base64";

	private final TableSchema schema;

	private String path;
	private int numFiles = -1;
	private FileSystem.WriteMode writeMode;

	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	private SerializationSchema<Row> serializationSchema;

	// 每行数据压缩格式, 如base64
	private String codec;

	public NewLineDelimitedTableSink(
		String path,
		TableSchema schema,
		int numFiles,
		FileSystem.WriteMode writeMode,
		SerializationSchema<Row> serializationSchema,
		String codec) {
		this.path = path;
		this.schema = schema;
		this.numFiles = numFiles;
		this.writeMode = writeMode;
		this.serializationSchema = serializationSchema;
		this.codec = codec;
	}

	public NewLineDelimitedTableSink(
		String path,
		TableSchema schema,
		SerializationSchema<Row> serializationSchema,
		String codec) {
		this(path, schema, -1, null, serializationSchema, codec);
	}

	public NewLineDelimitedTableSink(
		String path,
		TableSchema schema,
		SerializationSchema<Row> serializationSchema) {
		this(path, schema, serializationSchema, null);
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		SingleOutputStreamOperator<String> lineRows =
			dataStream.map(new LineFormatter(serializationSchema, codec));

		DataStreamSink<String> sink;
		if (writeMode != null) {
			sink = lineRows.writeAsText(path, writeMode);
		} else {
			sink = lineRows.writeAsText(path);
		}

		int parallelism = numFiles > 0 ? numFiles : dataStream.getParallelism();
		lineRows.setParallelism(parallelism);
		sink.setParallelism(parallelism);

		sink.name(TableConnectorUtils.generateRuntimeName(NewLineDelimitedTableSink.class, fieldNames));

		return sink;
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		NewLineDelimitedTableSink configuredSink = new NewLineDelimitedTableSink(path, schema, numFiles, writeMode, serializationSchema, null);
		configuredSink.fieldNames = fieldNames;
		configuredSink.fieldTypes = fieldTypes;
		return configuredSink;
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(getFieldTypes(), getFieldNames());
	}

	@Override
	public String[] getFieldNames() {
		return schema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return schema.getFieldTypes();
	}

	/**
	 * Format a Row into a String by serializationSchema and codec.
	 */
	public static class LineFormatter implements MapFunction<Row, String> {
		private static final long serialVersionUID = 1L;

		private SerializationSchema<Row> serializationSchema;

		private String codec;

		LineFormatter(SerializationSchema<Row> serializationSchema, String codec) {
			this.serializationSchema = serializationSchema;
			this.codec = codec;
		}

		@Override
		public String map(Row row) {
			if (null == serializationSchema) {
				throw new RuntimeException("serializationSchema can not be null");
			}

			byte[] bytes = serializationSchema.serialize(row);

			if (null == codec) {
				// CsvRowSerializationSchema序列化的结果自带换行符，需要去掉
				return new String(bytes).trim();
			}

			switch (codec) {
				case BASE64_CODEC:
					String msg = Base64.getEncoder().encodeToString(bytes);
					return msg;
				default:
					throw new RuntimeException("Unsupported codec: " + codec);
			}
		}
	}
}
