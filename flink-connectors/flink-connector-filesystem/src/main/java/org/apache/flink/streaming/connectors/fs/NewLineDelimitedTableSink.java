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
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;

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

	private String fieldDelimiter;

	public NewLineDelimitedTableSink(
		String path,
		TableSchema schema,
		int numFiles,
		FileSystem.WriteMode writeMode,
		SerializationSchema<Row> serializationSchema,
		String codec,
		String fieldDelimiter) {
		this.path = path;
		this.schema = schema;
		this.numFiles = numFiles;
		this.writeMode = writeMode;
		this.serializationSchema = serializationSchema;
		this.codec = codec;
		this.fieldDelimiter = fieldDelimiter;
	}

	public NewLineDelimitedTableSink(
		String path,
		TableSchema schema,
		SerializationSchema<Row> serializationSchema,
		String codec,
		String fieldDelimiter) {
		this(path, schema, -1, null, serializationSchema, codec, fieldDelimiter);
	}

	public NewLineDelimitedTableSink(
		String path,
		TableSchema schema,
		SerializationSchema<Row> serializationSchema,
		String fieldDelimiter) {
		this(path, schema, serializationSchema, null, fieldDelimiter);
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
		TextWithHeaderOutputFormat textWithHeaderOutputFormat = new TextWithHeaderOutputFormat(new Path(path), schema.getFieldNames(), fieldDelimiter);
		if (writeMode != null) {
			textWithHeaderOutputFormat.setWriteMode(writeMode);
			sink = lineRows.writeUsingOutputFormat(textWithHeaderOutputFormat);
		} else {
			sink = lineRows.writeUsingOutputFormat(textWithHeaderOutputFormat);
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
		NewLineDelimitedTableSink configuredSink = new NewLineDelimitedTableSink(path, schema, numFiles, writeMode, serializationSchema, null, fieldDelimiter);
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

	/**
	 * TextWithHeaderOutputFormat.
	 */
	public static class TextWithHeaderOutputFormat<T> extends FileOutputFormat<T> {

		private static final long serialVersionUID = 1L;

		private static final int NEWLINE = '\n';

		private String charsetName;

		private transient Charset charset;

		private String[] fieldNames;

		private String fieldDelimiter;

		public TextWithHeaderOutputFormat(Path outputPath, String[] fieldNames, String fieldDelimiter) {
			this(outputPath, "UTF-8", fieldNames, fieldDelimiter);
		}

		public TextWithHeaderOutputFormat(Path outputPath) {
			this(outputPath, "UTF-8", null, null);
		}

		public TextWithHeaderOutputFormat(Path outputPath, String charset, String[] fieldNames, String fieldDelimiter) {
			super(outputPath);
			this.charsetName = charset;
			this.fieldNames = fieldNames;
			this.fieldDelimiter = fieldDelimiter;
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			super.open(taskNumber, numTasks);

			try {
				this.charset = Charset.forName(charsetName);
			} catch (IllegalCharsetNameException e) {
				throw new IOException("The charset " + charsetName + " is not valid.", e);
			} catch (UnsupportedCharsetException e) {
				throw new IOException("The charset " + charsetName + " is not supported.", e);
			}

			if (fieldDelimiter != null && fieldNames != null) {
				String header = Arrays.stream(fieldNames).collect(Collectors.joining(fieldDelimiter.toString()));
				byte[] bytes = header.getBytes(charset);
				this.stream.write(bytes);
				this.stream.write(NEWLINE);
			}
		}

		@Override
		public void writeRecord(T record) throws IOException {
			byte[] bytes = record.toString().getBytes(charset);
			this.stream.write(bytes);
			this.stream.write(NEWLINE);
		}

		// --------------------------------------------------------------------------------------------

		@Override
		public String toString() {
			return "TextWithHeaderOutputFormat (" + getOutputFilePath() + ") - " + this.charsetName;
		}
	}
}
