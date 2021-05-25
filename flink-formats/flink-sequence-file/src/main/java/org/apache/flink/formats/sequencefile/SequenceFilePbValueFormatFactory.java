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

package org.apache.flink.formats.sequencefile;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapreduce.wrapper.HadoopInputSplit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.formats.pb.PbFormatFactory;
import org.apache.flink.formats.pb.PbRowDataDeserializationSchema;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.TableSchemaInferrable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.formats.pb.PbOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.pb.PbOptions.IS_AD_INSTANCE_FORMAT;
import static org.apache.flink.formats.pb.PbOptions.PB_CLASS;
import static org.apache.flink.formats.pb.PbOptions.SINK_WITH_SIZE_HEADER;
import static org.apache.flink.formats.pb.PbOptions.SKIP_BYTES;
import static org.apache.flink.formats.pb.PbOptions.WITH_WRAPPER;

/**
 * FormatFactory for sequence file.
 * 1. ignore the key.
 * 2. value is pb bytes, we parse the pb bytes to pb object and convert it to a Row.
 */
public class SequenceFilePbValueFormatFactory implements FileSystemFormatFactory, TableSchemaInferrable {

	public static final String IDENTIFIER = "sequence-file-pb-value";

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		String pbClass = context.getFormatOptions().get(PB_CLASS);
		int skipBytes = context.getFormatOptions().get(SKIP_BYTES);
		boolean withWrapper = context.getFormatOptions().get(WITH_WRAPPER);
		boolean ignoreParseErrors = context.getFormatOptions().get(IGNORE_PARSE_ERRORS);
		boolean isAdInstanceFormat = context.getFormatOptions().get(IS_AD_INSTANCE_FORMAT);

		PbRowDataDeserializationSchema pbRowDeserializationSchema =
			PbRowDataDeserializationSchema.builder()
				.setPbDescriptorClass(pbClass)
				.setRowType(context.getFormatRowType())
				.setResultTypeInfo(new GenericTypeInfo(GenericRowData.class))
				.setSkipBytes(skipBytes)
				.setWithWrapper(withWrapper)
				.setAdInstanceFormat(isAdInstanceFormat)
				.setIgnoreParseErrors(ignoreParseErrors)
				.build();
		return new SequenceFilePbValueInputFormat(pbRowDeserializationSchema, context, ignoreParseErrors);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PB_CLASS);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(SKIP_BYTES);
		options.add(WITH_WRAPPER);
		options.add(IGNORE_PARSE_ERRORS);
		options.add(SINK_WITH_SIZE_HEADER);
		options.add(IS_AD_INSTANCE_FORMAT);
		return options;
	}

	@Override
	public Optional<TableSchema> getOptionalTableSchema(Map<String, String> formatOptions) {
		String oldKey = fullKey(PB_CLASS.key());
		String pbClass = formatOptions.get(oldKey);
		Map<String, String> newFormatOptions = new HashMap<>(formatOptions);
		newFormatOptions.put("pb." + PB_CLASS.key(), pbClass);
		return new PbFormatFactory().getOptionalTableSchema(newFormatOptions);
	}

	@VisibleForTesting
	static String fullKey(String key) {
		return IDENTIFIER + "." + key;
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		return Optional.empty();
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		return Optional.empty();
	}

	static class SequenceFilePbValueInputFormat implements InputFormat<RowData, HadoopInputSplit> {
		private static final long serialVersionUID = 1L;

		private final HadoopInputFormat<BytesWritable, BytesWritable> hadoopInputFormat;
		private final Tuple2<BytesWritable, BytesWritable> reusableTuple2 = new Tuple2<>();
		private final PbRowDataDeserializationSchema pbRowDeserializationSchema;
		private final boolean ignoreParseErrors;
		private final int[] selectFields;

		public SequenceFilePbValueInputFormat(
				PbRowDataDeserializationSchema pbRowDeserializationSchema,
				ReaderContext context,
				boolean ignoreParseErrors) {
			this.pbRowDeserializationSchema = pbRowDeserializationSchema;
			this.ignoreParseErrors = ignoreParseErrors;
			this.selectFields = context.getProjectFields();
			// Only support one input path currently.
			String inputPath = context.getPaths()[0].toUri().toString();
			try {
				Job job = Job.getInstance();
				job.getConfiguration().set(FileInputFormat.INPUT_DIR, inputPath);
				hadoopInputFormat = new HadoopInputFormat<>(
					new SequenceFileInputFormat<>(), BytesWritable.class, BytesWritable.class, job);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to create HadoopInputFormat.", e);
			}
		}

		@Override
		public void configure(Configuration parameters) {
			hadoopInputFormat.configure(parameters);
		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
			return hadoopInputFormat.getStatistics(cachedStatistics);
		}

		@Override
		public HadoopInputSplit[] createInputSplits(int minNumSplits) throws IOException {
			return hadoopInputFormat.createInputSplits(minNumSplits);
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(HadoopInputSplit[] inputSplits) {
			return hadoopInputFormat.getInputSplitAssigner(inputSplits);
		}

		@Override
		public void open(HadoopInputSplit split) throws IOException {
			pbRowDeserializationSchema.open(UnregisteredMetricsGroup::new);
			hadoopInputFormat.open(split);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return hadoopInputFormat.reachedEnd();
		}

		@Override
		public RowData nextRecord(RowData reuse) throws IOException {
			try {
				Tuple2<BytesWritable, BytesWritable> tuple2 = hadoopInputFormat.nextRecord(reusableTuple2);
				BytesWritable value = tuple2.f1;
				if (value == null) {
					return null;
				}

				// We must call copyBytes() rather than getBytes(), as returned byte[] of the latter
				// one may filled with '0'.
				GenericRowData originRow =
					(GenericRowData) pbRowDeserializationSchema.deserialize(value.copyBytes());

				return getSelectedFields(originRow, selectFields);
			} catch (Throwable t) {
				if (ignoreParseErrors) {
					return null;
				}
				throw new IOException("Failed to read data from sequence file.", t);
			}
		}

		private static RowData getSelectedFields(GenericRowData originRow, int[] selectFields) {
			if (originRow == null) {
				return null;
			}
			if (selectFields == null || selectFields.length == 0) {
				return originRow;
			}

			GenericRowData selectedRow = new GenericRowData(selectFields.length);
			for (int i = 0; i < selectFields.length; i++) {
				selectedRow.setField(i, originRow.getField(selectFields[i]));
			}
			return selectedRow;
		}

		@Override
		public void close() throws IOException {
			hadoopInputFormat.close();
		}

		@Override
		public boolean takeNullAsEndOfStream() {
			return false;
		}
	}
}
