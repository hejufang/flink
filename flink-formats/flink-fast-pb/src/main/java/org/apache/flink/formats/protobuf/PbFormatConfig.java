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

package org.apache.flink.formats.protobuf;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.formats.protobuf.PbFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.protobuf.PbFormatOptions.IS_AD_INSTANCE_FORMAT;
import static org.apache.flink.formats.protobuf.PbFormatOptions.READ_DEFAULT_VALUES;
import static org.apache.flink.formats.protobuf.PbFormatOptions.SINK_WITH_SIZE_HEADER;
import static org.apache.flink.formats.protobuf.PbFormatOptions.SIZE_HEADER_WITH_LITTLE_ENDIAN;
import static org.apache.flink.formats.protobuf.PbFormatOptions.SKIP_BYTES;
import static org.apache.flink.formats.protobuf.PbFormatOptions.WITH_WRAPPER;
import static org.apache.flink.formats.protobuf.PbFormatOptions.WRITE_NULL_STRING_LITERAL;

/**
 * Config of protobuf configs.
 */
public class PbFormatConfig implements Serializable {

	private String pbDescriptorClass;
	private boolean ignoreParseErrors;
	private boolean readDefaultValues;
	private String writeNullStringLiterals;
	private boolean withWrapper;
	private int skipBytes;
	private boolean isAdInstanceFormat;
	private boolean sinkWithSizeHeader;
	private boolean sizeHeaderWithLittleEndian;

	public PbFormatConfig(
			String pbDescriptorClass,
			boolean ignoreParseErrors,
			boolean readDefaultValues,
			String writeNullStringLiterals,
			boolean withWrapper,
			int skipBytes,
			boolean isAdInstanceFormat,
			boolean sinkWithSizeHeader,
			boolean sizeHeaderWithLittleEndian) {
		this.pbDescriptorClass = pbDescriptorClass;
		this.ignoreParseErrors = ignoreParseErrors;
		this.readDefaultValues = readDefaultValues;
		this.writeNullStringLiterals = writeNullStringLiterals;
		this.withWrapper = withWrapper;
		this.skipBytes = skipBytes;
		this.isAdInstanceFormat = isAdInstanceFormat;
		this.sinkWithSizeHeader = sinkWithSizeHeader;
		this.sizeHeaderWithLittleEndian = sizeHeaderWithLittleEndian;
	}

	public String getPbDescriptorClass() {
		return pbDescriptorClass;
	}

	public boolean isIgnoreParseErrors() {
		return ignoreParseErrors;
	}

	public boolean isReadDefaultValues() {
		return readDefaultValues;
	}

	public String getWriteNullStringLiterals() {
		return writeNullStringLiterals;
	}

	public boolean isWithWrapper() {
		return withWrapper;
	}

	public int getSkipBytes() {
		return skipBytes;
	}

	public boolean isAdInstanceFormat() {
		return isAdInstanceFormat;
	}

	public boolean isSinkWithSizeHeader() {
		return sinkWithSizeHeader;
	}

	public boolean isSizeHeaderWithLittleEndian() {
		return sizeHeaderWithLittleEndian;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		PbFormatConfig that = (PbFormatConfig) o;
		return ignoreParseErrors == that.ignoreParseErrors
			&& readDefaultValues == that.readDefaultValues
			&& Objects.equals(pbDescriptorClass, that.pbDescriptorClass)
			&& Objects.equals(writeNullStringLiterals, that.writeNullStringLiterals)
			&& withWrapper == that.withWrapper
			&& skipBytes == that.skipBytes
			&& isAdInstanceFormat == that.isAdInstanceFormat
			&& sinkWithSizeHeader == that.sinkWithSizeHeader
			&& sizeHeaderWithLittleEndian == that.sizeHeaderWithLittleEndian;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			pbDescriptorClass, ignoreParseErrors, readDefaultValues, writeNullStringLiterals);
	}

	/**
	 * Builder of PbFormatConfig.
	 */
	public static class PbFormatConfigBuilder {

		private String pbDescriptorClass;
		private boolean ignoreParseErrors = IGNORE_PARSE_ERRORS.defaultValue();
		private boolean readDefaultValues = READ_DEFAULT_VALUES.defaultValue();
		private String writeNullStringLiterals = WRITE_NULL_STRING_LITERAL.defaultValue();
		private boolean withWrapper = WITH_WRAPPER.defaultValue();
		private int skipBytes = SKIP_BYTES.defaultValue();
		private boolean isAdInstanceFormat = IS_AD_INSTANCE_FORMAT.defaultValue();
		private boolean sinkWithSizeHeader = SINK_WITH_SIZE_HEADER.defaultValue();
		private boolean sizeHeaderWithLittleEndian = SIZE_HEADER_WITH_LITTLE_ENDIAN.defaultValue();

		public PbFormatConfigBuilder pbDescriptorClass(String messageClassName) {
			this.pbDescriptorClass = messageClassName;
			return this;
		}

		public PbFormatConfigBuilder ignoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public PbFormatConfigBuilder readDefaultValues(boolean readDefaultValues) {
			this.readDefaultValues = readDefaultValues;
			return this;
		}

		public PbFormatConfigBuilder writeNullStringLiterals(String writeNullStringLiterals) {
			this.writeNullStringLiterals = writeNullStringLiterals;
			return this;
		}

		public PbFormatConfigBuilder withWrapper(boolean withWrapper) {
			this.withWrapper = withWrapper;
			return this;
		}

		public PbFormatConfigBuilder skipBytes(int skipBytes) {
			this.skipBytes = skipBytes;
			return this;
		}

		public PbFormatConfigBuilder isAdInstanceFormat(boolean isAdInstanceFormat) {
			this.isAdInstanceFormat = isAdInstanceFormat;
			return this;
		}

		public PbFormatConfigBuilder sinkWithSizeHeader(boolean sinkWithSizeHeader) {
			this.sinkWithSizeHeader = sinkWithSizeHeader;
			return this;
		}

		public PbFormatConfigBuilder sizeHeaderWithLittleEndian(boolean sizeHeaderWithLittleEndian) {
			this.sizeHeaderWithLittleEndian = sizeHeaderWithLittleEndian;
			return this;
		}

		public PbFormatConfig build() {
			return new PbFormatConfig(
				pbDescriptorClass,
				ignoreParseErrors,
				readDefaultValues,
				writeNullStringLiterals,
				withWrapper,
				skipBytes,
				isAdInstanceFormat,
				sinkWithSizeHeader,
				sizeHeaderWithLittleEndian);
		}
	}
}
