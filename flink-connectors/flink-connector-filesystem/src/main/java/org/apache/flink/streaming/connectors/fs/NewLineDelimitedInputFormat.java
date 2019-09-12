/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Base64;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * newLine delimited input format.
 */
public class NewLineDelimitedInputFormat<OUT> extends DelimitedInputFormat<OUT> {
	public static final String DEFAULT_CODEC = "none";
	public static final String BASE64_CODEC = "base64";

	public static final ConfigOption<String> CONF_INPUT_FORMAT_COMPRESS_CODEC =
		key("fileinputformat.compress.codec")
		.defaultValue(DEFAULT_CODEC);


	private Configuration configuration;
	private DeserializationSchema<Row> deserializationSchema;

	public NewLineDelimitedInputFormat(Path filePath, Configuration configuration, DeserializationSchema<Row> deserializationSchema) {
		super(filePath, configuration);
		this.configuration = configuration;
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public OUT readRecord(OUT reuse, byte[] bytes, int offset, int numBytes) throws IOException {
		byte[] msg = new byte[numBytes];
		System.arraycopy(bytes, offset, msg, 0, numBytes);
		String codec = configuration.getString(CONF_INPUT_FORMAT_COMPRESS_CODEC);

		OUT row;
		if (codec.equals(DEFAULT_CODEC)) {
			row = (OUT) deserializationSchema.deserialize(msg);
		} else if (codec.equals(BASE64_CODEC)) {
			msg = Base64.getDecoder().decode(msg);
			row = (OUT) deserializationSchema.deserialize(msg);
		} else {
			throw new RuntimeException("Not supported compress codec: " + codec);
		}
		return row;
	}

}
