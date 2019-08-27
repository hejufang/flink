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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.table.utils.ParameterEntity;
import org.apache.flink.table.utils.ParameterParseUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_PARAMETERS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED;

/**
 * Validator for {@link Rowtime}.
 */
@PublicEvolving
public class RowtimeValidator implements DescriptorValidator {
	private static final Logger LOG = LoggerFactory.getLogger(RowtimeValidator.class);

	private final boolean supportsSourceTimestamps;
	private final boolean supportsSourceWatermarks;
	private final String prefix;

	public RowtimeValidator(boolean supportsSourceTimestamps, boolean supportsSourceWatermarks) {
		this(supportsSourceTimestamps, supportsSourceWatermarks, "");
	}

	public RowtimeValidator(boolean supportsSourceTimestamps, boolean supportsSourceWatermarks, String prefix) {
		this.supportsSourceTimestamps = supportsSourceTimestamps;
		this.supportsSourceWatermarks = supportsSourceWatermarks;
		this.prefix = prefix;
	}

	@Override
	public void validate(DescriptorProperties properties) {
		Consumer<String> timestampExistingField =
				s -> properties.validateString(prefix + ROWTIME_TIMESTAMPS_FROM, false, 1);

		Consumer<String> timestampCustom = s -> {
			properties.validateString(prefix + ROWTIME_TIMESTAMPS_CLASS, false, 1);
			properties.validateString(prefix + ROWTIME_TIMESTAMPS_SERIALIZED, false, 1);
		};

		Map<String, Consumer<String>> timestampsValidation = new HashMap<>();
		if (supportsSourceTimestamps) {
			timestampsValidation.put(ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD, timestampExistingField);
			timestampsValidation.put(ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE, DescriptorProperties.noValidation());
			timestampsValidation.put(ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM, timestampCustom);
		} else {
			timestampsValidation.put(ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD, timestampExistingField);
			timestampsValidation.put(ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM, timestampCustom);
		}

		properties.validateEnum(prefix + ROWTIME_TIMESTAMPS_TYPE, false, timestampsValidation);

		Consumer<String> watermarkPeriodicBounded =
				s -> properties.validateLong(prefix + ROWTIME_WATERMARKS_DELAY, false, 0);

		Consumer<String> watermarkCustom = s -> {
			properties.validateString(prefix + ROWTIME_WATERMARKS_CLASS, false, 1);
			properties.validateString(prefix + ROWTIME_WATERMARKS_SERIALIZED, true, 1);
			properties.validateString(prefix + ROWTIME_WATERMARKS_PARAMETERS, true, 1);
		};

		Map<String, Consumer<String>> watermarksValidation = new HashMap<>();
		if (supportsSourceWatermarks) {
			watermarksValidation.put(ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING, DescriptorProperties.noValidation());
			watermarksValidation.put(ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED, watermarkPeriodicBounded);
			watermarksValidation.put(ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE, DescriptorProperties.noValidation());
			watermarksValidation.put(ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM, watermarkCustom);
		} else {
			watermarksValidation.put(ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING, DescriptorProperties.noValidation());
			watermarksValidation.put(ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED, watermarkPeriodicBounded);
			watermarksValidation.put(ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM, watermarkCustom);
		}

		properties.validateEnum(prefix + ROWTIME_WATERMARKS_TYPE, false, watermarksValidation);
	}

	// utilities

	public static Optional<Tuple2<TimestampExtractor, WatermarkStrategy>> getRowtimeComponents(
			DescriptorProperties properties, String prefix) {
		// create timestamp extractor
		TimestampExtractor extractor;
		Optional<String> t = properties.getOptionalString(prefix + ROWTIME_TIMESTAMPS_TYPE);
		if (!t.isPresent()) {
			return Optional.empty();
		}

		switch (t.get()) {
			case ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD:
				String field = properties.getString(prefix + ROWTIME_TIMESTAMPS_FROM);
				extractor = new ExistingField(field);
				break;
			case ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE:
				extractor = StreamRecordTimestamp.INSTANCE;
				break;
			case ROWTIME_TIMESTAMPS_TYPE_VALUE_CUSTOM:
				Class<TimestampExtractor> clazz = properties.getClass(
						prefix + ROWTIME_TIMESTAMPS_CLASS, TimestampExtractor.class);
				extractor = EncodingUtils.decodeStringToObject(
						properties.getString(prefix + ROWTIME_TIMESTAMPS_SERIALIZED),
						clazz);
				break;
			default:
				throw new ValidationException("Unsupported rowtime timestamps type: " + t.get());
		}

		// create watermark strategy
		WatermarkStrategy strategy;
		String s = properties.getString(prefix + ROWTIME_WATERMARKS_TYPE);
		switch (s) {
			case ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_ASCENDING:
				strategy = new AscendingTimestamps();
				break;
			case ROWTIME_WATERMARKS_TYPE_VALUE_PERIODIC_BOUNDED:
				long delay = properties.getLong(prefix + ROWTIME_WATERMARKS_DELAY);
				strategy = new BoundedOutOfOrderTimestamps(delay);
				break;
			case ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE:
				strategy = PreserveWatermarks.INSTANCE;
				break;
			case ROWTIME_WATERMARKS_TYPE_VALUE_CUSTOM:
				Class<WatermarkStrategy> clazz = properties.getClass(
						prefix + ROWTIME_WATERMARKS_CLASS, WatermarkStrategy.class);
				Optional<String> watermarkSerialized =
					properties.getOptionalString(prefix + ROWTIME_WATERMARKS_SERIALIZED);
				Optional<String> watermarkParameters =
					properties.getOptionalString(prefix + ROWTIME_WATERMARKS_PARAMETERS);
				if (watermarkSerialized.isPresent()) {
					// 1.Config watermark strategy with serialized object.
					strategy = EncodingUtils.decodeStringToObject(watermarkSerialized.get(), clazz);
				} else if (watermarkParameters.isPresent()) {
					// 2.Config watermark strategy with class and constructor parameters.
					String parameterStr = watermarkParameters.get();
					try {
						ParameterEntity parameterEntry = ParameterParseUtils.parse(parameterStr);
						List<Class> paramClassList = parameterEntry.getParamClassList();
						List<Object> parsedParams = parameterEntry.getParsedParams();
						Constructor constructor = clazz.getDeclaredConstructor(
							paramClassList.toArray(new Class[paramClassList.size()]));
						strategy =
							(WatermarkStrategy) constructor.newInstance(parsedParams.toArray());
					} catch (Exception e) {
						LOG.error("Failed to construct WatermarkStrategy with parameters: {}, " +
							"please make sure {} has the proper constructor",
							parameterStr, clazz.getName(), e);
						throw new RuntimeException("Failed to parse WatermarkStrategy " +
							"with parameters", e);
					}
				} else {
					// 3.Config watermark strategy with empty constructor class.
					try {
						strategy = clazz.newInstance();
					} catch (InstantiationException | IllegalAccessException e) {
						LOG.error("Failed to parse WatermarkStrategy, the custom watermark " +
								"strategy must be config in three ways: 1. serialized object; " +
								"2. class and constructor parameters; 3. empty constructor class");
						throw new RuntimeException("Failed to parse WatermarkStrategy", e);
					}
				}
				break;
			default:
				throw new RuntimeException("Unsupported rowtime timestamps type: " + s);
		}

		return Optional.of(new Tuple2<>(extractor, strategy));
	}
}
