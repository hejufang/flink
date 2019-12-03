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

package org.apache.flink.streaming.connectors.kafka.utils;

import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.Serializable;
import java.util.Properties;

/**
 * KafkaProducerConfPOJO contains cluster, topic, partitioner, key serialization schema
 * and properties.
 */
public class KafkaProducerConfPOJO<T> implements Serializable {
	private String clusterName;
	private String defaultTopic;
	private FlinkKafkaPartitioner<RecordWrapperPOJO<T>> customPartitioner;
	private KeyedSerializationSchema<T> keyedSerializationSchema;
	private Properties properties;

	public KafkaProducerConfPOJO(String clusterName, String defaultTopic,
			FlinkKafkaPartitioner customPartitioner) {
		this(clusterName, defaultTopic, customPartitioner, null);
	}

	public KafkaProducerConfPOJO(String clusterName, String defaultTopic,
			KeyedSerializationSchema<T> keyedSerializationSchema) {
		this(clusterName, defaultTopic, null, keyedSerializationSchema);
	}

	public KafkaProducerConfPOJO(String clusterName, String defaultTopic,
			SerializationSchema<T> serializationSchema) {
		this(clusterName, defaultTopic, null,
			new KeyedSerializationSchemaWrapper<>(serializationSchema));
	}

	public KafkaProducerConfPOJO(String clusterName, String defaultTopic) {
		this(clusterName, defaultTopic, null, null);
	}

	public KafkaProducerConfPOJO(
		String clusterName,
		String defaultTopic,
		FlinkKafkaPartitioner customPartitioner,
		KeyedSerializationSchema<T> keyedSerializationSchema) {
		this(clusterName, defaultTopic, null, null, null);
	}

	public KafkaProducerConfPOJO(
		String clusterName,
		String defaultTopic,
		FlinkKafkaPartitioner customPartitioner,
		KeyedSerializationSchema<T> keyedSerializationSchema,
		Properties properties) {
		this.clusterName = clusterName;
		this.defaultTopic = defaultTopic;
		this.customPartitioner = customPartitioner;
		this.keyedSerializationSchema = keyedSerializationSchema;
		this.properties = properties;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getDefaultTopic() {
		return defaultTopic;
	}

	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	public FlinkKafkaPartitioner<RecordWrapperPOJO<T>> getCustomPartitioner() {
		return customPartitioner;
	}

	public void setCustomPartitioner(FlinkKafkaPartitioner<RecordWrapperPOJO<T>> customPartitioner) {
		this.customPartitioner = customPartitioner;
	}

	public KeyedSerializationSchema<T> getKeyedSerializationSchema() {
		return keyedSerializationSchema;
	}

	public void setKeyedSerializationSchema(KeyedSerializationSchema<T> keyedSerializationSchema) {
		this.keyedSerializationSchema = keyedSerializationSchema;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KafkaProducerConfPOJO<?> that = (KafkaProducerConfPOJO<?>) o;

		if (clusterName == null && that.clusterName != null) {
			return false;
		} else if (!clusterName.equals(that.clusterName)) {
			return false;
		}
		if (defaultTopic == null && that.defaultTopic != null) {
			return false;
		} else if (!defaultTopic.equals(that.defaultTopic)) {
			return false;
		}
		if (customPartitioner == null && that.customPartitioner != null) {
			return false;
		} else if (!customPartitioner.equals(that.customPartitioner)) {
			return false;
		}
		if (keyedSerializationSchema == null && that.keyedSerializationSchema != null) {
			return false;
		} else if (!keyedSerializationSchema.equals(that.keyedSerializationSchema)) {
			return false;
		}

		return properties != null ? properties.equals(that.properties) : that.properties == null;
	}

	@Override
	public int hashCode() {
		int result = clusterName != null ? clusterName.hashCode() : 0;
		result = 31 * result + (defaultTopic != null ? defaultTopic.hashCode() : 0);
		result = 31 * result + (customPartitioner != null ? customPartitioner.hashCode() : 0);
		result = 31 * result + (keyedSerializationSchema != null ?
			keyedSerializationSchema.hashCode() : 0);
		result = 31 * result + (properties != null ? properties.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "KafkaProducerConfPOJO{" +
			"clusterName='" + clusterName + '\'' +
			", defaultTopic='" + defaultTopic + '\'' +
			", customPartitioner='" + customPartitioner + '\'' +
			", keyedSerializationSchema='" + keyedSerializationSchema + '\'' +
			", properties='" + properties + '\'' +
			'}';
	}
}
