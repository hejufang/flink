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

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Produces data into multi kafka cluster.
 */
public class FlinkKafkaMultiClusterProducer010<IN, OUT> implements SinkFunction<IN>, RichFunction {
	private HashMap<String, FlinkKafkaProducer010<RecordWrapperPOJO<OUT>>> producerMap;
	private Router<IN, OUT> router;
	private KeyedSchemaWrapper<OUT> keyedSchemaWrapper;

	public FlinkKafkaMultiClusterProducer010(
		Collection<KafkaProducerConfPOJO<OUT>> producerConfs,
		KeyedSerializationSchema<OUT> keyedserializationSchema,
		Router<IN, OUT> router) throws Exception {

		if (producerConfs == null || producerConfs.size() == 0) {
			throw new Exception("No Kafka cluster configed");
		}
		if (keyedserializationSchema == null) {
			throw new Exception("KeyedSerializationSchema cannot be null");
		}
		if (router == null) {
			throw new Exception("Router cannot be null");
		}

		this.keyedSchemaWrapper = new KeyedSchemaWrapper<>(keyedserializationSchema);
		this.router = router;
		this.producerMap = new HashMap<>();
		initKafkaProducers(producerConfs);
	}

	public FlinkKafkaMultiClusterProducer010(
		Collection<KafkaProducerConfPOJO<OUT>> producerConfs,
		SerializationSchema<OUT> serializationSchema,
		Router<IN, OUT> router) throws Exception {
		if (serializationSchema == null) {
			throw new Exception("SerializationSchema cannot be null");
		}
		KeyedSerializationSchema<OUT> keyedserializationSchem =
			new KeyedSerializationSchemaWrapper<>(serializationSchema);
		this.keyedSchemaWrapper = new KeyedSchemaWrapper<>(keyedserializationSchem);
		this.router = router;
		this.producerMap = new HashMap<>();
		initKafkaProducers(producerConfs);
	}

	public void initKafkaProducers(Collection<KafkaProducerConfPOJO<OUT>> producerConfs) {
		for (KafkaProducerConfPOJO<OUT> produceConf : producerConfs) {
			String defaultTopic = produceConf.getDefaultTopic();
			KeyedSerializationSchema<OUT> specificKeyedserializationSchema =
				produceConf.getKeyedSerializationSchema();
			Properties properties = produceConf.getProperties();
			FlinkKafkaProducer010<RecordWrapperPOJO<OUT>> flinkKafkaProducer010;
			FlinkKafkaPartitioner<RecordWrapperPOJO<OUT>> partitioner = produceConf.getCustomPartitioner();

			if (specificKeyedserializationSchema == null) {
				flinkKafkaProducer010 = Kafka010Utils.customFlinkKafkaProducer010(
					produceConf.getClusterName(), defaultTopic, properties, this.keyedSchemaWrapper, partitioner);
			} else {
				flinkKafkaProducer010 = Kafka010Utils.customFlinkKafkaProducer010(
					produceConf.getClusterName(),
					defaultTopic,
					properties,
					new KeyedSchemaWrapper<>(specificKeyedserializationSchema),
					partitioner);
			}
			producerMap.put(produceConf.getClusterName(), flinkKafkaProducer010);
		}
	}

	public Set<FlinkKafkaProducer010> getProducers(){
		return new HashSet(producerMap.values());
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		RecordWrapperPOJO<OUT> recordWrapperPOJO = router.getTargetClusterAndTopic(value);
		String cluster = recordWrapperPOJO.getCluster();
		if (cluster == null) {
			throw new Exception("Kafka cluster name is null");
		}
		if (!producerMap.containsKey(cluster)) {
			throw new Exception("Kafka cluster " + cluster + "is not in producer config map");
		}
		FlinkKafkaProducer010<RecordWrapperPOJO<OUT>> producer = producerMap.get(cluster);
		producer.invoke(recordWrapperPOJO, context);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		for (FlinkKafkaProducer010 producer : producerMap.values()) {
			producer.open(parameters);
		}
	}

	@Override
	public void close() throws Exception {
		for (FlinkKafkaProducer010 producer : producerMap.values()) {
			producer.close();
		}
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		List<FlinkKafkaProducer010> producerList = new ArrayList<>(producerMap.values());
		return producerList.get(0).getIterationRuntimeContext();
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		for (FlinkKafkaProducer010 producer : producerMap.values()) {
			producer.setRuntimeContext(t);
		}
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		List<FlinkKafkaProducer010> producerList = new ArrayList<>(producerMap.values());
		return producerList.get(0).getIterationRuntimeContext();
	}
}
