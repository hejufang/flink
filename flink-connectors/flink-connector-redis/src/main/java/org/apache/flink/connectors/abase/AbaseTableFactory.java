package org.apache.flink.connectors.abase;

import org.apache.flink.connectors.redis.RedisTableFactory;
import org.apache.flink.table.descriptors.AbaseValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.RedisValidator.ABASE;

/**
 * Factory for creating abase sink.
 */
public class AbaseTableFactory extends RedisTableFactory {
	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, ABASE); // abase
		return context;
	}

	@Override
	public DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new AbaseValidator().validate(descriptorProperties);
		return descriptorProperties;
	}

}
