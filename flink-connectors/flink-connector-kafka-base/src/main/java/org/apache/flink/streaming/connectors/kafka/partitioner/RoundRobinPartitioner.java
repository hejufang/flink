package org.apache.flink.streaming.connectors.kafka.partitioner;

public class RoundRobinPartitioner<T> extends FlinkKafkaPartitioner<T> {
	private int current;

	@Override
	public int partition(T in, byte[] bytes, byte[] bytes1, String s, int[] partitions) {
		current += 1;
		if (current >= partitions.length) {
			current = 0;
		}
		return partitions[current];
	}
}
