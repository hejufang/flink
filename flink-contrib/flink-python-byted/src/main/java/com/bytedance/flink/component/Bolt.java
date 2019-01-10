package com.bytedance.flink.component;

import org.apache.flink.api.java.tuple.Tuple;

import com.bytedance.flink.collector.BoltCollector;
import com.bytedance.flink.pojo.BoltInfo;
import com.bytedance.flink.pojo.RuntimeConfig;

import java.io.Serializable;
import java.util.List;

/**
 * Basic bolt interface.
 */
public interface Bolt extends Serializable {
	/**
	 * Called when a bolt is initialized.
	 */
	void open(RuntimeConfig runtimeConfig, BoltCollector boltCollector);

	/**
	 * Process list of inputs.
	 */
	void execute(Tuple tuple);

	/**
	 * Process list of inputs.
	 */
	void execute(List<Object> objects);

	/**
	 * Called when a bolt is going to shutdown.
	 */
	void close();

	/**
	 * Get bolt info.
	 */
	BoltInfo getBoltInfo();
}
