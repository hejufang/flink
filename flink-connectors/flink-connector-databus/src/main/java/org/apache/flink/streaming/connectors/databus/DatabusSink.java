/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.databus;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.bytedance.data.databus.DatabusClient;

/**
 * The databus connector used to sink data to Databus.
 * typically used in addSink() function.
 */
public class DatabusSink<T> extends RichSinkFunction<T> {

	private DatabusClient databusClient;
	private String channel;
	private int port = -1;
	private int codec = 0;
	private TransforType transformType;
	private boolean hasCheckedType = false;

	/**
	 * The enum type to identify the data format which will sink to databus.
	 */
	public enum TransforType {
		STRING,
		BYTE_ARRAY,
		TUPLE2_STRING_STRING,
		TUPLE2_STRING_BYTE_ARRAY
	}

	/**
	 * The constructor.
	 *
	 * @param port which port to connect default 9133.
	 * @param channel DatabusChannel to which the data sink.
	 * @param codec the way to compress, default 0, means do not compress.
	 * @param type type of data to sink.
	 */
	public DatabusSink(int port, String channel, int codec, TransforType type) {
		this.port = port;
		this.channel = channel;
		this.codec = codec;
		this.transformType = type;
	}

	/**
	 * The constructor using default codec = 0 (not compress).
	 *
	 * @param port which port to connect.
	 * @param channel DatabusChannel to which the data sink.
	 * @param type type of data to sink.
	 */
	public DatabusSink(int port, String channel, TransforType type) {
		this.port = port;
		this.channel = channel;
		this.transformType = type;
	}

	/**
	 * The constructor using default port = 9133 to connect databus.
	 *
	 * @param channel DatabusChannel to which the data sink.
	 * @param codec the way to compress, default 0, means do not compress.
	 * @param type type of data to sink.
	 */
	public DatabusSink(String channel, int codec, TransforType type) {
		this.channel = channel;
		this.codec = codec;
		this.transformType = type;
	}

	/**
	 * The constructor using default port = 9133 and codec = 0 (not compress).
	 *
	 * @param channel DatabusChannel to which the data sink.
	 * @param type type of data to sink.
	 */
	public DatabusSink(String channel, TransforType type) {
		this.channel = channel;
		this.transformType = type;
	}

	/**
	 * Called for only once. Initial the databusClient.
	 *
	 * @param parameters super Class function needed.
	 * @throws Exception throw the Exception of super.open().
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		this.databusClient = new DatabusClient(this.port, this.channel);
		super.open(parameters);
	}

	/**
	 * Called for each data. Just test the correctness of type one time. Then call diff function
	 * following the transformType. The support Type is String, byte[], Tuple2< String, String > and
	 * Tuple2< String, byte[] >.
	 *
	 * @param msg the msg to sink.
	 * @param context the super class needed, the function without Context isn't suggest.
	 * @throws Exception when transfromType is diff with the dataType. Must be the same for the
	 * 					 following send process.
	 */
	@Override
	public void invoke(T msg, Context context) throws Exception {
		if (!hasCheckedType) {
			if (msg == null) {
				this.databusClient.send(null, (String) null, this.codec);
				return;
			}
			if (msg instanceof Tuple2 && ((Tuple2) msg).f0 == null && ((Tuple2) msg).f1 == null) {
				this.databusClient.send(null, (String) null, this.codec);
				return;
			}
			TransforType msgType = getMsgType(msg);
			if (msgType == null || msgType != this.transformType) {
				throw new Exception("Sink to Databus type error: Only support String, byte[], Tuple2<String, String>, Tuple2<String, byte[]>");
			}
			this.hasCheckedType = true;
		}

		switch (this.transformType) {
			case STRING:
				this.databusClient.send("", (String) msg, this.codec);
				break;
			case BYTE_ARRAY:
				this.databusClient.send("", (byte[]) msg, this.codec);
				break;
			case TUPLE2_STRING_STRING:
				this.databusClient.send((String) ((Tuple2) msg).f0, (String) ((Tuple2) msg).f1, this.codec);
				break;
			case TUPLE2_STRING_BYTE_ARRAY:
				this.databusClient.send((String) ((Tuple2) msg).f0, (byte[]) ((Tuple2) msg).f1, this.codec);
				break;
			default:
				throw new Exception("Databus Case Type error. Typically is never happended.");
		}
	}

	private TransforType getMsgType(T msg) {
		if (msg instanceof String) {
			return TransforType.STRING;
		} else if (msg instanceof byte[]) {
			return TransforType.BYTE_ARRAY;
		} else if (msg instanceof Tuple2) {
			if (((Tuple2) msg).f0 instanceof String || ((Tuple2) msg).f0 == null) {
				if (((Tuple2) msg).f1 instanceof String) {
					return TransforType.TUPLE2_STRING_STRING;
				} else if (((Tuple2) msg).f1 instanceof byte[]) {
					return TransforType.TUPLE2_STRING_BYTE_ARRAY;
				}
			}
		}
		return null;
	}
}
