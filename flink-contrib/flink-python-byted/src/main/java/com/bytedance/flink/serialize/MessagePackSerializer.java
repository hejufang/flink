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

package com.bytedance.flink.serialize;

import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.pojo.ShellMessage;
import org.msgpack.MessagePack;
import org.msgpack.MessageTypeException;
import org.msgpack.annotation.Message;
import org.msgpack.template.Template;
import org.msgpack.template.Templates;
import org.msgpack.type.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Messagepack serializer.
 */
public class MessagePackSerializer implements MessageSerializable {
	private static final Logger LOG = LoggerFactory.getLogger(MessageSerializable.class);

	private DataOutputStream processIn;
	private InputStream processOut;
	private MessagePack msgPack;
	private Template<Map<String, Value>> mapTmpl;
	private Template<List<Value>> listTmpl;
	private boolean isIgnoreMismatchedMsg = false;

	public MessagePackSerializer(boolean isIgnoreMismatchedMsg) {
		this.isIgnoreMismatchedMsg = isIgnoreMismatchedMsg;
	}

	public void initialize(OutputStream processIn, InputStream processOut) {
		this.processIn = new DataOutputStream(processIn);
		this.processOut = processOut;
		this.msgPack = new MessagePack();
		this.mapTmpl = Templates.tMap(Templates.TString, Templates.TValue);
		this.listTmpl = Templates.tList(Templates.TValue);
	}

	public Number connect(RuntimeConfig config) throws IOException {
		Map<String, Object> setupmsg = new HashMap();
		setupmsg.put(Constants.CONF, config.toMap());
		setupmsg.put(Constants.PID_DIR_KEY, config.getPidDir());
		this.writeMessage(setupmsg);
		Map<String, Value> pidmsg = this.readMessage();
		Value pid = pidmsg.get(Constants.PID);
		return pid.asIntegerValue().getInt();
	}

	public ShellMessage readShellMsg() throws IOException {
		Map<String, Value> msg = this.readMessage();
		ShellMessage shellMsg = new ShellMessage();
		String command = (msg.get(Constants.COMMAND)).asRawValue().getString();
		shellMsg.setCommand(command);

		Value log = msg.get(Constants.MESSAGE);
		if (log != null) {
			shellMsg.setMessage(log.asRawValue().getString());
		}

		Value tupleValue = msg.get(Constants.TUPLE);
		if (tupleValue != null) {
			Iterator iterator = tupleValue.asArrayValue().iterator();
			while (iterator.hasNext()) {
				Value element = (Value) iterator.next();
				if (shellMsg.getTuple() == null) {
					shellMsg.setTuple(new ArrayList<>());
				}
				try {
					shellMsg.getTuple().add(valueToJavaType(element));
				} catch (Throwable e) {
					LOG.error("ValueToJavaType error, error element: {}, tupleValue: {}", element, tupleValue);
					throw e;
				}
			}
		}
		return shellMsg;
	}

	@Override
	public void writeShellMsg(ShellMessage message) throws IOException {
		Map<String, Object> map = new HashMap<>();
		List<Object> tuple = new ArrayList<>(message.getTuple().size());
		Iterator iterator = message.getTuple().iterator();
		while (iterator.hasNext()) {
			Object o = iterator.next();
			tuple.add(this.javaToMsgpack(o));
		}
		map.put(Constants.TUPLE, tuple);
		map.put(Constants.COMMAND, message.getCommand());
		this.writeMessage(map);
	}

	private Object valueToJavaType(Value element) {
		switch (element.getType()) {
			case RAW:
				byte[] bs = element.asRawValue().getByteArray();
				return new MessagePackSerializer.BytesKey(bs);
			case INTEGER:
				return element.asIntegerValue().getLong();
			case FLOAT:
				return element.asFloatValue().getDouble();
			case BOOLEAN:
				return element.asBooleanValue().getBoolean();
			case NIL:
				return null;
			case ARRAY:
				List<Object> elementList = new ArrayList();
				Value[] array = element.asArrayValue().getElementArray();
				int length = array.length;

				for (int i = 0; i < length; ++i) {
					Value e = array[i];
					elementList.add(this.valueToJavaType(e));
				}
				return elementList;
			case MAP:
				Map<Object, Object> elementMap = new HashMap();
				Iterator iterator = element.asMapValue().entrySet().iterator();

				while (iterator.hasNext()) {
					Map.Entry<Value, Value> v = (Map.Entry) iterator.next();
					elementMap.put(this.valueToJavaType(v.getKey()), this.valueToJavaType(v.getValue()));
				}
				return elementMap;
			default:
				return element;
		}
	}

	public Object javaToMsgpack(Object o) {
		if (o instanceof MessagePackSerializer.BytesKey) {
			return ((MessagePackSerializer.BytesKey) o).key;
		} else {
			Iterator var3;
			if (o instanceof Map) {
				Map<Object, Object> m = new HashMap();
				var3 = ((Map) o).entrySet().iterator();

				while (var3.hasNext()) {
					Map.Entry<Object, Object> v = (Map.Entry) var3.next();
					m.put(this.javaToMsgpack(v.getKey()), this.javaToMsgpack(v.getValue()));
				}

				return m;
			} else if (!(o instanceof List)) {
				return o;
			} else {
				List<Object> list = new ArrayList();
				var3 = ((List) o).iterator();

				while (var3.hasNext()) {
					Object l = var3.next();
					list.add(this.javaToMsgpack(l));
				}

				return list;
			}
		}
	}

	private Map<String, Value> readMessage() throws IOException {
		Map<String, Value> msg;
		while (true) {
			try {
				msg = this.msgPack.read(this.processOut, this.mapTmpl);
				return msg;
			} catch (MessageTypeException e) {
				if (isIgnoreMismatchedMsg) {
					LOG.warn("Get an MessageTypeException from python, just ignore it.");
				} else {
					throw new RuntimeException(e);
				}
			}
		}
	}

	private void writeMessage(Map<String, Object> msg) throws IOException {
		this.msgPack.write(this.processIn, msg);
		this.processIn.flush();
	}

	/**
	 * Bytes container.
	 * */
	@Message
	public static class BytesKey {
		public byte[] key;

		public BytesKey() {
		}

		public BytesKey(byte[] key) {
			this.key = key;
		}

		public boolean equals(Object o) {
			if (this == o) {
				return true;
			} else {
				return !(o instanceof MessagePackSerializer.BytesKey) ? false : Arrays.equals(this.key, ((MessagePackSerializer.BytesKey) o).key);
			}
		}

		public int hashCode() {
			return Arrays.hashCode(this.key);
		}
	}
}

