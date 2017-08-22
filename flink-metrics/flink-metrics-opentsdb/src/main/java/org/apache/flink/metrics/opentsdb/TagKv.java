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

package org.apache.flink.metrics.opentsdb;

/**
 * Created by zhangguanghui on 2017/7/27.
 */
import java.util.List;

public class TagKv {
	String name;
	String value;

	public TagKv() {
	}

	public TagKv(String name, String value) {
		this.name = name;
		this.value = value;
	}

	public static String compositeTags(List<TagKv> tagKvList) {
		StringBuilder sb = new StringBuilder();
		for (TagKv tag : tagKvList) {
			sb.append(tag.name);
			sb.append("=");
			sb.append(tag.value);
			sb.append("|");
		}
		if (sb.length() == 0) {
			return sb.toString();
		}

		return sb.substring(0, sb.length() - 1);
	}
}
