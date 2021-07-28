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

package com.bytedance.flink.pojo;

import java.io.Serializable;
import java.util.Map;

/**
 * Configurations of all spouts.
 */
public class SpoutConfig implements Serializable {
	private Map<String, Object> commonArgs;
	private Map<String, SpoutInfo> spoutInfoMap;

	public Map<String, Object> getCommonArgs() {
		return commonArgs;
	}

	public void setCommonArgs(Map<String, Object> commonArgs) {
		this.commonArgs = commonArgs;
	}

	public Map<String, SpoutInfo> getSpoutInfoMap() {
		return spoutInfoMap;
	}

	public void setSpoutInfoMap(Map<String, SpoutInfo> spoutInfoMap) {
		this.spoutInfoMap = spoutInfoMap;
	}

	@Override
	public String toString() {
		return "SpoutConfig{" +
			"commonArgs=" + commonArgs +
			", spoutInfoMap=" + spoutInfoMap +
			'}';
	}
}
