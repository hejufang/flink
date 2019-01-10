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

package com.bytedance.flink.topology;

import java.io.Serializable;
import java.util.List;

/**
 * Grouping configuration.
 */
public class Grouping implements Serializable{
	private GroupType groupType;
	private List<String> groupFields;
	private String inputStream;

	public Grouping() {
	}

	public Grouping(GroupType groupType, List<String> groupFields, String inputStream) {
		this.groupType = groupType;
		this.groupFields = groupFields;
		this.inputStream = inputStream;
	}

	public GroupType getGroupType() {
		return groupType;
	}

	public void setGroupType(GroupType groupType) {
		this.groupType = groupType;
	}

	public List<String> getGroupFields() {
		return groupFields;
	}

	public void setGroupFields(List<String> groupFields) {
		this.groupFields = groupFields;
	}

	public String getInputStream() {
		return inputStream;
	}

	public void setInputStream(String inputStream) {
		this.inputStream = inputStream;
	}

	@Override
	public String toString() {
		return "Grouping{" +
			"groupType=" + groupType +
			", groupFields=" + groupFields +
			", inputStream='" + inputStream + '\'' +
			'}';
	}
}
