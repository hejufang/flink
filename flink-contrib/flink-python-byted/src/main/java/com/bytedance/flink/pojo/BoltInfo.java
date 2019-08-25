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

import com.bytedance.flink.topology.Grouping;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Bolt info POJO.
 */
public class BoltInfo implements Serializable {
	private String name;
	private String script;
	private String interpreter;
	private int parallelism;
	private Map<String, Object> args;
	private List<String> outputFields;
	private List<Grouping> groupList;
	private Schema outputSchema;

	public BoltInfo() {
	}

	public BoltInfo(String name, String script, String interpreter, int parallelism,
					Map<String, Object> args, List<String> outputFields,
					List<Grouping> groupList) {
		this.name = name;
		this.script = script;
		this.interpreter = interpreter;
		this.parallelism = parallelism;
		this.args = args;
		this.outputFields = outputFields;
		this.groupList = groupList;
		this.outputSchema = new Schema(outputFields);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public String getInterpreter() {
		return interpreter;
	}

	public void setInterpreter(String interpreter) {
		this.interpreter = interpreter;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public Map<String, Object> getArgs() {
		return args;
	}

	public void setArgs(Map<String, Object> args) {
		this.args = args;
	}

	public List<String> getOutputFields() {
		return outputFields;
	}

	public void setOutputFields(List<String> outputFields) {
		this.outputFields = outputFields;
		this.outputSchema = new Schema(outputFields);
	}

	public List<Grouping> getGroupList() {
		return groupList;
	}

	public void setGroupList(List<Grouping> groupList) {
		this.groupList = groupList;
	}

	public Schema getOutputSchema() {
		return outputSchema;
	}

	public void setOutputSchema(Schema outputSchema) {
		this.outputSchema = outputSchema;
	}

	@Override
	public String toString() {
		return "BoltInfo{" +
			"name='" + name + '\'' +
			", script='" + script + '\'' +
			", interpreter='" + interpreter + '\'' +
			", parallelism=" + parallelism +
			", args=" + args +
			", outputFields=" + outputFields +
			", groupList=" + groupList +
			", outputSchema=" + outputSchema +
			'}';
	}
}
