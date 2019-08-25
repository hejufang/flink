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

package org.apache.flink.smartresources;

/**
 * UpdateContainerResources.
 */
public class UpdateContainersResources implements java.io.Serializable {

	private static final long serialVersionUID = 9043166037948331420L;

	private int memoryMB;
	private int vcores;
	private int durtionMinutes;

	public UpdateContainersResources(int memory, int vcores, int durtionMinutes) {
		this.memoryMB = memory;
		this.vcores = vcores;
		this.durtionMinutes = durtionMinutes;
	}

	public int getMemoryMB() {
		return memoryMB;
	}

	public void setMemoryMB(int memoryMB) {
		this.memoryMB = memoryMB;
	}

	public int getVcores() {
		return vcores;
	}

	public void setVcores(int vcores) {
		this.vcores = vcores;
	}

	public int getDurtionMinutes() {
		return durtionMinutes;
	}

	public void setDurtionMinutes(int durtionMinutes) {
		this.durtionMinutes = durtionMinutes;
	}

	@Override
	public String toString() {
		return "UpdateContainersResources{" +
			"memoryMB=" + memoryMB +
			", vcores=" + vcores +
			", durtionMinutes=" + durtionMinutes +
			'}';
	}
}
