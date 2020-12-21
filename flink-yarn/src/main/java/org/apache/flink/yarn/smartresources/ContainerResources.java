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

package org.apache.flink.yarn.smartresources;

import java.util.Objects;

/**
 * Container resources.
 * */
public class ContainerResources {
	private int memoryMB;
	private double vcores;

	public ContainerResources(int memoryMB, double vcores) {
		this.memoryMB = memoryMB;
		this.vcores = vcores;
	}

	public int getMemoryMB() {
		return memoryMB;
	}

	public double getVcores() {
		return vcores;
	}

	public void setMemoryMB(int memoryMB) {
		this.memoryMB = memoryMB;
	}

	public void setVcores(double vcores) {
		this.vcores = vcores;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ContainerResources that = (ContainerResources) o;
		return memoryMB == that.memoryMB &&
			vcores == that.vcores;
	}

	@Override
	public int hashCode() {
		return Objects.hash(memoryMB, vcores);
	}

	@Override
	public String toString() {
		return "ContainerResources{" +
			"memoryMB=" + memoryMB +
			", vcores=" + vcores +
			'}';
	}
}
