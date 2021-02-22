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

package org.apache.flink.table.api;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Resource such as hive jar, dorado resource.
 */
public class Resource {
	private final ResourceType resourceType;
	private final String uri;
	// whether this resource is from hive DDL.
	private final boolean isFromHive;

	public Resource(ResourceType resourceType, String uri, boolean isFromHive) {
		this.resourceType = resourceType;
		this.uri = uri;
		this.isFromHive = isFromHive;
	}

	public ResourceType getResourceType() {
		return resourceType;
	}

	public String getUri() {
		return uri;
	}

	public boolean isFromHive() {
		return isFromHive;
	}

	/**
	 * Resource type.
	 * */
	public enum ResourceType {
		JAR,
		FILE,
		ARCHIVE,
		DORADO;

		private static final List<String> ALL_RESOURCE_TYPE_NAMES =
			Stream.of(ResourceType.values())
				.map(ResourceType::toString)
				.collect(Collectors.toList());

		public static ResourceType parseFrom(String typeName) {
			if (ALL_RESOURCE_TYPE_NAMES.contains(typeName)) {
				return ResourceType.valueOf(typeName);
			}
			throw new IllegalArgumentException("Unsupported resource type :" + typeName);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Resource resource = (Resource) o;
		return isFromHive == resource.isFromHive &&
			resourceType == resource.resourceType &&
			Objects.equals(uri, resource.uri);
	}

	@Override
	public int hashCode() {
		return Objects.hash(resourceType, uri, isFromHive);
	}

	@Override
	public String toString() {
		return "Resource{" +
			"resourceType=" + resourceType +
			", uri='" + uri + '\'' +
			", isFromHive=" + isFromHive +
			'}';
	}
}
