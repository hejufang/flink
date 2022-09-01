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

package org.apache.flink.client.deployment.application.classpath;

import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * The user classpath constructor when user classpath compatible mode is on.
 */
public enum NoOpClasspathConstructor implements UserClasspathConstructor {

	INSTANCE;

	@Override
	public List<URL> getUserJar(Configuration flinkConfiguration) {
		return Collections.emptyList();
	}

	@Override
	public List<URL> getExternalJars(Configuration flinkConfiguration) {
		return Collections.emptyList();
	}

	@Override
	public List<URL> getUserLibFiles(@Nullable File jobDir) {
		return Collections.emptyList();
	}

	@Override
	public List<URL> getClasspathInConfig(Configuration flinkConfiguration, @Nullable String flinkHome) {
		return Collections.emptyList();
	}
}
