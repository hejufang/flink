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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.codesplit.JavaCodeSplitter;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A wrapper for generated class, defines a {@link #newInstance(ClassLoader)} method
 * to get an instance by reference objects easily.
 */
public abstract class GeneratedClass<T> implements Serializable {
	private static final long serialVersionUID = 6170410716745042722L;

	private final String className;
	private final String code;
	private final String splitCode;
	private final Object[] references;
	/**
	 * Source terms for generating this class.
	 * As the name of variables and classes in the generated code depends on a counter,
	 * which is not stable enough. We should compare the inputs instead of the generated code
	 * when we want to tell whether two generated classes are equal or not.
	 */
	private final Object[] sourcesForGenerating;

	private transient Class<T> compiledClass;

	protected GeneratedClass(String className, String code, Object[] references, TableConfig config) {
		this(className, code, references, null, config);
	}

	public GeneratedClass(
			String className,
			String code,
			Object[] references,
			Object[] sourcesForGenerating,
			TableConfig config) {
		checkNotNull(className, "name must not be null");
		checkNotNull(code, "code must not be null");
		checkNotNull(references, "references must not be null");
		this.className = className;
		this.code = code;
		this.references = references;
		this.sourcesForGenerating = sourcesForGenerating;
		this.splitCode =
			code.isEmpty() || !config.getConfiguration().get(TableConfigOptions.SPLIT_GENERATED_CODE_ENABLED)
				? null
				: JavaCodeSplitter.split(
				code,
				config.getConfiguration().get(TableConfigOptions.MAX_LENGTH_GENERATED_CODE),
				config.getConfiguration().get(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE));
	}

	/**
	 * Create a new instance of this generated class.
	 */
	@SuppressWarnings("unchecked")
	public T newInstance(ClassLoader classLoader) {
		try {
			return compile(classLoader).getConstructor(Object[].class)
					// Because Constructor.newInstance(Object... initargs), we need to load
					// references into a new Object[], otherwise it cannot be compiled.
					.newInstance(new Object[] {references});
		} catch (Exception e) {
			throw new RuntimeException(
				"Could not instantiate generated class '" + className + "'", e);
		}
	}

	@SuppressWarnings("unchecked")
	public T newInstance(ClassLoader classLoader, Object... args) {
		try {
			return (T) compile(classLoader).getConstructors()[0].newInstance(args);
		} catch (Exception e) {
			throw new RuntimeException(
					"Could not instantiate generated class '" + className + "'", e);
		}
	}

	/**
	 * Compiles the generated code, the compiled class will be cached in the {@link GeneratedClass}.
	 */
	public Class<T> compile(ClassLoader classLoader) {
		if (compiledClass == null) {
			// cache the compiled class
			compiledClass = CompileUtils.compile(classLoader, className,
				splitCode == null ? code : splitCode);
		}
		return compiledClass;
	}

	public String getClassName() {
		return className;
	}

	public String getCode() {
		return code;
	}

	public Object[] getReferences() {
		return references;
	}

	public Class<T> getClass(ClassLoader classLoader) {
		return compile(classLoader);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof GeneratedClass) {
			GeneratedClass generatedClass = (GeneratedClass) obj;
			if (generatedClass.sourcesForGenerating == null || this.sourcesForGenerating == null) {
				//For backward compatibility, as the old serialized object doesn't contain sourcesForGenerating.
				return this.getClassName().equals(generatedClass.getClassName()) &&
					this.getCode().equals(generatedClass.getCode()) &&
					Arrays.equals(this.getReferences(), generatedClass.getReferences());
			} else {
				return Arrays.deepEquals(generatedClass.sourcesForGenerating, this.sourcesForGenerating);
			}
		} else {
			return false;
		}
	}
}
