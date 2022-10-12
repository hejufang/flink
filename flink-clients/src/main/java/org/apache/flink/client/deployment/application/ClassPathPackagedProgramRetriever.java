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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.deployment.application.classpath.DefaultClasspathConstructor;
import org.apache.flink.client.deployment.application.classpath.UserClasspathConstructor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * A {@link org.apache.flink.client.program.PackagedProgramRetriever PackagedProgramRetriever}
 * which creates the {@link org.apache.flink.client.program.PackagedProgram PackagedProgram} containing
 * the user's {@code main()} from a class on the class path.
 */
@Internal
public class ClassPathPackagedProgramRetriever implements PackagedProgramRetriever {

	private static final Logger LOG = LoggerFactory.getLogger(ClassPathPackagedProgramRetriever.class);

	/** User classpaths in relative form to the working directory. */
	@Nonnull
	private final Collection<URL> userClassPaths;

	@Nonnull
	private final String[] programArguments;

	@Nullable
	private final String jobClassName;

	@Nonnull
	private final Supplier<Iterable<File>> jarsOnClassPath;

	@Nullable
	private final File userLibDirectory;

	@Nullable
	private final File jarFile;

	@Nonnull private final Configuration configuration;

	private ClassPathPackagedProgramRetriever(
		@Nonnull String[] programArguments,
		@Nullable String jobClassName,
		@Nonnull Supplier<Iterable<File>> jarsOnClassPath,
		@Nullable File userLibDirectory,
		@Nullable File jarFile,
		@Nonnull UserClasspathConstructor userClasspathConstructor,
		@Nonnull Configuration configuration) throws IOException {
		this.userLibDirectory = userLibDirectory;
		this.programArguments = requireNonNull(programArguments, "programArguments");
		this.jobClassName = jobClassName;
		this.jarsOnClassPath = requireNonNull(jarsOnClassPath);
		this.jarFile = jarFile;
		this.configuration = configuration;
		this.userClassPaths = UserClasspathConstructor.getFlinkUserClasspath(userClasspathConstructor, configuration, userLibDirectory, null);
	}

	@Override
	public PackagedProgram getPackagedProgram() throws FlinkException {
		try {
			if (configuration.getBoolean(PipelineOptions.USER_CLASSPATH_COMPATIBLE)) {
				// need to make the user classpath setting as same as yarn per job mode
				// in this mode, all jars including user jar, external jars, connector should be in the system classpath
				// so the classpath as well as the jar file in packaged program is empty.
				LOG.warn("user classpath compatible mode enabled, set the jar file and classpath in packaged program to null");
				final String entryClassName = jarFile != null ? jobClassName : getJobClassNameOrScanClassPath();
				return PackagedProgram.newBuilder()
						.setUserClassPaths(Collections.emptyList())
						.setArguments(programArguments)
						.setJarFile(null)
						.setConfiguration(configuration)
						.setEntryPointClassName(entryClassName)
						.build();
			}
			if (jarFile != null) {
				return PackagedProgram.newBuilder()
					.setUserClassPaths(new ArrayList<>(userClassPaths))
					.setArguments(programArguments)
					.setJarFile(jarFile)
					.setConfiguration(configuration)
					.setEntryPointClassName(jobClassName)
					.build();
			}

			final String entryClass = getJobClassNameOrScanClassPath();
			return PackagedProgram.newBuilder()
				.setUserClassPaths(new ArrayList<>(userClassPaths))
				.setEntryPointClassName(entryClass)
				.setArguments(programArguments)
				.setConfiguration(configuration)
				.build();
		} catch (ProgramInvocationException e) {
			throw new FlinkException("Could not load the provided entrypoint class.", e);
		}
	}

	private String getJobClassNameOrScanClassPath() throws FlinkException {
		if (jobClassName != null) {
			if (userLibDirectory != null) {
				// check that we find the entrypoint class in the user lib directory.
				if (!userClassPathContainsJobClass(jobClassName)) {
					throw new FlinkException(
						String.format(
							"Could not find the provided job class (%s) in the user lib directory (%s).",
							jobClassName,
							userLibDirectory));
				}
			}
			return jobClassName;
		}

		try {
			return scanClassPathForJobJar();
		} catch (IOException | NoSuchElementException | IllegalArgumentException e) {
			throw new FlinkException("Failed to find job JAR on class path. Please provide the job class name explicitly.", e);
		}
	}

	private boolean userClassPathContainsJobClass(String jobClassName) {
		for (URL userClassPath : userClassPaths) {
			try (final JarFile jarFile = new JarFile(userClassPath.getFile())) {
				if (jarContainsJobClass(jobClassName, jarFile)) {
					return true;
				}
			} catch (IOException e) {
				ExceptionUtils.rethrow(
					e,
					String.format(
						"Failed to open user class path %s. Make sure that all files on the user class path can be accessed.",
						userClassPath));
			}
		}
		return false;
	}

	private boolean jarContainsJobClass(String jobClassName, JarFile jarFile) {
		return jarFile
			.stream()
			.map(JarEntry::getName)
			.filter(fileName -> fileName.endsWith(FileUtils.CLASS_FILE_EXTENSION))
			.map(FileUtils::stripFileExtension)
			.map(fileName -> fileName.replaceAll(Pattern.quote(File.separator), FileUtils.PACKAGE_SEPARATOR))
			.anyMatch(name -> name.equals(jobClassName));
	}

	private String scanClassPathForJobJar() throws IOException {
		final Iterable<File> jars;
		if (userLibDirectory == null) {
			LOG.info("Scanning system class path for job JAR");
			jars = jarsOnClassPath.get();
		} else {
			LOG.info("Scanning user class path for job JAR");
			jars = userClassPaths
				.stream()
				.map(url -> new File(url.getFile()))
				.collect(Collectors.toList());
		}

		final JarManifestParser.JarFileWithEntryClass jobJar = JarManifestParser.findOnlyEntryClass(jars);
		LOG.info("Using {} as job jar", jobJar);
		return jobJar.getEntryClass();
	}

	@VisibleForTesting
	enum JarsOnClassPath implements Supplier<Iterable<File>> {
		INSTANCE;

		static final String JAVA_CLASS_PATH = "java.class.path";
		static final String PATH_SEPARATOR = "path.separator";
		static final String DEFAULT_PATH_SEPARATOR = ":";

		@Override
		public Iterable<File> get() {
			String classPath = System.getProperty(JAVA_CLASS_PATH, "");
			String pathSeparator = System.getProperty(PATH_SEPARATOR, DEFAULT_PATH_SEPARATOR);

			return Arrays.stream(classPath.split(pathSeparator))
				.filter(JarsOnClassPath::notNullAndNotEmpty)
				.map(File::new)
				.filter(File::isFile)
				.collect(Collectors.toList());
		}

		private static boolean notNullAndNotEmpty(String string) {
			return string != null && !string.equals("");
		}
	}

	/**
	 * A builder for the {@link ClassPathPackagedProgramRetriever}.
	 */
	public static class Builder {

		private final String[] programArguments;

		@Nullable
		private String jobClassName;

		@Nullable
		private File userLibDirectory;

		private Supplier<Iterable<File>> jarsOnClassPath = JarsOnClassPath.INSTANCE;

		private File jarFile;

		/**
		 * this constructor is used to get all type of jar files including user jar, user lib jars, external resources, connectors etc.
		 */
		@Nonnull
		private UserClasspathConstructor userClasspathConstructor = DefaultClasspathConstructor.INSTANCE;

		private final Configuration configuration;

		private Builder(String[] programArguments, Configuration configuration) {
			this.programArguments = requireNonNull(programArguments);
			this.configuration = requireNonNull(configuration);
		}

		public Builder setJobClassName(@Nullable String jobClassName) {
			this.jobClassName = jobClassName;
			return this;
		}

		public Builder setUserLibDirectory(File userLibDirectory) {
			this.userLibDirectory = userLibDirectory;
			return this;
		}

		public Builder setJarsOnClassPath(Supplier<Iterable<File>> jarsOnClassPath) {
			this.jarsOnClassPath = jarsOnClassPath;
			return this;
		}

		public Builder setJarFile(File file) {
			this.jarFile = file;
			return this;
		}

		public Builder setUserClasspathConstructor(@Nonnull UserClasspathConstructor userClasspathConstructor) {
			this.userClasspathConstructor = userClasspathConstructor;
			return this;
		}

		public ClassPathPackagedProgramRetriever build() throws IOException {
			return new ClassPathPackagedProgramRetriever(
				programArguments,
				jobClassName,
				jarsOnClassPath,
				userLibDirectory,
				jarFile,
				userClasspathConstructor,
				configuration);
		}
	}

	public static Builder newBuilder(String[] programArguments, Configuration configuration) {
		return new Builder(programArguments, configuration);
	}
}