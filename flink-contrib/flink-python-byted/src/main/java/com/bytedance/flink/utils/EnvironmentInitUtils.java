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

package com.bytedance.flink.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.RuntimeConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Utility class for environment initialization.
 */

public class EnvironmentInitUtils {
	private static final Logger LOG = LoggerFactory.getLogger(EnvironmentInitUtils.class);
	private static final ConcurrentMap<String, Object> resourcesLockMap = new ConcurrentHashMap<>();
	private static final List<String> FORCE_REPLACE_ENV = Arrays.asList("SEC_KV_AUTH");

	/**
	 * @param runtimeConfig runtime configuration.
	 * @param runtimeObject a runtime object in user jar.
	 */
	public static void prepareLocalDir(RuntimeConfig runtimeConfig, Object runtimeObject) {
		LOG.info("Prepare local dir runtimeConfig = {}", runtimeConfig);
		String jobName = runtimeConfig.getJobName();
		String codeDir = runtimeConfig.getCodeDir();
		List<String> resources = runtimeConfig.getResourceFiles();

		if (runtimeConfig.getRunMode() == Constants.RUN_MODE_STANDLONE) {
			String tmpDir = runtimeConfig.getTmpDir();
			try {
				preparePublicDir(tmpDir);
				preparePublicDir(codeDir);
			} catch (IOException e) {
				LOG.warn("Failed to init work dir: {}", codeDir, e);
			}
		}

		Path jobBaseDir = Paths.get(codeDir, jobName);
		if (Files.notExists(jobBaseDir)) {
			LOG.info("JobBaseDir not exist {}", jobBaseDir);
			Object lock = resourcesLockMap.computeIfAbsent(jobBaseDir.toString(), s -> new Object());
			synchronized (lock) {
				if (Files.exists(jobBaseDir)) {
					LOG.info("JobBaseDir created by other thread, {}", jobBaseDir);
					return;
				}
				Path jobBaseTmpDir = Paths.get(jobBaseDir.toString() + "_tmp_" +
					String.valueOf(System.currentTimeMillis()));
				ClassLoader classLoader = runtimeObject.getClass().getClassLoader();
				for (String resource : resources) {
					LOG.info("Copy resource: {}", resource);
					InputStream in = classLoader.getResourceAsStream(resource);
					if (in == null) {
						throw new RuntimeException("Resource file is null, " + resource);
					}

					Path resourceTmpPath = Paths.get(jobBaseTmpDir.toString(), resource);
					if (Files.notExists(resourceTmpPath.getParent())) {
						File parentDir = new File(resourceTmpPath.getParent().toString());
						if (!parentDir.mkdirs()) {
							throw new RuntimeException("Can't create file path " + parentDir.toString());
						}
					}
					try {
						LOG.info("decompress resource {} -> {}", resource, resourceTmpPath.toString());
						Files.copy(in, resourceTmpPath);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				try {
					LOG.info("mv jobbase dir {} -> {}", jobBaseTmpDir, jobBaseDir);
					FileUtils.moveDirectory(jobBaseTmpDir.toFile(), jobBaseDir.toFile());
					LOG.info("mv jobbase dir successed, {}", jobBaseDir.toString());
				} catch (IOException e) {
					try {
						FileUtils.deleteDirectory(jobBaseDir.toFile());
					} catch (IOException e1) {
						LOG.error("Failed to delete directory: {}", jobBaseDir.toFile());
					}
					throw new RuntimeException(e);
				}
			}
		}
	}

	public static void preparePublicDir(String dirName) throws IOException {
		if (Files.exists(Paths.get(dirName))) {
			LOG.info("Dir: {} exists, ignore", dirName);
			return;
		}
		Files.createDirectories(Paths.get(dirName));
		File dir = new File(dirName);
		dir.setExecutable(true, false);
		dir.setReadable(true, false);
		dir.setWritable(true, false);
	}

	public static String getResourceDir(RuntimeConfig runtimeConfig) {
		String jobName = runtimeConfig.getJobName();
		String codeDir = runtimeConfig.getCodeDir();

		Path jobBaseDir = Paths.get(codeDir, jobName, Constants.RESOURCE_FILE_PREFIX);
		return jobBaseDir.toAbsolutePath().toString();
	}

	public static List<String> getResourceFileList(String localJar) {
		List<String> resources = new ArrayList<>();
		Enumeration<? extends ZipEntry> entries;
		try {
			entries = (new ZipFile(localJar)).entries();
			while (entries != null && entries.hasMoreElements()) {
				ZipEntry zipEntry = entries.nextElement();
				if (zipEntry.getName().startsWith(Constants.RESOURCE_FILE_PREFIX)) {
					resources.add(zipEntry.getName());
				}
			}
		} catch (IOException e) {
			LOG.error("Local jar error", e);
		}
		return resources;
	}

	public static List<String> getResourceFileListFromParameter(String resourceFiles) {
		List<String> resources = new ArrayList<>();
		String[] resourceFliesArray = resourceFiles.split(";");
		for (String file: resourceFliesArray) {
			resources.add(file);
		}
		return resources;
	}

	public static String[] buildShellCommand(String interpreter, String scriptName, Map<String, Object> args,
		boolean dontWriteBytecode) {
		String[] command = new String[3];
		command[0] = "bash";
		command[1] = "-c";
		StringBuilder strBuf = new StringBuilder();
		String argsStr = "";
		try {
			ObjectMapper objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
			argsStr = objectMapper.writeValueAsString(args);
		} catch (JsonProcessingException ex) {
			throw new RuntimeException("cloud not build shell command with args: " + args.toString(), ex);
		}
		argsStr = argsStr.replace("\"", "\\\"");
		if (dontWriteBytecode) {
			strBuf.append(String.format("%s -B %s --args \"%s\"", interpreter, scriptName, argsStr));
		} else {
			strBuf.append(String.format("%s %s --args \"%s\"", interpreter, scriptName, argsStr));
		}
		command[2] = strBuf.toString();
		return command;
	}

	/**
	 * Get user log file path.
	 */
	public static String getLogFile(RuntimeConfig runtimeConfig) {
		if (runtimeConfig.getRunMode() == Constants.RUN_MODE_STANDLONE) {
			// Use user defined log file in user yaml in local mode.
			String jobName = runtimeConfig.getJobName();
			String jobLogDir = Paths.get(runtimeConfig.getLocalLogDir(), jobName).toString();
			try {
				preparePublicDir(jobLogDir);
			} catch (IOException e) {
				LOG.error("Failed to create local log dir: {}", jobLogDir);
			}
			return Paths.get(jobLogDir, jobName + ".userlog").toString();
		}
		String taskManagerLogFile = System.getProperty("log.file");
		if (taskManagerLogFile != null) {
			return taskManagerLogFile.substring(0, taskManagerLogFile.length() - 4)
				+ ".userlog";
		}
		return null;
	}

	/**
	 * Get pyFlink required environments.
	 *
	 * @param config runtime configuration.
	 * @return return pyFlink required environments.
	 */
	public static Map<String, String> getPyFlinkRequiredEnvironment(RuntimeConfig config) {
		Map<String, String> pyFlinkEnv = new HashMap<>();
		List<String> pythonPathList = new ArrayList<>();
		String resourceDir = EnvironmentInitUtils.getResourceDir(config);
		pythonPathList.add(Constants.PYTHONPATH_VAL);
		pythonPathList.add(resourceDir);
		String pythonPath = String.join(":", pythonPathList);
		pyFlinkEnv.put(Constants.PYTHONPATH_KEY, pythonPath);
		return pyFlinkEnv;
	}

	/**
	 * Merge user defined envs, pyFlink required envs and java envs.
	 *
	 * @param config runtime configuration.
	 * @return return the real environments for the python process.
	 */
	public static Map<String, String> getEnvironment(RuntimeConfig config) {
		Map<String, String> userDefinedEnv = config.getEnvironment();
		Map<String, String> javaEnv = System.getenv();
		Map<String, String> pyFlinkEnv = getPyFlinkRequiredEnvironment(config);
		Map<String, String> realEnv = new HashMap<>(javaEnv);

		mergeEnviroment(realEnv, userDefinedEnv);
		mergeEnviroment(realEnv, pyFlinkEnv);
		return realEnv;
	}

	public static Map<String, String> mergeEnviroment(Map<String, String> baseEnv,
		Map<String, String> env) {
		for (Map.Entry<String, String> entry : env.entrySet()) {
			// Some environments are added on Python side, for some reason we need add a default environment on Java side,
			// but the value of these environments cannot be merged, such as SEC_KV_AUTH which need be a integer.
			// So only merge environments which not in FORCE_REPLACE_ENV.
			if (!FORCE_REPLACE_ENV.contains(entry.getKey()) && baseEnv.containsKey(entry.getKey())) {
				baseEnv.put(entry.getKey(), baseEnv.get(entry.getKey()) + ":" + entry.getValue());
			} else {
				baseEnv.put(entry.getKey(), entry.getValue());
			}
		}
		return baseEnv;
	}
}
