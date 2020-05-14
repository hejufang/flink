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

package org.apache.flink.connectors.bytable.util;

import org.apache.flink.connectors.bytable.BytableOption;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.bytedance.bytable.Client;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;

/**
 * BytableConnectorUtils Function.
 */
public class BytableConnectorUtils {
	private static final Logger LOG = LoggerFactory.getLogger(BytableConnectorUtils.class);
	public static final int BATCH_SIZE_DEFAULT = 10;
	public static final int THREAD_POOL_SIZE_DEFAULT = 10;
	public static final int MASTER_TIMEOUT_MS_DEFAULT = 5000;
	public static final int TABLE_SERVER_CONNECT_TIMEOUT_MS_DEFAULT = 5000;
	public static final int TABLE_SERVER_READ_TIMEOUT_MS_DEFAULT = 5000;
	public static final int TABLE_SERVER_WRITE_TIMEOUT_MS_DEFAULT = 5000;
	public static final Client.ClientMetaCacheType CLIENT_META_CACHE_TYPE_DEFAULT =
		Client.ClientMetaCacheType.OnDemandMetaCache;
	private static int referenceCount = 0;
	private static String fileLibPath = "bytableLib";
	private static boolean initFlag = false;

	public static synchronized Client getBytableClient(BytableOption bytableOption) {
		if (!initFlag) {
			try {
				LOG.info("Init the bytable environment.");
				initBytableEnvironment();
				initFlag = true;
			} catch (Exception e) {
				throw new FlinkRuntimeException("Init the bytable environment failed.", e);
			}
		}
		referenceCount++;
		String clusterName = bytableOption.getClusterName();
		String masterUrls = bytableOption.getMasterUrls();
		int threadPoolSize = bytableOption.getThreadPoolSize();
		int masterTimeoutMs = bytableOption.getMasterTimeOutMs();
		int tableServerConnectTimeoutMs = bytableOption.getTableServerConnectTimeoutMs();
		int tableServerReadTimeoutMs = bytableOption.getTableServerReadTimeoutMs();
		int tableServerWriteTimeoutMs = bytableOption.getTableServerWriteTimeoutMs();
		Client.ClientMetaCacheType clientMetaCacheType = bytableOption.getClientMetaCacheType();
		Preconditions.checkNotNull(clusterName, "clusterName can not be null");
		Preconditions.checkNotNull(masterUrls, "masterUrls can not be null");
		Client client = null;
		try {
			client = new Client(clusterName, masterUrls, threadPoolSize, masterTimeoutMs,
				tableServerConnectTimeoutMs, tableServerReadTimeoutMs, tableServerWriteTimeoutMs, clientMetaCacheType);
		} catch (IOException e) {
			throw new FlinkRuntimeException(e);
		}
		LOG.info("Connection established.");
		return client;
	}

	private static void initBytableEnvironment() {
		fileLibPath += new AbstractID();
		LOG.info("Extract the file to : " + fileLibPath);
		extractLib(fileLibPath, "libgflags.so.2");
		extractLib(fileLibPath, "libcrypto.so.1.0.0");
		extractLib(fileLibPath, "libinfsec.so");
		extractLib(fileLibPath, "libjemalloc.so.2");
		extractLib(fileLibPath, "libmsgpack.so.3");
		extractLib(fileLibPath, "libprotobuf.so.15");
		extractLib(fileLibPath, "libssl.so.1.0.0");
		extractLib(fileLibPath, "libthrift-0.9.1.so");
		extractLib(fileLibPath, "libglog.so.0");
		extractLib(fileLibPath, "libbytable-cclient.so");
		String libPath = System.getProperty("user.dir") + "/" + fileLibPath;
		LOG.info("begin to set the D library : " + libPath);
		addLibraryDir(libPath);
		LOG.info("init the bytable environment success");
	}

	/**
	 * Extract the dynamic library, then load it.
	 * @param path the location of the .so
	 * @param name the name of the so
	 */
	private static void extractLib(String path, String name) {
		InputStream in = null;
		OutputStream out = null;
		try {
			in = BytableConnectorUtils.class.getClassLoader()
				.getResourceAsStream("org/apache/flink/connectors/bytable/util/" + name);
			File fileOutDic = new File(path);
			if (!fileOutDic.exists()) {
				fileOutDic.mkdirs();
			}
			File fileOut = new File(path + "/" + name);
			if (!fileOut.exists()) {
				fileOut.createNewFile();
			}
			out = new FileOutputStream(fileOut);
			IOUtils.copy(in, out, 8024);
			System.load(fileOut.getAbsolutePath());
		} catch (Exception e) {
			throw new FlinkRuntimeException("Load the dynamic library failed !", e);
		} finally {
			try {
				in.close();
				out.close();
			} catch (IOException e) {
				LOG.error("Close the dynamic file failed.", e);
			}
		}
	}

	private static void addLibraryDir(String libraryPath) {
		try {
			Field field = ClassLoader.class.getDeclaredField("usr_paths");
			field.setAccessible(true);
			String[] paths = (String[]) field.get(null);
			for (int i = 0; i < paths.length; i++) {
				if (libraryPath.equals(paths[i])) {
					return;
				}
			}
			String[] tmp = new String[paths.length + 1];
			System.arraycopy(paths, 0, tmp, 0, paths.length);
			tmp[paths.length] = libraryPath;
			field.set(null, tmp);
		} catch (IllegalAccessException | NoSuchFieldException e) {
			if (e instanceof IllegalAccessException) {
				throw new FlinkRuntimeException("Failed to get permissions to set library path", e);
			} else {
				throw new FlinkRuntimeException("Failed to get field handle to set library path", e);
			}
		}
	}

	public static synchronized void closeFile() {
		referenceCount--;
		if (referenceCount == 0) {
			String dir = System.getProperty("user.dir") + "/" + fileLibPath;
			boolean success = deleteDir((new File(dir)));
			if (success) {
				(new File(dir)).delete();
				LOG.info("Successfully deleted {} . ", dir);
			} else {
				LOG.error("Failed to delete {} .", dir);
			}
		}
	}

	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}
}
