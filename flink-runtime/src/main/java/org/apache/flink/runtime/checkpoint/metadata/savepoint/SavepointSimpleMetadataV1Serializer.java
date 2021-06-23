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

package org.apache.flink.runtime.checkpoint.metadata.savepoint;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * SavepointSimpleMetadataSerializer basic implementation.
 */
public class SavepointSimpleMetadataV1Serializer implements SavepointSimpleMetadataSerializer{
	/** The savepoint simple meta version. */
	public static final int VERSION = 1;

	/** The singleton instance of the serializer. */
	public static final SavepointSimpleMetadataV1Serializer INSTANCE = new SavepointSimpleMetadataV1Serializer();

	private SavepointSimpleMetadataV1Serializer() {

	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	// ------------------------------------------------------------------------
	//  (De)serialization entry points
	// ------------------------------------------------------------------------

	public static void serialize(SavepointSimpleMetadata metadata, DataOutputStream dos) throws IOException {
		// first: checkpoint ID
		dos.writeLong(metadata.getCheckpointId());

		// second: savepoint location path
		dos.writeUTF(metadata.getActualSavepointPath());
	}

	@Override
	public SavepointSimpleMetadata deserialize(DataInputStream dis, ClassLoader classLoader) throws IOException {
		// first: checkpoint ID
		final long checkpointId = dis.readLong();
		if (checkpointId < 0) {
			throw new IOException("invalid checkpoint ID: " + checkpointId);
		}

		final String savepointPath = dis.readUTF();
		return new SavepointSimpleMetadata(checkpointId, savepointPath);
	}
}
