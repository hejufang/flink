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

package org.apache.flink.connectors.bytable.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.bytable.BytableTableSchema;
import org.apache.flink.types.Row;

import com.bytedance.bytable.RowMutation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * A read and write helper for Bytable. The helper can used to create a {@link RowMutation} for writing
 * to Bytable table, and supports delete the data in Bytable.
 */
public class BytableReadWriteHelper {

	// family keys
	private final byte[][] families;
	// qualifier keys
	private final byte[][][] qualifiers;
	// qualifier types
	private final int[][] qualifierTypes;

	// row key index in output row
	private final int rowKeyIndex;
	// type of row key
	private final int rowKeyType;

	private final int fieldLength;
	private final Charset charset;

	// row which is returned
	private Row resultRow;
	// nested family rows
	private Row[] familyRows;

	public BytableReadWriteHelper(BytableTableSchema bytableTableSchema) {
		this.families = bytableTableSchema.getFamilyKeys();
		this.qualifiers = new byte[this.families.length][][];
		this.qualifierTypes = new int[this.families.length][];
		this.familyRows = new Row[this.families.length];
		String[] familyNames = bytableTableSchema.getFamilyNames();
		for (int f = 0; f < families.length; f++) {
			this.qualifiers[f] = bytableTableSchema.getQualifierKeys(familyNames[f]);
			TypeInformation[] typeInfos = bytableTableSchema.getQualifierTypes(familyNames[f]);
			this.qualifierTypes[f] = new int[typeInfos.length];
			for (int i = 0; i < typeInfos.length; i++) {
				qualifierTypes[f][i] = BytableTypeUtils.getTypeIndex(typeInfos[i]);
			}
			this.familyRows[f] = new Row(typeInfos.length);
		}
		this.charset = Charset.forName(bytableTableSchema.getStringCharset());
		// row key
		this.rowKeyIndex = bytableTableSchema.getRowKeyIndex();
		this.rowKeyType = bytableTableSchema.getRowKeyTypeInfo()
			.map(BytableTypeUtils::getTypeIndex)
			.orElse(-1);

		// field length need take row key into account if it exists.
		this.fieldLength = rowKeyIndex == -1 ? families.length : families.length + 1;

		// prepare output rows
		this.resultRow = new Row(fieldLength);
	}

	/**
	 * Returns an instance of RowMutation that writes record to Bytable table.
	 *
	 * @return The appropriate instance of RowMutation for this use case.
	 */
	public RowMutation createPutMutation(Row row) throws IOException {
		assert rowKeyIndex != -1;
		byte[] rowkey = BytableTypeUtils.serializeFromObject(row.getField(rowKeyIndex), rowKeyType, charset);
		// upsert
		RowMutation m = new RowMutation(rowkey);
		for (int i = 0; i < fieldLength; i++) {
			if (i != rowKeyIndex) {
				int f = i > rowKeyIndex ? i - 1 : i;
				// get family key
				byte[] familyKey = families[f];
				Row familyRow = (Row) row.getField(i);
				for (int q = 0; q < this.qualifiers[f].length; q++) {
					// get quantifier key
					byte[] qualifier = qualifiers[f][q];
					// get quantifier type idx
					int typeIdx = qualifierTypes[f][q];
					// read value
					byte[] value = BytableTypeUtils.serializeFromObject(familyRow.getField(q), typeIdx, charset);
					m.put(familyKey, qualifier, value);
				}
			}
		}
		return m;
	}

}
