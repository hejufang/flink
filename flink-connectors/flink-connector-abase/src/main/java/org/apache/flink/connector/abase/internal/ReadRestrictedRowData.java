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

package org.apache.flink.connector.abase.internal;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.Set;

/**
 * An internal implementation of {@link RowData} in which some fields are not allowed to read.
 *
 * <p>{@link ReadRestrictedRowData} is backed by a {@link GenericRowData} and it throws an
 * {@link UnsupportedOperationException} when trying to read forbidden fields.
 *
 */
public class ReadRestrictedRowData implements RowData {

	private final Set<Integer> readOnlyPos;
	private GenericRowData genericRowData;

	public ReadRestrictedRowData(int arity) {
		readOnlyPos = new HashSet<>();
		genericRowData = new GenericRowData(arity);
	}

	public void addRestrictedField(int pos) {
		readOnlyPos.add(pos);
	}

	public void setField(int pos, Object value) {
		genericRowData.setField(pos, value);
	}

	@Override
	public int getArity() {
		return genericRowData.getArity();
	}

	@Override
	public RowKind getRowKind() {
		return genericRowData.getRowKind();
	}

	@Override
	public void setRowKind(RowKind kind) {
		genericRowData.setRowKind(kind);
	}

	@Override
	public boolean isNullAt(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.isNullAt(pos);
	}

	@Override
	public boolean getBoolean(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getBoolean(pos);
	}

	@Override
	public byte getByte(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getByte(pos);
	}

	@Override
	public short getShort(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getShort(pos);
	}

	@Override
	public int getInt(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getInt(pos);
	}

	@Override
	public long getLong(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getLong(pos);
	}

	@Override
	public float getFloat(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getFloat(pos);
	}

	@Override
	public double getDouble(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getDouble(pos);
	}

	@Override
	public StringData getString(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getString(pos);
	}

	@Override
	public DecimalData getDecimal(int pos, int precision, int scale) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getDecimal(pos, precision, scale);
	}

	@Override
	public TimestampData getTimestamp(int pos, int precision) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getTimestamp(pos, precision);
	}

	@Override
	public <T> RawValueData<T> getRawValue(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getRawValue(pos);
	}

	@Override
	public byte[] getBinary(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getBinary(pos);
	}

	@Override
	public ArrayData getArray(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getArray(pos);
	}

	@Override
	public MapData getMap(int pos) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getMap(pos);
	}

	@Override
	public RowData getRow(int pos, int numFields) {
		if (readOnlyPos.contains(pos)) {
			throw new UnsupportedOperationException("The field at the index " + pos + " is write-only, can't read it!");
		}
		return genericRowData.getRow(pos, numFields);
	}
}
