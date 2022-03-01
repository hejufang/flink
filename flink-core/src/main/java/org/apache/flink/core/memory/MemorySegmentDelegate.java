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

package org.apache.flink.core.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Delegate the operation of segment and check the owner before modify the value in it.
 */
public class MemorySegmentDelegate extends MemorySegment {
	private final Object owner;
	private final MemorySegment segment;

	public MemorySegmentDelegate(Object owner, MemorySegment segment) {
		super(segment.getAddress(), segment.size(), owner);
		this.owner = owner;
		this.segment = segment;
	}

	@Override
	public int size() {
		validateOwner();
		return segment.size();
	}

	@Override
	public boolean isFreed() {
		validateOwner();
		return segment.isFreed();
	}

	@Override
	public void free() {
		validateOwner();
		segment.free();
	}

	@Override
	public boolean isOffHeap() {
		validateOwner();
		return segment.isOffHeap();
	}

	@Override
	public byte[] getArray() {
		validateOwner();
		return segment.getArray();
	}

	@Override
	public char getChar(int index) {
		validateOwner();
		return segment.getChar(index);
	}

	@Override
	public final char getCharLittleEndian(int index) {
		validateOwner();
		return segment.getCharLittleEndian(index);
	}

	@Override
	public final char getCharBigEndian(int index) {
		validateOwner();
		return segment.getCharBigEndian(index);
	}

	@Override
	public final void putChar(int index, char value) {
		validateOwner();
		segment.putChar(index, value);
	}

	@Override
	public final void putCharLittleEndian(int index, char value) {
		validateOwner();
		segment.putCharLittleEndian(index, value);
	}

	@Override
	public final void putCharBigEndian(int index, char value) {
		validateOwner();
		segment.putCharBigEndian(index, value);
	}

	@Override
	public final short getShort(int index) {
		validateOwner();
		return segment.getShort(index);
	}

	@Override
	public final short getShortLittleEndian(int index) {
		validateOwner();
		return segment.getShortLittleEndian(index);
	}

	@Override
	public final short getShortBigEndian(int index) {
		validateOwner();
		return segment.getShortBigEndian(index);
	}

	@Override
	public final void putShort(int index, short value) {
		validateOwner();
		segment.putShort(index, value);
	}

	@Override
	public final void putShortLittleEndian(int index, short value) {
		validateOwner();
		segment.putShortLittleEndian(index, value);
	}

	@Override
	public final void putShortBigEndian(int index, short value) {
		validateOwner();
		segment.putShortBigEndian(index, value);
	}

	@Override
	public final int getInt(int index) {
		validateOwner();
		return segment.getInt(index);
	}

	@Override
	public final int getIntLittleEndian(int index) {
		validateOwner();
		return segment.getIntLittleEndian(index);
	}

	@Override
	public final int getIntBigEndian(int index) {
		validateOwner();
		return segment.getIntBigEndian(index);
	}

	@Override
	public final void putInt(int index, int value) {
		validateOwner();
		segment.putInt(index, value);
	}

	@Override
	public final void putIntLittleEndian(int index, int value) {
		validateOwner();
		segment.putIntLittleEndian(index, value);
	}

	@Override
	public final void putIntBigEndian(int index, int value) {
		validateOwner();
		segment.putIntBigEndian(index, value);
	}

	@Override
	public final long getLong(int index) {
		validateOwner();
		return segment.getLong(index);
	}

	/**
	 * Reads a long integer value (64bit, 8 bytes) from the given position, in little endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getLong(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getLong(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final long getLongLittleEndian(int index) {
		validateOwner();
		return segment.getLongLittleEndian(index);
	}

	/**
	 * Reads a long integer value (64bit, 8 bytes) from the given position, in big endian byte order.
	 * This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getLong(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getLong(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final long getLongBigEndian(int index) {
		validateOwner();
		return segment.getLongBigEndian(index);
	}

	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in the system's native
	 * byte order. This method offers the best speed for long integer writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final void putLong(int index, long value) {
		validateOwner();
		segment.putLong(index, value);
	}

	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putLong(int, long)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putLong(int, long)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final void putLongLittleEndian(int index, long value) {
		validateOwner();
		segment.putLongLittleEndian(index, value);
	}

	/**
	 * Writes the given long value (64bit, 8 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putLong(int, long)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putLong(int, long)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final void putLongBigEndian(int index, long value) {
		validateOwner();
		segment.putLongBigEndian(index, value);
	}

	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in the system's
	 * native byte order. This method offers the best speed for float reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The float value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 4.
	 */
	public final float getFloat(int index) {
		validateOwner();
		return segment.getFloat(index);
	}

	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getFloat(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getFloat(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 4.
	 */
	public final float getFloatLittleEndian(int index) {
		validateOwner();
		return segment.getFloatLittleEndian(index);
	}

	/**
	 * Reads a single-precision floating point value (32bit, 4 bytes) from the given position, in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getFloat(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getFloat(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 4.
	 */
	public final float getFloatBigEndian(int index) {
		validateOwner();
		return segment.getFloatBigEndian(index);
	}

	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in the system's native
	 * byte order. This method offers the best speed for float writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The float value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 4.
	 */
	public final void putFloat(int index, float value) {
		validateOwner();
		segment.putFloat(index, value);
	}

	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putFloat(int, float)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putFloat(int, float)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 4.
	 */
	public final void putFloatLittleEndian(int index, float value) {
		validateOwner();
		segment.putFloatLittleEndian(index, value);
	}

	/**
	 * Writes the given single-precision float value (32bit, 4 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putFloat(int, float)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putFloat(int, float)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 4.
	 */
	public final void putFloatBigEndian(int index, float value) {
		validateOwner();
		segment.putFloatBigEndian(index, value);
	}

	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in the system's
	 * native byte order. This method offers the best speed for double reading and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The double value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final double getDouble(int index) {
		validateOwner();
		return segment.getDouble(index);
	}

	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getDouble(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getDouble(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final double getDoubleLittleEndian(int index) {
		validateOwner();
		return segment.getDoubleLittleEndian(index);
	}

	/**
	 * Reads a double-precision floating point value (64bit, 8 bytes) from the given position, in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #getDouble(int)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #getDouble(int)} is the preferable choice.
	 *
	 * @param index The position from which the value will be read.
	 * @return The long value at the given position.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final double getDoubleBigEndian(int index) {
		validateOwner();
		return segment.getDoubleBigEndian(index);
	}

	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in the
	 * system's native byte order. This method offers the best speed for double writing and should be used
	 * unless a specific byte order is required. In most cases, it suffices to know that the
	 * byte order in which the value is written is the same as the one in which it is read
	 * (such as transient storage in memory, or serialization for I/O and network), making this
	 * method the preferable choice.
	 *
	 * @param index The position at which the memory will be written.
	 * @param value The double value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final void putDouble(int index, double value) {
		validateOwner();
		segment.putDouble(index, value);
	}

	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in little endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putDouble(int, double)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putDouble(int, double)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final void putDoubleLittleEndian(int index, double value) {
		validateOwner();
		segment.putDoubleLittleEndian(index, value);
	}

	/**
	 * Writes the given double-precision floating-point value (64bit, 8 bytes) to the given position in big endian
	 * byte order. This method's speed depends on the system's native byte order, and it
	 * is possibly slower than {@link #putDouble(int, double)}. For most cases (such as
	 * transient storage in memory or serialization for I/O and network),
	 * it suffices to know that the byte order in which the value is written is the same as the
	 * one in which it is read, and {@link #putDouble(int, double)} is the preferable choice.
	 *
	 * @param index The position at which the value will be written.
	 * @param value The long value to be written.
	 *
	 * @throws IndexOutOfBoundsException Thrown, if the index is negative, or larger than the segment
	 *                                   size minus 8.
	 */
	public final void putDoubleBigEndian(int index, double value) {
		validateOwner();
		segment.putDoubleBigEndian(index, value);
	}

	public final void copyTo(int offset, MemorySegment target, int targetOffset, int numBytes) {
		validateOwner();
		segment.copyTo(offset, target, targetOffset, numBytes);
	}

	/**
	 * Bulk copy method. Copies {@code numBytes} bytes to target unsafe object and pointer.
	 * NOTE: This is an unsafe method, no check here, please be careful.
	 *
	 * @param offset The position where the bytes are started to be read from in this memory segment.
	 * @param target The unsafe memory to copy the bytes to.
	 * @param targetPointer The position in the target unsafe memory to copy the chunk to.
	 * @param numBytes The number of bytes to copy.
	 *
	 * @throws IndexOutOfBoundsException If the source segment does not contain the given number
	 *           of bytes (starting from offset).
	 */
	public final void copyToUnsafe(int offset, Object target, int targetPointer, int numBytes) {
		validateOwner();
		segment.copyToUnsafe(offset, target, targetPointer, numBytes);
	}

	/**
	 * Bulk copy method. Copies {@code numBytes} bytes from source unsafe object and pointer.
	 * NOTE: This is an unsafe method, no check here, please be careful.
	 *
	 * @param offset The position where the bytes are started to be write in this memory segment.
	 * @param source The unsafe memory to copy the bytes from.
	 * @param sourcePointer The position in the source unsafe memory to copy the chunk from.
	 * @param numBytes The number of bytes to copy.
	 *
	 * @throws IndexOutOfBoundsException If this segment can not contain the given number
	 *           of bytes (starting from offset).
	 */
	public final void copyFromUnsafe(int offset, Object source, int sourcePointer, int numBytes) {
		validateOwner();
		segment.copyFromUnsafe(offset, source, sourcePointer, numBytes);
	}

	/**
	 * Compares two memory segment regions.
	 *
	 * @param seg2 Segment to compare this segment with
	 * @param offset1 Offset of this segment to start comparing
	 * @param offset2 Offset of seg2 to start comparing
	 * @param len Length of the compared memory region
	 *
	 * @return 0 if equal, -1 if seg1 &lt; seg2, 1 otherwise
	 */
	public final int compare(MemorySegment seg2, int offset1, int offset2, int len) {
		validateOwner();
		return segment.compare(seg2, offset1, offset2, len);
	}

	/**
	 * Compares two memory segment regions with different length.
	 *
	 * @param seg2 Segment to compare this segment with
	 * @param offset1 Offset of this segment to start comparing
	 * @param offset2 Offset of seg2 to start comparing
	 * @param len1 Length of this memory region to compare
	 * @param len2 Length of seg2 to compare
	 *
	 * @return 0 if equal, -1 if seg1 &lt; seg2, 1 otherwise
	 */
	public final int compare(MemorySegment seg2, int offset1, int offset2, int len1, int len2) {
		validateOwner();
		return segment.compare(seg2, offset1, offset2, len1, len2);
	}

	/**
	 * Swaps bytes between two memory segments, using the given auxiliary buffer.
	 *
	 * @param tempBuffer The auxiliary buffer in which to put data during triangle swap.
	 * @param seg2 Segment to swap bytes with
	 * @param offset1 Offset of this segment to start swapping
	 * @param offset2 Offset of seg2 to start swapping
	 * @param len Length of the swapped memory region
	 */
	public final void swapBytes(byte[] tempBuffer, MemorySegment seg2, int offset1, int offset2, int len) {
		validateOwner();
		segment.swapBytes(tempBuffer, seg2, offset1, offset2, len);
	}

	/**
	 * Equals two memory segment regions.
	 *
	 * @param seg2 Segment to equal this segment with
	 * @param offset1 Offset of this segment to start equaling
	 * @param offset2 Offset of seg2 to start equaling
	 * @param length Length of the equaled memory region
	 *
	 * @return true if equal, false otherwise
	 */
	public final boolean equalTo(MemorySegment seg2, int offset1, int offset2, int length) {
		validateOwner();
		return segment.equalTo(seg2, offset1, offset2, length);
	}

	/**
	 * Get the heap byte array object.
	 * @return Return non-null if the memory is on the heap, and return null if the memory if off the heap.
	 */
	public byte[] getHeapMemory() {
		validateOwner();
		return segment.heapMemory;
	}

	@Override
	public long getAddress() {
		validateOwner();
		return segment.getAddress();
	}

	@Override
	public void clear() {
		validateOwner();
		this.segment.clear();
	}

	@Override
	public void freeOwner() {
		validateOwner();
		segment.freeOwner();
	}

	private void validateOwner() {
		Object segmentOwner = segment.getOwner();
		if (owner != segment.getOwner()) {
			throw new RuntimeException("Owner " + owner.getClass() + "@" + owner.hashCode() + " (" + owner +
				") try to modify segment " + segment + " which belongs to " +
				segmentOwner.getClass() + "@" + segmentOwner.hashCode() + " (" + segmentOwner + ")");
		}
	}

	@Override
	public ByteBuffer wrap(int offset, int length) {
		validateOwner();
		return this.segment.wrap(offset, length);
	}

	@Override
	public byte get(int index) {
		validateOwner();
		return this.segment.get(index);
	}

	@Override
	public void put(int index, byte b) {
		validateOwner();
		this.segment.put(index, b);
	}

	@Override
	public void get(int index, byte[] dst) {
		validateOwner();
		this.segment.get(index, dst);
	}

	@Override
	public void put(int index, byte[] src) {
		validateOwner();
		segment.put(index, src);
	}

	@Override
	public void get(int index, byte[] dst, int offset, int length) {
		validateOwner();
		segment.get(index, dst, offset, length);
	}

	@Override
	public void put(int index, byte[] src, int offset, int length) {
		validateOwner();
		segment.put(index, src, offset, length);
	}

	@Override
	public boolean getBoolean(int index) {
		validateOwner();
		return segment.getBoolean(index);
	}

	@Override
	public void putBoolean(int index, boolean value) {
		validateOwner();
		segment.putBoolean(index, value);
	}

	@Override
	public void get(DataOutput out, int offset, int length) throws IOException {
		validateOwner();
		segment.get(out, offset, length);
	}

	@Override
	public void put(DataInput in, int offset, int length) throws IOException {
		validateOwner();
		segment.put(in, offset, length);
	}

	@Override
	public void get(int offset, ByteBuffer target, int numBytes) {
		validateOwner();
		segment.get(offset, target, numBytes);
	}

	@Override
	public void put(int offset, ByteBuffer source, int numBytes) {
		validateOwner();
		segment.put(offset, source, numBytes);
	}

	@Override
	public <T> T processAsByteBuffer(Function<ByteBuffer, T> processFunction) {
		validateOwner();
		return segment.processAsByteBuffer(processFunction);
	}

	@Override
	public void processAsByteBuffer(Consumer<ByteBuffer> processConsumer) {
		validateOwner();
		segment.processAsByteBuffer(processConsumer);
	}

	public MemorySegment getSegment() {
		return segment;
	}
}
