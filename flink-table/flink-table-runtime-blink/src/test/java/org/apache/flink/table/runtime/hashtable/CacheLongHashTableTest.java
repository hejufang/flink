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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.CacheMemoryManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.UnionIterator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for {@link LongHashPartition} with {@link CacheMemoryManager}.
 */
@RunWith(Parameterized.class)
public class CacheLongHashTableTest {

	private static final int PAGE_SIZE = 32 * 1024;
	private static MemoryManager memManager;

	private IOManager ioManager;
	private BinaryRowDataSerializer buildSideSerializer;
	private BinaryRowDataSerializer probeSideSerializer;

	private boolean useCompress;
	private Configuration conf;

	public CacheLongHashTableTest(boolean useCompress) {
		this.useCompress = useCompress;
	}

	@Parameterized.Parameters(name = "useCompress-{0}")
	public static List<Boolean> getVarSeg() {
		return Arrays.asList(true, false);
	}

	@BeforeClass
	public static void beforeClass() {
		memManager = new CacheMemoryManager(
							896 * PAGE_SIZE,
							PAGE_SIZE,
							Duration.ofSeconds(10),
							true,
							1);
	}

	@AfterClass
	public static void afterClass() {
		memManager.shutdown();
	}

	@Before
	public void init() {
		TypeInformation[] types = new TypeInformation[]{Types.INT, Types.INT};
		this.buildSideSerializer = new BinaryRowDataSerializer(types.length);
		this.probeSideSerializer = new BinaryRowDataSerializer(types.length);
		this.ioManager = new IOManagerAsync();

		conf = new Configuration();
		conf.setBoolean(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED, useCompress);
	}

	private class MyHashTable extends LongHybridHashTable {

		public MyHashTable(long memorySize) {
			super(conf, CacheLongHashTableTest.this, buildSideSerializer, probeSideSerializer, memManager,
				memorySize, CacheLongHashTableTest.this.ioManager, 24, 200000);
		}

		@Override
		public long getBuildLongKey(RowData row) {
			return row.getInt(0);
		}

		@Override
		public long getProbeLongKey(RowData row) {
			return row.getInt(0);
		}

		@Override
		public BinaryRowData probeToBinary(RowData row) {
			return (BinaryRowData) row;
		}
	}

	@Test
	public void testInMemory() throws IOException {
		final int numKeys = 100000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(500 * PAGE_SIZE);

		int numRecordsInJoinResult = join(table, buildInput, probeInput);
		Assert.assertEquals("Wrong number of records in join result.", numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();

		table.free();
	}

	@Test
	public void testSpillingHashJoinOneRecursion() throws IOException {
		final int numKeys = 100000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(300 * PAGE_SIZE);

		int numRecordsInJoinResult = join(table, buildInput, probeInput);

		Assert.assertEquals("Wrong number of records in join result.", numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	/**
	 * Non partition in memory in level 0.
	 */
	@Test
	public void testSpillingHashJoinOneRecursionPerformance() throws IOException {
		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(100 * PAGE_SIZE);

		int numRecordsInJoinResult = join(table, buildInput, probeInput);

		Assert.assertEquals("Wrong number of records in join result.", numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	@Test
	public void testSpillingHashJoinOneRecursionValidity() throws IOException {
		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<>(numKeys);

		// ----------------------------------------------------------------------------------------
		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(100 * PAGE_SIZE);

		BinaryRowData buildRow = buildSideSerializer.createInstance();
		while ((buildRow = buildInput.next(buildRow)) != null) {
			table.putBuildRow(buildRow);
		}
		table.endBuild();

		BinaryRowData probeRow = probeSideSerializer.createInstance();
		while ((probeRow = probeInput.next(probeRow)) != null) {
			if (table.tryProbe(probeRow)) {
				testJoin(table, map);
			}
		}

		while (table.nextMatching()) {
			testJoin(table, map);
		}

		table.close();

		Assert.assertEquals("Wrong number of keys", numKeys, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();

			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
				probeValsPerKey * buildValsPerKey, val);
		}

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	@Test
	public void testSpillingHashJoinWithMassiveCollisions() throws IOException {
		// the following two values are known to have a hash-code collision on the initial level.
		// we use them to make sure one partition grows over-proportionally large
		final int repeatedValue1 = 40559;
		final int repeatedValue2 = 92882;
		final int repeatedValueCountBuild = 200000;
		final int repeatedValueCountProbe = 5;

		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<BinaryRowData> build1 = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		MutableObjectIterator<BinaryRowData> build2 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCountBuild);
		MutableObjectIterator<BinaryRowData> build3 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCountBuild);
		List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probe1 = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probe2 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue1, 17, 5);
		MutableObjectIterator<BinaryRowData> probe3 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue2, 23, 5);
		List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);

		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<>(numKeys);

		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(896 * PAGE_SIZE);

		BinaryRowData buildRow = buildSideSerializer.createInstance();
		while ((buildRow = buildInput.next(buildRow)) != null) {
			table.putBuildRow(buildRow);
		}
		table.endBuild();

		BinaryRowData probeRow = probeSideSerializer.createInstance();
		while ((probeRow = probeInput.next(probeRow)) != null) {
			if (table.tryProbe(probeRow)) {
				testJoin(table, map);
			}
		}

		while (table.nextMatching()) {
			testJoin(table, map);
		}

		table.close();

		Assert.assertEquals("Wrong number of keys", numKeys, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();

			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
				(key == repeatedValue1 || key == repeatedValue2) ?
					(probeValsPerKey + repeatedValueCountProbe) * (buildValsPerKey + repeatedValueCountBuild) :
					probeValsPerKey * buildValsPerKey, val);
		}

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	@Test
	public void testSpillingHashJoinWithTwoRecursions() throws IOException {
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int repeatedValue1 = 40559;
		final int repeatedValue2 = 92882;
		final int repeatedValueCountBuild = 200000;
		final int repeatedValueCountProbe = 5;

		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<BinaryRowData> build1 = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		MutableObjectIterator<BinaryRowData> build2 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCountBuild);
		MutableObjectIterator<BinaryRowData> build3 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCountBuild);
		List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probe1 = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probe2 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue1, 17, 5);
		MutableObjectIterator<BinaryRowData> probe3 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue2, 23, 5);
		List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);

		// create the map for validating the results
		HashMap<Integer, Long> map = new HashMap<>(numKeys);

		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(896 * PAGE_SIZE);

		BinaryRowData buildRow = buildSideSerializer.createInstance();
		while ((buildRow = buildInput.next(buildRow)) != null) {
			table.putBuildRow(buildRow);
		}
		table.endBuild();

		BinaryRowData probeRow = probeSideSerializer.createInstance();
		while ((probeRow = probeInput.next(probeRow)) != null) {
			if (table.tryProbe(probeRow)) {
				testJoin(table, map);
			}
		}

		while (table.nextMatching()) {
			testJoin(table, map);
		}

		table.close();

		Assert.assertEquals("Wrong number of keys", numKeys, map.size());
		for (Map.Entry<Integer, Long> entry : map.entrySet()) {
			long val = entry.getValue();
			int key = entry.getKey();

			Assert.assertEquals("Wrong number of values in per-key cross product for key " + key,
				(key == repeatedValue1 || key == repeatedValue2) ?
					(probeValsPerKey + repeatedValueCountProbe) * (buildValsPerKey + repeatedValueCountBuild) :
					probeValsPerKey * buildValsPerKey, val);
		}

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	/*
	 * This test is basically identical to the "testSpillingHashJoinWithMassiveCollisions" test, only that the number
	 * of repeated values (causing bucket collisions) are large enough to make sure that their target partition no longer
	 * fits into memory by itself and needs to be repartitioned in the recursion again.
	 */
	@Test
	public void testFailingHashJoinTooManyRecursions() throws IOException {
		// the following two values are known to have a hash-code collision on the first recursion level.
		// we use them to make sure one partition grows over-proportionally large
		final int repeatedValue1 = 40559;
		final int repeatedValue2 = 92882;
		final int repeatedValueCount = 3000000;

		final int numKeys = 1000000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 3 million pairs with 3 values sharing the same key, plus 400k pairs with two colliding keys
		MutableObjectIterator<BinaryRowData> build1 = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
		MutableObjectIterator<BinaryRowData> build2 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCount);
		MutableObjectIterator<BinaryRowData> build3 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCount);
		List<MutableObjectIterator<BinaryRowData>> builds = new ArrayList<>();
		builds.add(build1);
		builds.add(build2);
		builds.add(build3);
		MutableObjectIterator<BinaryRowData> buildInput = new UnionIterator<>(builds);

		// create a probe input that gives 10 million pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probe1 = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);
		MutableObjectIterator<BinaryRowData> probe2 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue1, 17, repeatedValueCount);
		MutableObjectIterator<BinaryRowData> probe3 = new BinaryHashTableTest.ConstantsKeyValuePairsIterator(repeatedValue2, 23, repeatedValueCount);
		List<MutableObjectIterator<BinaryRowData>> probes = new ArrayList<>();
		probes.add(probe1);
		probes.add(probe2);
		probes.add(probe3);
		MutableObjectIterator<BinaryRowData> probeInput = new UnionIterator<>(probes);
		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(896 * PAGE_SIZE);

		try {
			join(table, buildInput, probeInput);
			fail("Hash Join must have failed due to too many recursions.");
		} catch (Exception ex) {
			// expected
		}

		table.close();

		// ----------------------------------------------------------------------------------------

		table.free();
	}

	@Test
	public void testSparseProbeSpilling() throws IOException, MemoryAllocationException {
		final int numBuildKeys = 1000000;
		final int numBuildVals = 1;
		final int numProbeKeys = 20;
		final int numProbeVals = 1;

		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(
			numBuildKeys, numBuildVals, false);
		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(100 * PAGE_SIZE);

		int expectedNumResults = (Math.min(numProbeKeys, numBuildKeys) * numBuildVals)
			* numProbeVals;

		int numRecordsInJoinResult = join(table, buildInput, new UniformBinaryRowGenerator(numProbeKeys, numProbeVals, true));

		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);

		table.close();

		table.free();
	}

	@Test
	public void validateSpillingDuringInsertion() throws IOException, MemoryAllocationException {
		final int numBuildKeys = 500000;
		final int numBuildVals = 1;
		final int numProbeKeys = 10;
		final int numProbeVals = 1;

		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numBuildKeys, numBuildVals, false);
		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(85 * PAGE_SIZE);

		int expectedNumResults = (Math.min(numProbeKeys, numBuildKeys) * numBuildVals)
			* numProbeVals;

		int numRecordsInJoinResult = join(table, buildInput, new UniformBinaryRowGenerator(numProbeKeys, numProbeVals, true));

		Assert.assertEquals("Wrong number of records in join result.", expectedNumResults, numRecordsInJoinResult);

		table.close();

		table.free();
	}

	@Test
	public void testBucketsNotFulfillSegment() throws Exception {
		final int numKeys = 10000;
		final int buildValsPerKey = 3;
		final int probeValsPerKey = 10;

		// create a build input that gives 30000 pairs with 3 values sharing the same key
		MutableObjectIterator<BinaryRowData> buildInput = new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);

		// create a probe input that gives 100000 pairs with 10 values sharing a key
		MutableObjectIterator<BinaryRowData> probeInput = new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

		// ----------------------------------------------------------------------------------------

		final CacheLongHashTableTest.MyHashTable table = new CacheLongHashTableTest.MyHashTable(35 * PAGE_SIZE);

		int numRecordsInJoinResult = join(table, buildInput, probeInput);

		Assert.assertEquals("Wrong number of records in join result.", numKeys * buildValsPerKey * probeValsPerKey, numRecordsInJoinResult);

		table.close();
		table.free();
	}

	private void testJoin(CacheLongHashTableTest.MyHashTable table, HashMap<Integer, Long> map) throws IOException {
		BinaryRowData record;
		int numBuildValues = 0;

		final RowData probeRec = table.getCurrentProbeRow();
		int key = probeRec.getInt(0);

		RowIterator<BinaryRowData> buildSide = table.getBuildSideIterator();
		if (buildSide.advanceNext()) {
			numBuildValues = 1;
			record = buildSide.getRow();
			assertEquals("Probe-side key was different than build-side key.", key, record.getInt(0));
		} else {
			fail("No build side values found for a probe key.");
		}
		while (buildSide.advanceNext()) {
			numBuildValues++;
			record = buildSide.getRow();
			assertEquals("Probe-side key was different than build-side key.", key, record.getInt(0));
		}

		Long contained = map.get(key);
		if (contained == null) {
			contained = (long) numBuildValues;
		} else {
			contained = contained + numBuildValues;
		}

		map.put(key, contained);
	}

	private int join(
		CacheLongHashTableTest.MyHashTable table,
		MutableObjectIterator<BinaryRowData> buildInput,
		MutableObjectIterator<BinaryRowData> probeInput) throws IOException {
		int count = 0;

		BinaryRowData reuseBuildSizeRow = buildSideSerializer.createInstance();
		BinaryRowData buildRow;
		while ((buildRow = buildInput.next(reuseBuildSizeRow)) != null) {
			table.putBuildRow(buildRow);
		}
		table.endBuild();

		BinaryRowData probeRow = probeSideSerializer.createInstance();
		while ((probeRow = probeInput.next(probeRow)) != null) {
			if (table.tryProbe(probeRow)){
				count += joinWithNextKey(table);
			}
		}

		while (table.nextMatching()){
			count += joinWithNextKey(table);
		}
		return count;
	}

	private int joinWithNextKey(CacheLongHashTableTest.MyHashTable table) throws IOException {
		int count = 0;
		final RowIterator<BinaryRowData> buildIterator = table.getBuildSideIterator();
		final RowData probeRow = table.getCurrentProbeRow();
		BinaryRowData buildRow;

		buildRow = buildIterator.advanceNext() ? buildIterator.getRow() : null;
		// get the first build side value
		if (probeRow != null && buildRow != null) {
			count++;
			while (buildIterator.advanceNext()) {
				count++;
			}
		}
		return count;
	}
}
