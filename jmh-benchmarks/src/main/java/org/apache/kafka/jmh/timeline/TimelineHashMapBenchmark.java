/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.timeline;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)

public class TimelineHashMapBenchmark {
    private final static int NUM_ENTRIES = 1_000_000;

    @State(Scope.Thread)
    public static class HashMapInput {
        public HashMap<Integer, String> map;
        public final List<Integer> keys = createKeys(NUM_ENTRIES);

        @Setup(Level.Invocation)
        public void setup() {
            map = new HashMap<>(keys.size());
            for (Integer key : keys) {
                map.put(key, String.valueOf(key));
            }

            Collections.shuffle(keys);
        }
    }

    @State(Scope.Thread)
    public static class ImmutableMapInput {
        scala.collection.immutable.HashMap<Integer, String> map;
        public final List<Integer> keys = createKeys(NUM_ENTRIES);

        @Setup(Level.Invocation)
        public void setup() {
            map = new scala.collection.immutable.HashMap<>();
            for (Integer key : keys) {
                map = map.updated(key, String.valueOf(key));
            }

            Collections.shuffle(keys);
        }
    }

    @State(Scope.Thread)
    public static class TimelineMapInput {
        public SnapshotRegistry snapshotRegistry;
        public TimelineHashMap<Integer, String> map;
        public final List<Integer> keys = createKeys(NUM_ENTRIES);

        @Setup(Level.Invocation)
        public void setup() {
            snapshotRegistry = new SnapshotRegistry(new LogContext());
            map = new TimelineHashMap<>(snapshotRegistry, keys.size());

            for (Integer key : keys) {
                map.put(key, String.valueOf(key));
            }

            Collections.shuffle(keys);
        }
    }

    @State(Scope.Thread)
    public static class TimelineMapSnapshotInput {
        public SnapshotRegistry snapshotRegistry;
        public TimelineHashMap<Integer, String> map;
        public final List<Integer> keys = createKeys(NUM_ENTRIES);

        @Setup(Level.Invocation)
        public void setup() {
            snapshotRegistry = new SnapshotRegistry(new LogContext());
            map = new TimelineHashMap<>(snapshotRegistry, keys.size());

            for (Integer key : keys) {
                map.put(key, String.valueOf(key));
            }

            int count = 0;
            for (Integer key : keys) {
                if (count % 1_000 == 0) {
                    snapshotRegistry.deleteSnapshotsUpTo(count - 10_000);
                    snapshotRegistry.createSnapshot(count);
                }
                map.put(key, String.valueOf(key));
                count++;
            }

            Collections.shuffle(keys);
        }
    }


    @Benchmark
    public Map<Integer, String> testAddEntriesInHashMap() {
        HashMap<Integer, String> map = new HashMap<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            int key = (int) (0xffffffff & ((i * 2862933555777941757L) + 3037000493L));
            map.put(key, String.valueOf(key));
        }

        return map;
    }

    @Benchmark
    public scala.collection.immutable.HashMap<Integer, String> testAddEntriesInImmutableMap() {
        scala.collection.immutable.HashMap<Integer, String> map = new scala.collection.immutable.HashMap<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            int key = (int) (0xffffffff & ((i * 2862933555777941757L) + 3037000493L));
            map = map.updated(key, String.valueOf(key));
        }

        return map;
    }

    @Benchmark
    public Map<Integer, String> testAddEntriesInTimelineMap() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Integer, String> map = new TimelineHashMap<>(snapshotRegistry, 16);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            int key = (int) (0xffffffff & ((i * 2862933555777941757L) + 3037000493L));
            map.put(key, String.valueOf(key));
        }

        return map;
    }

    @Benchmark
    public Map<Integer, String> testAddEntriesWithSnapshots() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Integer, String> map = new TimelineHashMap<>(snapshotRegistry, 16);

        long epoch = 0;
        int j = 0;
        for (int i = 0; i < NUM_ENTRIES; i++) {
            int key = (int) (0xffffffff & ((i * 2862933555777941757L) + 3037000493L));
            if (j > 10 && key % 3 == 0) {
                snapshotRegistry.deleteSnapshotsUpTo(epoch - 1000);
                snapshotRegistry.createSnapshot(epoch);
                j = 0;
            } else {
                j++;
            }
            map.put(key, String.valueOf(key));
            epoch++;
        }
        return map;
    }

    @Benchmark
    public Map<Integer, String> testUpdateEntriesInHashMap(HashMapInput input) {
        for (Integer key : input.keys) {
            input.map.put(key, String.valueOf(key));
        }
        return input.map;
    }

    @Benchmark
    public scala.collection.Map testUpdateEntriesInImmutableMap(ImmutableMapInput input) {
        scala.collection.immutable.HashMap<Integer, String> map = input.map;
        for (Integer key : input.keys) {
            map = map.updated(key, String.valueOf(key));
        }
        return map;
    }

    @Benchmark
    public Map<Integer, String> testUpdateEntriesInTimelineMap(TimelineMapInput input) {
        for (Integer key : input.keys) {
            input.map.put(key, String.valueOf(key));
        }
        return input.map;
    }

    @Benchmark
    public Map<Integer, String> testUpdateEntriesWithSnapshots(TimelineMapInput input) {
        long epoch = 0;
        int j = 0;
        for (Integer key : input.keys) {
            if (j > 1_000) {
                input.snapshotRegistry.deleteSnapshotsUpTo(epoch - 10_000);
                input.snapshotRegistry.createSnapshot(epoch);
                j = 0;
            } else {
                j++;
            }
            input.map.put(key, String.valueOf(key));
            epoch++;
        }
        return input.map;
    }

    @Benchmark
    public Map<Integer, String> testRemoveEntriesInHashMap(HashMapInput input) {
        for (Integer key : input.keys) {
            input.map.remove(key);
        }
        return input.map;
    }

    @Benchmark
    public scala.collection.Map testRemoveEntriesInImmutableMap(ImmutableMapInput input) {
        scala.collection.immutable.HashMap<Integer, String> map = input.map;
        for (Integer key : input.keys) {
            map = map.removed(key);
        }
        return map;
    }

    @Benchmark
    public Map<Integer, String> testRemoveEntriesInTimelineMap(TimelineMapInput input) {
        for (Integer key : input.keys) {
            input.map.remove(key);
        }
        return input.map;
    }

    @Benchmark
    public Map<Integer, String> testRemoveEntriesWithSnapshots(TimelineMapInput input) {
        long epoch = 0;
        int j = 0;
        for (Integer key : input.keys) {
            if (j > 1_000) {
                input.snapshotRegistry.deleteSnapshotsUpTo(epoch - 10_000);
                input.snapshotRegistry.createSnapshot(epoch);
                j = 0;
            } else {
                j++;
            }
            input.map.remove(key, String.valueOf(key));
            epoch++;
        }
        return input.map;
    }

    @Benchmark
    public int testIterateEntriesInHashMap(HashMapInput input) {
        int count = 0;
        for (HashMap.Entry<Integer, String> entry : input.map.entrySet()) {
            count++;
        }
        return count;
    }

    @Benchmark
    public int testIterateEntriesInImmutableMap(ImmutableMapInput input) {
        int count = 0;
        scala.collection.Iterator<scala.Tuple2<Integer, String>> iterator = input.map.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    @Benchmark
    public int testIterateEntriesWithSnapshots(TimelineMapSnapshotInput input) {
        int count = 0;
        for (TimelineHashMap.Entry<Integer, String> entry : input.map.entrySet(input.snapshotRegistry.epochsList().get(0))) {
            count++;
        }
        return count;
    }

    static List<Integer> createKeys(int numberOfEntries) {
        List<Integer> keys = new ArrayList<>(numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
            int key = (int) (0xffffffff & ((i * 2862933555777941757L) + 3037000493L));
            keys.add(key);
        }

        return keys;
    }
}
