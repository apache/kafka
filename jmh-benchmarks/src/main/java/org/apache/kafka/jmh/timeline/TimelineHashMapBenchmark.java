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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TimelineHashMapBenchmark {
    private final static Random random = new Random();
    private static int[] source;

    @Param("1000000")
    private int size;

    public enum MapType {
        HASH_MAP,
        SCALA_HASH_MAP,
        TIMELINE_MAP,
        TIMELINE_SNAPSHOT_MAP,
    }

    @Setup(Level.Trial)
    public void setup() {
        source = createSource(size);
    }

    @State(Scope.Thread)
    public static class Input {
        @Param({ "HASH_MAP", "SCALA_HASH_MAP", "TIMELINE_MAP", "TIMELINE_SNAPSHOT_MAP" })
        MapType mapType;

        SnapshotRegistry snapshotRegistry;
        Map<Integer, Integer> emptyMap;
        Map<Integer, Integer> fullMap;

        @Setup(Level.Invocation)
        public void setup() {
            createMaps();
            for (int value : source) {
                fullMap.put(value, value);
            }
        }

        void createMaps() {
            switch(mapType) {
                case HASH_MAP:
                    emptyMap = new HashMap<>();
                    fullMap = new HashMap<>();
                    break;
                case SCALA_HASH_MAP:
                    emptyMap = new ScalaHashMap(new scala.collection.immutable.HashMap<>());
                    fullMap = new ScalaHashMap(new scala.collection.immutable.HashMap<>());
                    break;
                case TIMELINE_MAP:
                case TIMELINE_SNAPSHOT_MAP:
                    snapshotRegistry = new SnapshotRegistry(new LogContext());
                    emptyMap = new TimelineHashMap<>(snapshotRegistry, 16);
                    fullMap = new TimelineHashMap<>(snapshotRegistry, 16);
                    break;
            }
        }
    }

    @Benchmark
    public boolean testAddEntries(Input input) {
        final boolean snapshotMap = input.mapType == MapType.TIMELINE_SNAPSHOT_MAP;

        long epoch = 0;
        int j = 0;

        for (int value : source) {
            if (snapshotMap) {
                if (j > 1_000) {
                    input.snapshotRegistry.deleteSnapshotsUpTo(epoch - 10_000);
                    input.snapshotRegistry.createSnapshot(epoch);
                    j = 0;
                } else {
                    j++;
                }
                epoch++;
            }

            input.emptyMap.put(value, value);
        }

        return input.emptyMap.isEmpty();
    }

    @Benchmark
    public boolean testUpdateEntries(Input input) {
        final boolean snapshotMap = input.mapType == MapType.TIMELINE_SNAPSHOT_MAP;

        long epoch = 0;
        int j = 0;

        for (int value : source) {
            if (snapshotMap) {
                if (j > 1_000) {
                    input.snapshotRegistry.deleteSnapshotsUpTo(epoch - 10_000);
                    input.snapshotRegistry.createSnapshot(epoch);
                    j = 0;
                } else {
                    j++;
                }
                epoch++;
            }

            input.fullMap.put(value, value);
        }

        return input.fullMap.isEmpty();
    }

    @Benchmark
    public boolean testRemoveEntries(Input input) {
        final boolean snapshotMap = input.mapType == MapType.TIMELINE_SNAPSHOT_MAP;

        long epoch = 0;
        int j = 0;

        for (int value : source) {
            if (snapshotMap) {
                if (j > 1_000) {
                    input.snapshotRegistry.deleteSnapshotsUpTo(epoch - 10_000);
                    input.snapshotRegistry.createSnapshot(epoch);
                    j = 0;
                } else {
                    j++;
                }
                epoch++;
            }

            input.fullMap.remove(value);
        }

        return input.fullMap.isEmpty();
    }

    @Benchmark
    public int testIterateEntries(Input input) {
        int count = 0;

        switch(input.mapType) {
            case HASH_MAP:
            case TIMELINE_MAP:
                for (Map.Entry<Integer, Integer> entry : input.fullMap.entrySet()) {
                    count++;
                }
                break;
            case TIMELINE_SNAPSHOT_MAP:
                long epoch = input.snapshotRegistry.epochsList().get(0);
                for (Map.Entry<Integer, Integer> entry : ((TimelineHashMap<Integer, Integer>) input.fullMap).entrySet(epoch)) {
                    count++;
                }
                break;
            case SCALA_HASH_MAP:
                scala.collection.Iterator<scala.Tuple2<Integer, Integer>> iterator = ((ScalaHashMap) input.fullMap).map.iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    count++;
                }
                break;
        }

        return count;
    }

    @Benchmark
    public int testGetEntries(Input input) {
        final boolean snapshotMap = input.mapType == MapType.TIMELINE_SNAPSHOT_MAP;

        TimelineHashMap<Integer, Integer> timeline = null;
        long epoch = 0;
        if (snapshotMap) {
            epoch = input.snapshotRegistry.epochsList().get(0);
            timeline = ((TimelineHashMap<Integer, Integer>) input.fullMap);
        }

        int sum = 0;
        for (int value : source) {
            if (snapshotMap) {
                sum += timeline.get(value, epoch);
            } else {
                sum += input.fullMap.get(value);
            }
        }

        return sum;
    }

    static int[] createSource(int numberOfEntries) {
        return IntStream
            .range(0, numberOfEntries)
            .map(i -> random.nextInt())
            .toArray();
    }

    static final class ScalaHashMap implements Map<Integer, Integer> {
        scala.collection.immutable.HashMap<Integer, Integer> map;

        ScalaHashMap(scala.collection.immutable.HashMap<Integer, Integer> map) {
            this.map = map;
        }

        @Override
        public Integer get(Object key) {
            return map.apply((Integer) key);
        }

        @Override
        public Integer put(Integer key, Integer value) {
            map = map.updated(key, value);
            return value;
        }


        @Override
        public Integer remove(Object key) {
            map = map.removed((Integer) key);
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public void putAll(Map<? extends Integer, ? extends Integer> source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Map.Entry<Integer, Integer>> entrySet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Integer> keySet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Integer> values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsKey(Object key) {
            throw new UnsupportedOperationException();
        }
    }
}
