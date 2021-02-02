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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.HashMap;
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

    @Benchmark
    public Map<Integer, String> testAddEntriesInHashMap() {
        HashMap<Integer, String> map = new HashMap<>(NUM_ENTRIES);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            int key = (int) (0xffffffff & ((i * 2862933555777941757L) + 3037000493L));
            map.put(key, String.valueOf(key));
        }
        return map;
    }

    @Benchmark
    public Map<Integer, String> testAddEntriesInTimelineMap() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Integer, String> map =
            new TimelineHashMap<>(snapshotRegistry, NUM_ENTRIES);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            int key = (int) (0xffffffff & ((i * 2862933555777941757L) + 3037000493L));
            map.put(key, String.valueOf(key));
        }
        return map;
    }

    @Benchmark
    public Map<Integer, String> testAddEntriesWithSnapshots() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<Integer, String> map =
            new TimelineHashMap<>(snapshotRegistry, NUM_ENTRIES);
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
}
