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

package org.apache.kafka.jmh.server;

import kafka.server.EvictableKey;
import kafka.server.LastUsedKey;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FetchKeyBenchmark {

    private static final EvictableKey EVICTABLE_KEY_0 = new EvictableKey(true, 1000, 1000);
    private static final EvictableKey EVICTABLE_KEY_1 = new EvictableKey(true, 1000, 1000);

    private static final LastUsedKey LAST_USED_KEY_0 = new LastUsedKey(1000, 1000);
    private static final LastUsedKey LAST_USED_KEY_1 = new LastUsedKey(1000, 1000);

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public int compareEvictableKey() {
        return EVICTABLE_KEY_0.compareTo(EVICTABLE_KEY_1);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public int compareLastUsedKey() {
        return LAST_USED_KEY_0.compareTo(LAST_USED_KEY_1);
    }

}
