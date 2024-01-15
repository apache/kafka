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

package org.apache.kafka.jmh.connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.ReplaceField;
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * This benchmark tests the performance of the {@link ReplaceField} {@link org.apache.kafka.connect.transforms.Transformation SMT}
 * when configured with a large number of include and exclude fields and applied on a {@link SourceRecord} containing a similarly
 * large number of fields.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReplaceFieldBenchmark {

    @Param({"100", "1000", "10000"})
    private int valueFieldCount;
    @Param({"1", "100", "10000"})
    private int includeExcludeFieldCount;
    private ReplaceField<SourceRecord> replaceFieldSmt;
    private SourceRecord record;

    @Setup(Level.Trial)
    public void setup() {
        this.replaceFieldSmt = new ReplaceField.Value<>();
        Map<String, String> replaceFieldConfigs = new HashMap<>();
        replaceFieldConfigs.put("exclude",
                IntStream.range(0, 2 * includeExcludeFieldCount).filter(x -> (x & 1) == 0).mapToObj(x -> "Field-" + x).collect(Collectors.joining(",")));
        replaceFieldConfigs.put("include",
                IntStream.range(0, 2 * includeExcludeFieldCount).filter(x -> (x & 1) == 1).mapToObj(x -> "Field-" + x).collect(Collectors.joining(",")));
        replaceFieldSmt.configure(replaceFieldConfigs);

        Map<String, Object> value = new HashMap<>();
        IntStream.range(0, valueFieldCount).forEach(x -> value.put("Field-" + x, new Object()));
        this.record = new SourceRecord(null, null, null, null, null, value);
    }

    @Benchmark
    public void includeExcludeReplaceFieldBenchmark() {
        replaceFieldSmt.apply(record);
    }
}
