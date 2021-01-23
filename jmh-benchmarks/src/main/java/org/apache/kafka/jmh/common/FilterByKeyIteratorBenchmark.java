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

package org.apache.kafka.jmh.common;

import org.apache.kafka.common.utils.FilterByKeyIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class FilterByKeyIteratorBenchmark {
    private static final List<String> LIST = new ArrayList<>(IntStream.range(0, 100).mapToObj(String::valueOf).collect(Collectors.toList()));
    private static final Predicate<String> PREDICATE = s -> s.equals("aa");

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public String benchmarkFilterByKeyIterator() {
        String last = null;
        Iterable<String> iterable = () -> new FilterByKeyIterator<>(LIST.iterator(), PREDICATE);
        for (String s : iterable) {
            last = s;
        }
        return last;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public String benchmarkStreamIterator() {
        String last = null;
        Iterable<String> iterable = () -> LIST.stream().filter(PREDICATE).iterator();
        for (String s : iterable) {
            last = s;
        }
        return last;
    }

}
