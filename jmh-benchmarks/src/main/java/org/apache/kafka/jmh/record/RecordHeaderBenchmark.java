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
package org.apache.kafka.jmh.record;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RecordHeaderBenchmark {

    private final ByteBuffer keyBuffer = ByteBuffer.wrap("key".getBytes(StandardCharsets.UTF_8));
    private final ByteBuffer valueBuffer = ByteBuffer.wrap("value".getBytes(StandardCharsets.UTF_8));

    @Benchmark
    public void key() {
        new RecordHeader(keyBuffer, valueBuffer).key();
    }

    @Benchmark
    public void value() {
        new RecordHeader(keyBuffer, valueBuffer).value();
    }

}
