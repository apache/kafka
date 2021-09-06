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

package org.apache.kafka.jmh.streams.statestores.windowed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.state.Stores;
import org.openjdk.jmh.annotations.*;

import java.time.Duration;

@State(Scope.Thread)
@Fork(value = 1)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.Throughput)
public class InMemoryWindowStoreBenchmark extends WindowedStoreBenchmark {

    @Setup(Level.Trial)
    public void setUp() {
        generateWindowedKeys();
        Stores.windowStoreBuilder(Stores.inMemoryWindowStore("in-memory", Duration.ofDays(1), Duration.ofMinutes(WINDOW_SIZE), false),
            Serdes.String(),
            Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();
        ProcessorContextImpl context = (ProcessorContextImpl) setupProcessorContext();
        this.windowStore.init((StateStoreContext) context, this.windowStore);
        putWindowedKeys();
    }
}
