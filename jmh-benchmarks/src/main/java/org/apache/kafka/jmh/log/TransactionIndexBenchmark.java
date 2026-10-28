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
package org.apache.kafka.jmh.log;

import org.apache.kafka.common.message.AbortedTxn;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.log.TxnIndexSearchResult;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TransactionIndexBenchmark {
    @Param({"16", "1024", "65536"})
    private int indexSize;

    @Param({"0", "50", "99"})
    private int fetchPercent;

    @Param({"1", "64"})
    private int scanSize;

    private File file;
    private TransactionIndex index;
    private long fetchOffset;
    private long upperBoundOffset;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        file = Files.createTempFile("transaction-index-benchmark", ".txnindex").toFile();
        index = new TransactionIndex(0L, file);
        for (int i = 0; i < indexSize; i++) {
            long firstOffset = i * 4L;
            index.append(new AbortedTxn()
                    .setProducerId(i)
                    .setFirstOffset(firstOffset)
                    .setLastOffset(firstOffset + 1)
                    .setLastStableOffset(firstOffset + 2));
        }
        long fetchIndex = (indexSize - 1L) * fetchPercent / 100;
        fetchOffset = fetchIndex * 4L + 1;
        upperBoundOffset = fetchIndex * 4L + scanSize * 4L;
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        index.close();
        Files.deleteIfExists(file.toPath());
    }

    @Benchmark
    public TxnIndexSearchResult collectAbortedTxns() {
        return index.collectAbortedTxns(fetchOffset, upperBoundOffset);
    }
}
