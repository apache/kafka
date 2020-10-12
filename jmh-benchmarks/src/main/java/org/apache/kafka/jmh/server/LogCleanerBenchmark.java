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

import kafka.api.ApiVersion;
import kafka.log.AppendOrigin;
import kafka.log.CleanedTransactionMetadata;
import kafka.log.Cleaner;
import kafka.log.CleanerStats;
import kafka.log.Log;
import kafka.log.LogConfig;
import kafka.log.LogUtils;
import kafka.log.SkimpyOffsetMap;
import kafka.utils.MockScheduler;
import kafka.utils.MockTime;
import kafka.utils.Scheduler;
import kafka.utils.TestUtils;
import kafka.utils.Throttler;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import scala.collection.immutable.Set$;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(value = Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
public class LogCleanerBenchmark {

    @Param({"10485760"})
    public int memory;

    @Param({"10485760", "104857600", "524288000", "1073741824"})
    public int segmentSize;

    @Param({"10", "100", "1000", "2000", "4000"})
    public int numKeys;

    @Param({"10"})
    public int keyLength;

    private Random random = new Random();

    private SkimpyOffsetMap skimpyOffsetMap;
    private Cleaner cleaner;
    private CleanerStats cleanerStats;
    private Log log;

    @Setup(Level.Invocation)
    public void setupTrial() {
        Time time = new MockTime();
        Scheduler scheduler = new MockScheduler(time);
        skimpyOffsetMap = new SkimpyOffsetMap(memory);
        Throttler throttler = new Throttler(Double.MAX_VALUE, Long.MAX_VALUE,
            true, "throttler", "entries", time);
        int maxMessageSize = 65536;
        cleaner = new Cleaner(0, skimpyOffsetMap, maxMessageSize, maxMessageSize,
            0.75, throttler, time, tp -> BoxedUnit.UNIT);
        cleanerStats = new CleanerStats(time);
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp(), segmentSize);
        logProps.put(LogConfig.CleanupPolicyProp(), LogConfig.Compact());
        logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp(), Long.toString(Long.MAX_VALUE));
        LogConfig config = new LogConfig(logProps, Set$.MODULE$.empty());

        File dir = TestUtils.randomPartitionLogDir(TestUtils.tempDir());

        log = LogUtils.createLog(dir, config, time, scheduler, 0);

        Set<String> keysSet = new HashSet<>(numKeys);
        for (int i = 0; i < numKeys; ++i) {
            String nextRandom;
            do {
                nextRandom = nextRandomString(keyLength);
            } while (keysSet.contains(nextRandom));
            keysSet.add(nextRandom);
        }
        String[] keys = new ArrayList<>(keysSet).toArray(new String[]{});

        long value = 0;
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(
                TestUtils.singletonRecords(
                    String.valueOf(value).getBytes(Charset.defaultCharset()),
                    keys[random.nextInt(numKeys)].getBytes(Charset.defaultCharset()),
                    CompressionType.NONE,
                    RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE
                    ), 0, AppendOrigin.Client$.MODULE$, ApiVersion.latestVersion());
            value++;
        }
    }

    private String nextRandomString(int length) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        return random.ints(leftLimit, rightLimit + 1)
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    @Benchmark
    @Threads(1)
    public void benchmarkLogCleanerBenchmark() {
        cleaner.buildOffsetMap(log, log.logStartOffset(), log.logEndOffset(), skimpyOffsetMap, cleanerStats);
        cleaner.cleanSegments(log, log.logSegments().toSeq(), skimpyOffsetMap, 0L, cleanerStats, new CleanedTransactionMetadata());
    }
}
