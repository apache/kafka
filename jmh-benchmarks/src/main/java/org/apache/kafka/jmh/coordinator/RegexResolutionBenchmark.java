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
package org.apache.kafka.jmh.coordinator;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupMetadataManager;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

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
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class RegexResolutionBenchmark {
    private static final Logger LOG = new LogContext().logger(RegexResolutionBenchmark.class);
    private static final Time TIME = Time.SYSTEM;
    private static final String GROUP_ID = "my-group-id";

    private static final List<String> WORDS = List.of(
        "data",
        "stream",
        "queue",
        "analytics",
        "service",
        "event",
        "log",
        "cloud",
        "process",
        "system",
        "message",
        "broker",
        "partition",
        "key",
        "value",
        "cluster",
        "zookeeper",
        "replication",
        "topic",
        "producer"
    );

    @Param({"10000", "100000", "1000000"})
    private int topicCount;

    @Param({"1", "10", "100"})
    private int regexCount;

    private MetadataImage image;

    private Set<String> regexes;

    @Setup(Level.Trial)
    public void setup() {
        Random random = new Random();

        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        for (int i = 0; i < topicCount; i++) {
            String topicName =
                WORDS.get(random.nextInt(WORDS.size())) + "_" +
                WORDS.get(random.nextInt(WORDS.size())) + "_" +
                i;

            delta.replay(new TopicRecord()
                .setTopicId(Uuid.randomUuid())
                .setName(topicName));
        }
        image = delta.apply(MetadataProvenance.EMPTY);

        regexes = new HashSet<>();
        for (int i = 0; i < regexCount; i++) {
            regexes.add(".*" + WORDS.get(random.nextInt(WORDS.size())) + ".*");
        }
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void run() {
        GroupMetadataManager.refreshRegularExpressions(
            GROUP_ID,
            LOG,
            TIME,
            image,
            regexes
        );
    }
}
