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

package org.apache.kafka.jmh.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TopicsImageSnapshotLoadBenchmark {
    @Param({"12500", "25000", "50000", "100000"})
    private int totalTopicCount;
    @Param({"10"})
    private int partitionsPerTopic;
    @Param({"3"})
    private int replicationFactor;
    @Param({"10000"})
    private int numReplicasPerBroker;

    private TopicsDelta topicsDelta;


    @Setup(Level.Trial)
    public void setup() {
        // build a delta to apply within the benchmark code
        // that consists of all the topics and partitions that would get loaded in a snapshot
        topicsDelta = getInitialTopicsDelta(totalTopicCount, partitionsPerTopic, replicationFactor, numReplicasPerBroker);
        System.out.print("(Loading a snapshot containing " + totalTopicCount + " total topics) ");
    }

    static TopicsDelta getInitialTopicsDelta(int totalTopicCount, int partitionsPerTopic, int replicationFactor, int numReplicasPerBroker) {
        int numBrokers = getNumBrokers(totalTopicCount, partitionsPerTopic, replicationFactor, numReplicasPerBroker);
        TopicsDelta buildupTopicsDelta = new TopicsDelta(TopicsImage.EMPTY);
        final AtomicInteger currentLeader = new AtomicInteger(0);
        IntStream.range(0, totalTopicCount).forEach(topicNumber -> {
            Uuid topicId = Uuid.randomUuid();
            buildupTopicsDelta.replay(new TopicRecord().setName("topic" + topicNumber).setTopicId(topicId));
            IntStream.range(0, partitionsPerTopic).forEach(partitionNumber -> {
                ArrayList<Integer> replicas = getReplicas(totalTopicCount, partitionsPerTopic, replicationFactor, numReplicasPerBroker, currentLeader.get());
                ArrayList<Integer> isr = new ArrayList<>(replicas);
                buildupTopicsDelta.replay(new PartitionRecord().
                    setPartitionId(partitionNumber).
                    setTopicId(topicId).
                    setReplicas(replicas).
                    setIsr(isr).
                    setRemovingReplicas(Collections.emptyList()).
                    setAddingReplicas(Collections.emptyList()).
                    setLeader(currentLeader.get()));
                currentLeader.set((1 + currentLeader.get()) % numBrokers);
            });
        });
        return buildupTopicsDelta;
    }

    static ArrayList<Integer> getReplicas(int totalTopicCount, int partitionsPerTopic, int replicationFactor, int numReplicasPerBroker, int currentLeader) {
        ArrayList<Integer> replicas = new ArrayList<>();
        int numBrokers = getNumBrokers(totalTopicCount, partitionsPerTopic, replicationFactor, numReplicasPerBroker);
        IntStream.range(0, replicationFactor).forEach(replicaNumber ->
            replicas.add((replicaNumber + currentLeader) % numBrokers));
        return replicas;
    }

    static int getNumBrokers(int totalTopicCount, int partitionsPerTopic, int replicationFactor, int numReplicasPerBroker) {
        int numBrokers = totalTopicCount * partitionsPerTopic * replicationFactor / numReplicasPerBroker;
        return numBrokers - numBrokers % 3;
    }

    @Benchmark
    public void testTopicsDeltaSnapshotLoad() {
        topicsDelta.apply();
    }
}
