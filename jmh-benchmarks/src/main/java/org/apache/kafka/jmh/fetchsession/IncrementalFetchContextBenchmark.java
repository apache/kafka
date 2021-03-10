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

package org.apache.kafka.jmh.fetchsession;

import kafka.server.CachedPartition;
import kafka.server.FetchSession;
import kafka.server.IncrementalFetchContext;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Time;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class IncrementalFetchContextBenchmark {

    private final List<FetchResponseData.PartitionData> partitionData = partitionData();
    private final IncrementalFetchContext fetchContext = incrementalFetchContext();

    private List<FetchResponseData.FetchableTopicResponse> topicResponses;

    // updateAndGenerateResponseData remove fields from topicResponses so we have to initialize topicResponses for each invocation
    @Setup(Level.Invocation)
    public void setup() {
        topicResponses = topicResponses();
    }

    private List<FetchResponseData.FetchableTopicResponse> topicResponses() {
        List<FetchResponseData.FetchableTopicResponse> topicResponses = new LinkedList<>();
        topicResponses.add(new FetchResponseData.FetchableTopicResponse()
            .setTopic("foo")
            .setPartitions(new LinkedList<>(partitionData)));
        return topicResponses;
    }

    private static List<FetchResponseData.PartitionData> partitionData() {
        return Collections.unmodifiableList(IntStream.range(0, 2000)
            .mapToObj(i -> new FetchResponseData.PartitionData()
                .setPartitionIndex(i)
                .setLastStableOffset(0)
                .setLogStartOffset(0))
                .collect(Collectors.toList()));
    }

    private static IncrementalFetchContext incrementalFetchContext() {
        FetchMetadata metadata = Mockito.mock(FetchMetadata.class);
        Mockito.when(metadata.epoch()).thenReturn(100);

        CachedPartition cp = Mockito.mock(CachedPartition.class);
        Mockito.when(cp.maybeUpdateResponseData(
                Mockito.any(FetchResponseData.PartitionData.class),
                Mockito.anyBoolean())).thenAnswer(invocation -> {
            FetchResponseData.PartitionData partitionData = invocation.getArgument(0);
            return partitionData.partitionIndex() % 2 == 0;
        });

        FetchSession session = Mockito.mock(FetchSession.class);
        Mockito.when(session.epoch()).thenReturn(101);

        return new IncrementalFetchContext(Time.SYSTEM, metadata, session) {
            @Override
            public CachedPartition cachedPartition(String topic, int partition) {
                return cp;
            }

            @Override
            public void update(CachedPartition cp) {

            }
        };
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public FetchResponse updateAndGenerateResponseData() {
        return fetchContext.updateAndGenerateResponseData(topicResponses);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public int getResponseSize() {
        return fetchContext.getResponseSize(topicResponses, FetchResponseData.HIGHEST_SUPPORTED_VERSION);
    }
}
