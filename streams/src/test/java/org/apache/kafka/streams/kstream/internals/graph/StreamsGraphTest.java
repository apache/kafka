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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StreamsGraphTest {
    
    private String expectedJoinedTopology = "Topologies:\n"
                                            + "   Sub-topology: 0\n"
                                            + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                            + "      --> KSTREAM-WINDOWED-0000000002\n"
                                            + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                            + "      --> KSTREAM-WINDOWED-0000000003\n"
                                            + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                            + "      --> KSTREAM-JOINTHIS-0000000004\n"
                                            + "      <-- KSTREAM-SOURCE-0000000000\n"
                                            + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                            + "      --> KSTREAM-JOINOTHER-0000000005\n"
                                            + "      <-- KSTREAM-SOURCE-0000000001\n"
                                            + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                            + "      --> KSTREAM-MERGE-0000000006\n"
                                            + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                            + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                            + "      --> KSTREAM-MERGE-0000000006\n"
                                            + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                            + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                            + "      --> none\n"
                                            + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n\n";

    private String expectedJoinedFilteredTopology = "Topologies:\n"
                                                    + "   Sub-topology: 0\n"
                                                    + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                                    + "      --> KSTREAM-WINDOWED-0000000002\n"
                                                    + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                                    + "      --> KSTREAM-WINDOWED-0000000003\n"
                                                    + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                                    + "      --> KSTREAM-JOINTHIS-0000000004\n"
                                                    + "      <-- KSTREAM-SOURCE-0000000000\n"
                                                    + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                                    + "      --> KSTREAM-JOINOTHER-0000000005\n"
                                                    + "      <-- KSTREAM-SOURCE-0000000001\n"
                                                    + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                                    + "      --> KSTREAM-MERGE-0000000006\n"
                                                    + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                                    + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                                    + "      --> KSTREAM-MERGE-0000000006\n"
                                                    + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                                    + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                                    + "      --> KSTREAM-FILTER-0000000007\n"
                                                    + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n"
                                                    + "    Processor: KSTREAM-FILTER-0000000007 (stores: [])\n"
                                                    + "      --> none\n"
                                                    + "      <-- KSTREAM-MERGE-0000000006\n\n";

    private String expectedFullTopology = "Topologies:\n"
                                          + "   Sub-topology: 0\n"
                                          + "    Source: KSTREAM-SOURCE-0000000000 (topics: [topic])\n"
                                          + "      --> KSTREAM-WINDOWED-0000000002\n"
                                          + "    Source: KSTREAM-SOURCE-0000000001 (topics: [other-topic])\n"
                                          + "      --> KSTREAM-WINDOWED-0000000003\n"
                                          + "    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                          + "      --> KSTREAM-JOINTHIS-0000000004\n"
                                          + "      <-- KSTREAM-SOURCE-0000000000\n"
                                          + "    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                          + "      --> KSTREAM-JOINOTHER-0000000005\n"
                                          + "      <-- KSTREAM-SOURCE-0000000001\n"
                                          + "    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])\n"
                                          + "      --> KSTREAM-MERGE-0000000006\n"
                                          + "      <-- KSTREAM-WINDOWED-0000000003\n"
                                          + "    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])\n"
                                          + "      --> KSTREAM-MERGE-0000000006\n"
                                          + "      <-- KSTREAM-WINDOWED-0000000002\n"
                                          + "    Processor: KSTREAM-MERGE-0000000006 (stores: [])\n"
                                          + "      --> KSTREAM-FILTER-0000000007\n"
                                          + "      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005\n"
                                          + "    Processor: KSTREAM-FILTER-0000000007 (stores: [])\n"
                                          + "      --> KSTREAM-MAPVALUES-0000000008\n"
                                          + "      <-- KSTREAM-MERGE-0000000006\n"
                                          + "    Processor: KSTREAM-MAPVALUES-0000000008 (stores: [])\n"
                                          + "      --> KSTREAM-SINK-0000000009\n"
                                          + "      <-- KSTREAM-FILTER-0000000007\n"
                                          + "    Sink: KSTREAM-SINK-0000000009 (topic: output-topic)\n"
                                          + "      <-- KSTREAM-MAPVALUES-0000000008\n\n";

    // Test builds topology in succesive manner but only graph node not yet processed written to topology

    @Test
    public void shouldBeAbleToBuildTopologyIncrementally() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> stream = builder.stream("topic");
        final KStream<String, String> streamII = builder.stream("other-topic");
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;


        final KStream<String, String> joinedStream = stream.join(streamII, valueJoiner, JoinWindows.of(5000));

        // build step one
        assertEquals(expectedJoinedTopology, builder.build().describe().toString());

        final KStream<String, String> filteredJoinStream = joinedStream.filter((k, v) -> v.equals("foo"));
        // build step two
        assertEquals(expectedJoinedFilteredTopology, builder.build().describe().toString());

        filteredJoinStream.mapValues(v -> v + "some value").to("output-topic");
        // build step three
        assertEquals(expectedFullTopology, builder.build().describe().toString());


    }

}
