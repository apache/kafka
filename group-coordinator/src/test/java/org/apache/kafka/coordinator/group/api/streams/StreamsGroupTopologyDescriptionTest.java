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
package org.apache.kafka.coordinator.group.api.streams;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StreamsGroupTopologyDescriptionTest {

    @Test
    public void testSourceEqualsAndHashCode() {
        StreamsGroupTopologyDescription.Source a = new StreamsGroupTopologyDescription.Source(
            "src", Set.of("in"), Set.of("proc"));
        StreamsGroupTopologyDescription.Source b = new StreamsGroupTopologyDescription.Source(
            "src", Set.of("in"), Set.of("proc"));
        StreamsGroupTopologyDescription.Source c = new StreamsGroupTopologyDescription.Source(
            "src", Set.of("in", "in2"), Set.of("proc"));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    public void testProcessorEqualsAndHashCode() {
        StreamsGroupTopologyDescription.Processor a = new StreamsGroupTopologyDescription.Processor(
            "p", Set.of("store"), Set.of("snk"));
        StreamsGroupTopologyDescription.Processor b = new StreamsGroupTopologyDescription.Processor(
            "p", Set.of("store"), Set.of("snk"));
        StreamsGroupTopologyDescription.Processor c = new StreamsGroupTopologyDescription.Processor(
            "p", Set.of(), Set.of("snk"));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    public void testSinkEqualsAndHashCode() {
        StreamsGroupTopologyDescription.Sink a = new StreamsGroupTopologyDescription.Sink(
            "snk", Optional.of("out"), Set.of());
        StreamsGroupTopologyDescription.Sink b = new StreamsGroupTopologyDescription.Sink(
            "snk", Optional.of("out"), Set.of());
        StreamsGroupTopologyDescription.Sink c = new StreamsGroupTopologyDescription.Sink(
            "snk", Optional.empty(), Set.of());

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    public void testTopologyEqualsAndHashCode() {
        StreamsGroupTopologyDescription a = buildSimpleTopology();
        StreamsGroupTopologyDescription b = buildSimpleTopology();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testCollectionAccessorsAreUnmodifiable() {
        StreamsGroupTopologyDescription.Source src = new StreamsGroupTopologyDescription.Source(
            "src", new HashSet<>(Set.of("in")), Set.of("proc"));
        assertThrows(UnsupportedOperationException.class, () -> src.topics().add("nope"));
        assertThrows(UnsupportedOperationException.class, () -> src.successors().add("nope"));
    }

    @Test
    public void testNullArgumentsRejected() {
        assertThrows(NullPointerException.class,
            () -> new StreamsGroupTopologyDescription.Source(null, Set.of(), Set.of()));
        assertThrows(NullPointerException.class,
            () -> new StreamsGroupTopologyDescription.Sink("s", null, Set.of()));
        assertThrows(NullPointerException.class,
            () -> new StreamsGroupTopologyDescription(null, List.of()));
    }

    @Test
    public void testNodeIsSealedToSourceProcessorSink() {
        Class<?>[] permitted = StreamsGroupTopologyDescription.Node.class.getPermittedSubclasses();
        assertNotNull(permitted, "Node must be a sealed interface");
        assertEquals(
            Set.of(
                StreamsGroupTopologyDescription.Source.class,
                StreamsGroupTopologyDescription.Processor.class,
                StreamsGroupTopologyDescription.Sink.class
            ),
            Set.of(permitted)
        );
    }

    private static StreamsGroupTopologyDescription buildSimpleTopology() {
        StreamsGroupTopologyDescription.Source source = new StreamsGroupTopologyDescription.Source(
            "src", Set.of("in"), Set.of("proc"));
        StreamsGroupTopologyDescription.Processor processor = new StreamsGroupTopologyDescription.Processor(
            "proc", Set.of("store"), Set.of("snk"));
        StreamsGroupTopologyDescription.Sink sink = new StreamsGroupTopologyDescription.Sink(
            "snk", Optional.of("out"), Set.of());
        StreamsGroupTopologyDescription.Subtopology sub = new StreamsGroupTopologyDescription.Subtopology(
            "0", List.of(source, processor, sink));
        return new StreamsGroupTopologyDescription(List.of(sub), List.of());
    }
}
