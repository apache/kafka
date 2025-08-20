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

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.internals.KStreamSessionWindowAggregate;
import org.apache.kafka.streams.kstream.internals.KStreamWindowAggregate;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

import org.junit.jupiter.api.Test;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.utils.TestUtils.mockStoreFactory;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class GraphGraceSearchUtilTest {
    @Test
    public void shouldThrowOnNull() {
        try {
            GraphGraceSearchUtil.findAndVerifyWindowGrace(null);
            fail("Should have thrown.");
        } catch (final TopologyException e) {
            assertThat(e.getMessage(), is("Invalid topology: Window close time is only defined for windowed computations. Got []."));
        }
    }

    @Test
    public void shouldFailIfThereIsNoGraceAncestor() {
        // doesn't matter if this ancestor is stateless or stateful. The important thing it that there is
        // no grace period defined on any ancestor of the node
        final ProcessorGraphNode<String, Long> gracelessAncestor = new ProcessorGraphNode<>(
            "graceless",
            new ProcessorParameters<>(
                () -> new Processor<String, Long, String, Long>() {
                    @Override
                    public void process(final Record<String, Long> record) {}

                },
                "graceless"
            )
        );

        final ProcessorGraphNode<String, Long> node = new ProcessorGraphNode<>(
            "stateless",
            new ProcessorParameters<>(
                () -> new Processor<String, Long, String, Long>() {

                    @Override
                    public void process(final Record<String, Long> record) {}

                },
                "stateless"
            )
        );

        gracelessAncestor.addChild(node);

        try {
            GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
            fail("should have thrown.");
        } catch (final TopologyException e) {
            assertThat(e.getMessage(), is("Invalid topology: Window close time is only defined for windowed computations. Got [graceless->stateless]."));
        }
    }

    @Test
    public void shouldExtractGraceFromKStreamWindowAggregateNode() {
        final TimeWindows windows = TimeWindows.ofSizeAndGrace(ofMillis(10L), ofMillis(1234L));
        final ProcessorGraphNode<String, Long> node = new GracePeriodGraphNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamWindowAggregate<String, Long, Integer, TimeWindow>(
                    windows,
                    mockStoreFactory("asdf"),
                    EmitStrategy.onWindowUpdate(),
                    null,
                    null
                ),
                "asdf"
            ),
            windows.gracePeriodMs()
        );

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(1234L));
    }

    @Test
    public void shouldExtractGraceFromKStreamSessionWindowAggregateNode() {
        final SessionWindows windows = SessionWindows.ofInactivityGapAndGrace(ofMillis(10L), ofMillis(1234L));

        final ProcessorGraphNode<String, Long> node = new GracePeriodGraphNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamSessionWindowAggregate<String, Long, Integer>(
                    windows,
                    mockStoreFactory("asdf"),
                    EmitStrategy.onWindowUpdate(),
                    null,
                    null,
                    null
                ),
                "asdf"
            ),
            windows.gracePeriodMs() + windows.inactivityGap()
        );

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(1244L));
    }

    @Test
    public void shouldExtractGraceFromSessionAncestorThroughStatefulParent() {
        final SessionWindows windows = SessionWindows.ofInactivityGapAndGrace(ofMillis(10L), ofMillis(1234L));
        final ProcessorGraphNode<String, Long> graceGrandparent = new GracePeriodGraphNode<>(
            "asdf",
            new ProcessorParameters<>(new KStreamSessionWindowAggregate<String, Long, Integer>(
                windows, mockStoreFactory("asdf"), EmitStrategy.onWindowUpdate(), null, null, null
            ), "asdf"),
            windows.gracePeriodMs() + windows.inactivityGap()
        );

        final ProcessorGraphNode<String, Long> statefulParent = new ProcessorGraphNode<>(
            "stateful",
            new ProcessorParameters<>(
                () -> new Processor<String, Long, String, Long>() {

                    @Override
                    public void process(final Record<String, Long> record) {}

                },
                "dummy"
            )
        );
        graceGrandparent.addChild(statefulParent);

        final ProcessorGraphNode<String, Long> node = new ProcessorGraphNode<>(
            "stateless",
            new ProcessorParameters<>(
                () -> new Processor<String, Long, String, Long>() {

                    @Override
                    public void process(final Record<String, Long> record) {}

                },
                "dummyChild-graceless"
            )
        );
        statefulParent.addChild(node);

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(1244L));
    }

    @Test
    public void shouldExtractGraceFromSessionAncestorThroughStatelessParent() {
        final SessionWindows windows = SessionWindows.ofInactivityGapAndGrace(ofMillis(10L), ofMillis(1234L));
        final ProcessorGraphNode<String, Long> graceGrandparent = new GracePeriodGraphNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamSessionWindowAggregate<String, Long, Integer>(
                    windows,
                    mockStoreFactory("asdf"),
                    EmitStrategy.onWindowUpdate(),
                    null,
                    null,
                    null
                ),
                "asdf"
            ),
            windows.gracePeriodMs() + windows.inactivityGap()
        );

        final ProcessorGraphNode<String, Long> statelessParent = new ProcessorGraphNode<>(
            "statelessParent",
            new ProcessorParameters<>(
                () -> new Processor<String, Long, String, Long>() {

                    @Override
                    public void process(final Record<String, Long> record) {}

                },
                "statelessParent"
            )
        );
        graceGrandparent.addChild(statelessParent);

        final ProcessorGraphNode<String, Long> node = new ProcessorGraphNode<>(
            "stateless",
            new ProcessorParameters<>(
                () -> new Processor<String, Long, String, Long>() {

                    @Override
                    public void process(final Record<String, Long> record) {}

                },
                "stateless"
            )
        );
        statelessParent.addChild(node);

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(1244L));
    }

    @Test
    public void shouldUseMaxIfMultiParentsDoNotAgreeOnGrace() {
        final SessionWindows leftWindows = SessionWindows.ofInactivityGapAndGrace(ofMillis(10L), ofMillis(1234L));
        final ProcessorGraphNode<String, Long> leftParent = new GracePeriodGraphNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamSessionWindowAggregate<String, Long, Integer>(
                    leftWindows,
                    mockStoreFactory("asdf"),
                    EmitStrategy.onWindowUpdate(),
                    null,
                    null,
                    null
                ),
                "asdf"
            ),
            leftWindows.gracePeriodMs() + leftWindows.inactivityGap()
        );

        final TimeWindows rightWindows = TimeWindows.ofSizeAndGrace(ofMillis(10L), ofMillis(4321L));
        final ProcessorGraphNode<String, Long> rightParent = new GracePeriodGraphNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamWindowAggregate<String, Long, Integer, TimeWindow>(
                    rightWindows,
                    mockStoreFactory("asdf"),
                    EmitStrategy.onWindowUpdate(),
                    null,
                    null
                ),
                "asdf"
            ),
            rightWindows.gracePeriodMs()
        );

        final ProcessorGraphNode<String, Long> node = new ProcessorGraphNode<>(
            "stateless",
            new ProcessorParameters<>(
                () -> new Processor<String, Long, String, Long>() {

                    @Override
                    public void process(final Record<String, Long> record) {}

                },
                "stateless"
            )
        );
        leftParent.addChild(node);
        rightParent.addChild(node);

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(4321L));
    }

}
