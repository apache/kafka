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
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KStreamSessionWindowAggregate;
import org.apache.kafka.streams.kstream.internals.KStreamWindowAggregate;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.TypedProcessor;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.Test;

import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

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
        final StatefulProcessorNode<String, Long, String, Long> gracelessAncestor = new StatefulProcessorNode<>(
            "stateful",
            new ProcessorParameters<>(
                () -> new TypedProcessor<String, Long, String, Long>() {
                    @Override
                    public void process(final String key, final Long value) {}
                },
                "dummy"
            ),
            (StoreBuilder<? extends StateStore>) null
        );

        final ProcessorGraphNode<String, Long, String, Long> node = new ProcessorGraphNode<>("stateless", null);
        gracelessAncestor.addChild(node);

        try {
            GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
            fail("should have thrown.");
        } catch (final TopologyException e) {
            assertThat(e.getMessage(), is("Invalid topology: Window close time is only defined for windowed computations. Got [stateful->stateless]."));
        }
    }

    @Test
    public void shouldExtractGraceFromKStreamWindowAggregateNode() {
        final TimeWindows windows = TimeWindows.of(ofMillis(10L)).grace(ofMillis(1234L));
        final StatefulProcessorNode<String, Long, Windowed<String>, Change<Integer>> node = new StatefulProcessorNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamWindowAggregate<String, Long, Integer, TimeWindow>(
                    windows,
                    "asdf",
                    null,
                    null
                ),
                "asdf"
            ),
            (StoreBuilder<? extends StateStore>) null
        );

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(windows.gracePeriodMs()));
    }

    @Test
    public void shouldExtractGraceFromKStreamSessionWindowAggregateNode() {
        final SessionWindows windows = SessionWindows.with(ofMillis(10L)).grace(ofMillis(1234L));

        final StatefulProcessorNode<String, Long, Windowed<String>, Change<Integer>> node = new StatefulProcessorNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamSessionWindowAggregate<String, Long, Integer>(
                    windows,
                    "asdf",
                    null,
                    null,
                    null
                ),
                "asdf"
            ),
            (StoreBuilder<? extends StateStore>) null
        );

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(windows.gracePeriodMs() + windows.inactivityGap()));
    }

    @Test
    public void shouldExtractGraceFromSessionAncestorThroughStatefulParent() {
        final SessionWindows windows = SessionWindows.with(ofMillis(10L)).grace(ofMillis(1234L));
        final StatefulProcessorNode<String, Long, Windowed<String>, Change<Integer>> graceGrandparent =
            new StatefulProcessorNode<>(
                "asdf",
                new ProcessorParameters<>(new KStreamSessionWindowAggregate<>(
                    windows, "asdf", null, null, null
                ), "asdf"),
                (StoreBuilder<? extends StateStore>) null
            );

        final StatefulProcessorNode<Windowed<String>, Change<Integer>, String, Long> statefulParent = new StatefulProcessorNode<>(
            "stateful",
            new ProcessorParameters<>(
                () -> new TypedProcessor<Windowed<String>, Change<Integer>, String, Long>() {
                    @Override
                    public void process(final Windowed<String> key, final Change<Integer> value) {

                    }
                },
                "dummy"
            ),
            (StoreBuilder<? extends StateStore>) null
        );
        graceGrandparent.addChild(statefulParent);

        final ProcessorGraphNode<String, Long, Object, Object> node = new ProcessorGraphNode<>("stateless", null);
        statefulParent.addChild(node);

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(windows.gracePeriodMs() + windows.inactivityGap()));
    }

    @Test
    public void shouldExtractGraceFromSessionAncestorThroughStatelessParent() {
        final SessionWindows windows = SessionWindows.with(ofMillis(10L)).grace(ofMillis(1234L));
        final StatefulProcessorNode<String, Long, Windowed<String>, Change<Integer>> graceGrandparent = new StatefulProcessorNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamSessionWindowAggregate<String, Long, Integer>(
                    windows,
                    "asdf",
                    null,
                    null,
                    null
                ),
                "asdf"
            ),
            (StoreBuilder<? extends StateStore>) null
        );

        final ProcessorGraphNode<Windowed<String>, Change<Integer>, Object, Object> statelessParent = new ProcessorGraphNode<>("stateless", null);
        graceGrandparent.addChild(statelessParent);

        final ProcessorGraphNode<Object, Object, Object, Object> node = new ProcessorGraphNode<>("stateless", null);
        statelessParent.addChild(node);

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(windows.gracePeriodMs() + windows.inactivityGap()));
    }

    @Test
    public void shouldUseMaxIfMultiParentsDoNotAgreeOnGrace() {
        final StatefulProcessorNode<String, Long, Windowed<String>, Change<Integer>> leftParent = new StatefulProcessorNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamSessionWindowAggregate<String, Long, Integer>(
                    SessionWindows.with(ofMillis(10L)).grace(ofMillis(1234L)),
                    "asdf",
                    null,
                    null,
                    null
                ),
                "asdf"
            ),
            (StoreBuilder<? extends StateStore>) null
        );

        final StatefulProcessorNode<String, Long, Windowed<String>, Change<Integer>> rightParent = new StatefulProcessorNode<>(
            "asdf",
            new ProcessorParameters<>(
                new KStreamWindowAggregate<String, Long, Integer, TimeWindow>(
                    TimeWindows.of(ofMillis(10L)).grace(ofMillis(4321L)),
                    "asdf",
                    null,
                    null
                ),
                "asdf"
            ),
            (StoreBuilder<? extends StateStore>) null
        );

        final ProcessorGraphNode<Windowed<String>, Change<Integer>, Object, Object> node = new ProcessorGraphNode<>("stateless", null);
        leftParent.addChild(node);
        rightParent.addChild(node);

        final long extracted = GraphGraceSearchUtil.findAndVerifyWindowGrace(node);
        assertThat(extracted, is(4321L));
    }

}
