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
import org.apache.kafka.streams.kstream.EnumerableWindowDefinition;
import org.apache.kafka.streams.kstream.internals.KStreamSessionWindowAggregate;
import org.apache.kafka.streams.kstream.internals.KStreamWindowAggregate;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public final class GraphGraceSearchUtil {
    private GraphGraceSearchUtil() {}

    public static long findAndVerifyWindowGrace(final StreamsGraphNode streamsGraphNode) {
        return findAndVerifyWindowGrace(streamsGraphNode, "");
    }

    private static long findAndVerifyWindowGrace(final StreamsGraphNode streamsGraphNode, final String chain) {
        // error base case: we traversed off the end of the graph without finding a window definition
        if (streamsGraphNode == null) {
            throw new TopologyException(
                "Window close time is only defined for windowed computations. Got [" + chain + "]."
            );
        }
        // base case: return if this node defines a grace period.
        {
            final Long gracePeriod = extractGracePeriod(streamsGraphNode);
            if (gracePeriod != null) {
                return gracePeriod;
            }
        }

        final String newChain = chain.equals("") ? streamsGraphNode.nodeName() : streamsGraphNode.nodeName() + "->" + chain;

        if (streamsGraphNode.parentNodes().isEmpty()) {
            // error base case: we traversed to the end of the graph without finding a window definition
            throw new TopologyException(
                "Window close time is only defined for windowed computations. Got [" + newChain + "]."
            );
        }

        // recursive case: all parents must define a grace period, and we use the max of our parents' graces.
        long inheritedGrace = -1;
        for (final StreamsGraphNode parentNode : streamsGraphNode.parentNodes()) {
            final long parentGrace = findAndVerifyWindowGrace(parentNode, newChain);
            inheritedGrace = Math.max(inheritedGrace, parentGrace);
        }

        if (inheritedGrace == -1) {
            throw new IllegalStateException(); // shouldn't happen, and it's not a legal grace period
        }

        return inheritedGrace;
    }

    private static Long extractGracePeriod(final StreamsGraphNode node) {
        if (node instanceof StatefulProcessorNode) {
            final ProcessorSupplier processorSupplier = ((StatefulProcessorNode) node).processorParameters().processorSupplier();
            if (processorSupplier instanceof KStreamWindowAggregate) {
                final KStreamWindowAggregate kStreamWindowAggregate = (KStreamWindowAggregate) processorSupplier;
                final EnumerableWindowDefinition windows = kStreamWindowAggregate.windows();
                return windows.gracePeriodMs();
            } else if (processorSupplier instanceof KStreamSessionWindowAggregate) {
                final KStreamSessionWindowAggregate kStreamSessionWindowAggregate = (KStreamSessionWindowAggregate) processorSupplier;
                final SessionWindows windows = kStreamSessionWindowAggregate.windows();
                return windows.gracePeriodMs() + windows.inactivityGap();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }
}
