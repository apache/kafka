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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class KBranchedStream<K, V> {

    private final ProcessorGraphNode<K, V> parentNode;
    private final List<Predicate<K, V>> predicates;
    private final List<String> childNames;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;
    private final Set<String> sourceNodes;
    private final boolean repartitionRequired;
    private final InternalStreamsBuilder builder;

    KBranchedStream(final ProcessorGraphNode<K, V> parentNode, final List<Predicate<K, V>> predicates,
                    final List<String> childNames, final Serde<K> keySerde, final Serde<V> valSerde,
                    final Set<String> sourceNodes, final boolean repartitionRequired, final InternalStreamsBuilder builder) {
        this.parentNode = parentNode;
        this.predicates = predicates;
        this.childNames = childNames;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.sourceNodes = sourceNodes;
        this.repartitionRequired = repartitionRequired;
        this.builder = builder;
    }

    public KBranchedStream<K, V> addBranch(final Predicate<K, V> predicate, final Consumer<KStream<K, V>> streamBranch) {
        add(predicate, streamBranch);
        return this;
    }

    public void defaultBranch(final Predicate<K, V> predicate, final Consumer<KStream<K, V>> streamBranch) {
        add(predicate, streamBranch);
    }

    private void add(final Predicate<K, V> predicate, final Consumer<KStream<K, V>> streamBranch) {
        final String branchChildName = builder.newProcessorName("***BRANCH_CHILD***");
        predicates.add(predicate);
        childNames.add(branchChildName);

        final ProcessorParameters<K, V> parameters = new ProcessorParameters<>(new KStreamPassThrough<>(), branchChildName);
        final ProcessorGraphNode<K, V> branchChildNode = new ProcessorGraphNode<>(branchChildName, parameters);

        builder.addGraphNode(parentNode, branchChildNode);
        final KStreamImpl<K, V> newStream = new KStreamImpl<>(branchChildName, keySerde, valSerde, sourceNodes, repartitionRequired, branchChildNode, builder);
        streamBranch.accept(newStream);
    }
}

