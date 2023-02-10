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

import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BranchedKStreamImpl<K, V> implements BranchedKStream<K, V> {

    private static final String BRANCH_NAME = "KSTREAM-BRANCH-";

    private final KStreamImpl<K, V> source;
    private final boolean repartitionRequired;
    private final String splitterName;
    private final Map<String, KStream<K, V>> outputBranches = new HashMap<>();

    private final List<Predicate<? super K, ? super V>> predicates = new ArrayList<>();
    private final List<String> childNames = new ArrayList<>();
    private final ProcessorGraphNode<K, V> splitterNode;

    BranchedKStreamImpl(final KStreamImpl<K, V> source, final boolean repartitionRequired, final NamedInternal named) {
        this.source = source;
        this.repartitionRequired = repartitionRequired;
        this.splitterName = named.orElseGenerateWithPrefix(source.builder, BRANCH_NAME);

        // predicates and childNames are passed by reference so when the user adds a branch they get added to
        final ProcessorParameters<K, V, ?, ?> processorParameters =
                new ProcessorParameters<>(new KStreamBranch<>(predicates, childNames), splitterName);
        splitterNode = new ProcessorGraphNode<>(splitterName, processorParameters);
        source.builder.addGraphNode(source.graphNode, splitterNode);
    }

    @Override
    public BranchedKStream<K, V> branch(final Predicate<? super K, ? super V> predicate) {
        return branch(predicate, BranchedInternal.empty());
    }

    @Override
    public BranchedKStream<K, V> branch(final Predicate<? super K, ? super V> predicate, final Branched<K, V> branched) {
        predicates.add(predicate);
        createBranch(branched, predicates.size());
        return this;
    }

    @Override
    public Map<String, KStream<K, V>> defaultBranch() {
        return defaultBranch(BranchedInternal.empty());
    }

    @Override
    public Map<String, KStream<K, V>> defaultBranch(final Branched<K, V> branched) {
        createBranch(branched, 0);
        return outputBranches;
    }

    private void createBranch(final Branched<K, V> branched, final int index) {
        final BranchedInternal<K, V> branchedInternal = new BranchedInternal<>(branched);
        final String branchChildName = getBranchChildName(index, branchedInternal);
        childNames.add(branchChildName);
        source.builder.newProcessorName(branchChildName);
        final ProcessorParameters<K, V, ?, ?> parameters = new ProcessorParameters<>(new PassThrough<>(), branchChildName);
        final ProcessorGraphNode<K, V> branchChildNode = new ProcessorGraphNode<>(branchChildName, parameters);
        source.builder.addGraphNode(splitterNode, branchChildNode);
        final KStreamImpl<K, V> branch = new KStreamImpl<>(branchChildName, source.keySerde,
                source.valueSerde, source.subTopologySourceNodes,
                repartitionRequired, branchChildNode, source.builder);
        process(branch, branchChildName, branchedInternal);
    }

    private String getBranchChildName(final int index, final BranchedInternal<K, V> branchedInternal) {
        if (branchedInternal.name() == null) {
            return splitterName + index;
        } else {
            return splitterName + branchedInternal.name();
        }
    }

    private void process(final KStreamImpl<K, V> branch, final String branchChildName,
                         final BranchedInternal<K, V> branchedInternal) {
        if (branchedInternal.chainFunction() != null) {
            final KStream<K, V> transformedStream = branchedInternal.chainFunction().apply(branch);
            if (transformedStream != null) {
                outputBranches.put(branchChildName, transformedStream);
            }
        } else if (branchedInternal.chainConsumer() != null) {
            branchedInternal.chainConsumer().accept(branch);
        } else {
            outputBranches.put(branchChildName, branch);
        }
    }

    @Override
    public Map<String, KStream<K, V>> noDefaultBranch() {
        return outputBranches;
    }
}
