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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StreamStreamJoinNode;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class KStreamImplJoin {

    private final InternalStreamsBuilder builder;
    private final boolean leftOuter;
    private final boolean rightOuter;


    KStreamImplJoin(final InternalStreamsBuilder builder,
                    final boolean leftOuter,
                    final boolean rightOuter) {
        this.builder = builder;
        this.leftOuter = leftOuter;
        this.rightOuter = rightOuter;
    }

    public <K1, R, V1, V2> KStream<K1, R> join(final KStream<K1, V1> lhs,
                                               final KStream<K1, V2> other,
                                               final ValueJoinerWithKey<? super K1, ? super V1, ? super V2, ? extends R> joiner,
                                               final JoinWindows windows,
                                               final StreamJoined<K1, V1, V2> streamJoined) {

        final StreamJoinedInternal<K1, V1, V2> streamJoinedInternal = new StreamJoinedInternal<>(streamJoined);
        final NamedInternal renamed = new NamedInternal(streamJoinedInternal.name());
        final String joinThisSuffix = rightOuter ? "-outer-this-join" : "-this-join";
        final String joinOtherSuffix = leftOuter ? "-outer-other-join" : "-other-join";

        final String thisWindowStreamProcessorName = renamed.suffixWithOrElseGet(
            "-this-windowed", builder, KStreamImpl.WINDOWED_NAME);
        final String otherWindowStreamProcessorName = renamed.suffixWithOrElseGet(
            "-other-windowed", builder, KStreamImpl.WINDOWED_NAME);

        final String joinThisGeneratedName = rightOuter ? builder.newProcessorName(KStreamImpl.OUTERTHIS_NAME) : builder.newProcessorName(KStreamImpl.JOINTHIS_NAME);
        final String joinOtherGeneratedName = leftOuter ? builder.newProcessorName(KStreamImpl.OUTEROTHER_NAME) : builder.newProcessorName(KStreamImpl.JOINOTHER_NAME);

        final String joinThisName = renamed.suffixWithOrElseGet(joinThisSuffix, joinThisGeneratedName);
        final String joinOtherName = renamed.suffixWithOrElseGet(joinOtherSuffix, joinOtherGeneratedName);

        final String joinMergeName = renamed.suffixWithOrElseGet(
            "-merge", builder, KStreamImpl.MERGE_NAME);

        final GraphNode thisGraphNode = ((AbstractStream<?, ?>) lhs).graphNode;
        final GraphNode otherGraphNode = ((AbstractStream<?, ?>) other).graphNode;

        final StoreBuilder<WindowStore<K1, V1>> thisWindowStore;
        final StoreBuilder<WindowStore<K1, V2>> otherWindowStore;
        final String userProvidedBaseStoreName = streamJoinedInternal.storeName();

        final WindowBytesStoreSupplier thisStoreSupplier = streamJoinedInternal.thisStoreSupplier();
        final WindowBytesStoreSupplier otherStoreSupplier = streamJoinedInternal.otherStoreSupplier();

        assertUniqueStoreNames(thisStoreSupplier, otherStoreSupplier);

        if (thisStoreSupplier == null) {
            final String thisJoinStoreName = userProvidedBaseStoreName == null ? joinThisGeneratedName : userProvidedBaseStoreName + joinThisSuffix;
            thisWindowStore = joinWindowStoreBuilder(thisJoinStoreName, windows, streamJoinedInternal.keySerde(), streamJoinedInternal.valueSerde(), streamJoinedInternal.loggingEnabled(), streamJoinedInternal.logConfig());
        } else {
            assertWindowSettings(thisStoreSupplier, windows);
            thisWindowStore = joinWindowStoreBuilderFromSupplier(thisStoreSupplier, streamJoinedInternal.keySerde(), streamJoinedInternal.valueSerde());
        }

        if (otherStoreSupplier == null) {
            final String otherJoinStoreName = userProvidedBaseStoreName == null ? joinOtherGeneratedName : userProvidedBaseStoreName + joinOtherSuffix;
            otherWindowStore = joinWindowStoreBuilder(otherJoinStoreName, windows, streamJoinedInternal.keySerde(), streamJoinedInternal.otherValueSerde(), streamJoinedInternal.loggingEnabled(), streamJoinedInternal.logConfig());
        } else {
            assertWindowSettings(otherStoreSupplier, windows);
            otherWindowStore = joinWindowStoreBuilderFromSupplier(otherStoreSupplier, streamJoinedInternal.keySerde(), streamJoinedInternal.otherValueSerde());
        }

        final KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.name());

        final ProcessorParameters<K1, V1, ?, ?> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamProcessorName);
        final ProcessorGraphNode<K1, V1> thisWindowedStreamsNode = new ProcessorGraphNode<>(thisWindowStreamProcessorName, thisWindowStreamProcessorParams);
        builder.addGraphNode(thisGraphNode, thisWindowedStreamsNode);

        final KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name());

        final ProcessorParameters<K1, V2, ?, ?> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamProcessorName);
        final ProcessorGraphNode<K1, V2> otherWindowedStreamsNode = new ProcessorGraphNode<>(otherWindowStreamProcessorName, otherWindowStreamProcessorParams);
        builder.addGraphNode(otherGraphNode, otherWindowedStreamsNode);

        final KStreamKStreamJoin<K1, R, V1, V2> joinThis = new KStreamKStreamJoin<>(
            otherWindowStore.name(),
            windows.beforeMs,
            windows.afterMs,
            joiner,
            leftOuter
        );

        final KStreamKStreamJoin<K1, R, V2, V1> joinOther = new KStreamKStreamJoin<>(
            thisWindowStore.name(),
            windows.afterMs,
            windows.beforeMs,
            AbstractStream.reverseJoinerWithKey(joiner),
            rightOuter
        );

        final PassThrough<K1, R> joinMerge = new PassThrough<>();

        final StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

        final ProcessorParameters<K1, V1, ?, ?> joinThisProcessorParams = new ProcessorParameters<>(joinThis, joinThisName);
        final ProcessorParameters<K1, V2, ?, ?> joinOtherProcessorParams = new ProcessorParameters<>(joinOther, joinOtherName);
        final ProcessorParameters<K1, R, ?, ?> joinMergeProcessorParams = new ProcessorParameters<>(joinMerge, joinMergeName);

        joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
                   .withJoinThisProcessorParameters(joinThisProcessorParams)
                   .withJoinOtherProcessorParameters(joinOtherProcessorParams)
                   .withThisWindowStoreBuilder(thisWindowStore)
                   .withOtherWindowStoreBuilder(otherWindowStore)
                   .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                   .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                   .withValueJoiner(joiner)
                   .withNodeName(joinMergeName);

        final GraphNode joinGraphNode = joinBuilder.build();

        builder.addGraphNode(Arrays.asList(thisGraphNode, otherGraphNode), joinGraphNode);

        final Set<String> allSourceNodes = new HashSet<>(((KStreamImpl<K1, V1>) lhs).subTopologySourceNodes);
        allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).subTopologySourceNodes);

        // do not have serde for joined result;
        // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
        return new KStreamImpl<>(joinMergeName, streamJoinedInternal.keySerde(), null, allSourceNodes, false, joinGraphNode, builder);
    }

    private void assertWindowSettings(final WindowBytesStoreSupplier supplier, final JoinWindows joinWindows) {
        if (!supplier.retainDuplicates()) {
            throw new StreamsException("The StoreSupplier must set retainDuplicates=true, found retainDuplicates=false");
        }
        final boolean allMatch = supplier.retentionPeriod() == (joinWindows.size() + joinWindows.gracePeriodMs()) &&
            supplier.windowSize() == joinWindows.size();
        if (!allMatch) {
            throw new StreamsException(String.format("Window settings mismatch. WindowBytesStoreSupplier settings %s must match JoinWindows settings %s" +
                                                         " for the window size and retention period", supplier, joinWindows));
        }
    }

    private void assertUniqueStoreNames(final WindowBytesStoreSupplier supplier,
                                        final WindowBytesStoreSupplier otherSupplier) {

        if (supplier != null
            && otherSupplier != null
            && supplier.name().equals(otherSupplier.name())) {
            throw new StreamsException("Both StoreSuppliers have the same name.  StoreSuppliers must provide unique names");
        }
    }

    private static <K, V> StoreBuilder<WindowStore<K, V>> joinWindowStoreBuilder(final String storeName,
                                                                                 final JoinWindows windows,
                                                                                 final Serde<K> keySerde,
                                                                                 final Serde<V> valueSerde,
                                                                                 final boolean loggingEnabled,
                                                                                 final Map<String, String> logConfig) {
        final StoreBuilder<WindowStore<K, V>> builder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                storeName + "-store",
                Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                Duration.ofMillis(windows.size()),
                true
            ),
            keySerde,
            valueSerde
        );
        if (loggingEnabled) {
            builder.withLoggingEnabled(logConfig);
        } else {
            builder.withLoggingDisabled();
        }

        return builder;
    }

    private static <K, V> StoreBuilder<WindowStore<K, V>> joinWindowStoreBuilderFromSupplier(final WindowBytesStoreSupplier storeSupplier,
                                                                                             final Serde<K> keySerde,
                                                                                             final Serde<V> valueSerde) {
        return Stores.windowStoreBuilder(
            storeSupplier,
            keySerde,
            valueSerde
        );
    }
}
