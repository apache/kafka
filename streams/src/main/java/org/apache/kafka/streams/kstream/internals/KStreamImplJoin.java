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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StreamStreamJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

class KStreamImplJoin {

    private KStreamImpl kStreamImpl;
    private final boolean leftOuter;
    private final boolean rightOuter;


    KStreamImplJoin(final KStreamImpl kStreamImpl,
                    final boolean leftOuter,
                    final boolean rightOuter) {
        this.kStreamImpl = kStreamImpl;
        this.leftOuter = leftOuter;
        this.rightOuter = rightOuter;
    }

    public <K1, R, V1, V2> KStream<K1, R> join(final KStream<K1, V1> lhs,
                                               final KStream<K1, V2> other,
                                               final ValueJoiner<? super V1, ? super V2, ? extends R> joiner,
                                               final JoinWindows windows,
                                               final StreamJoined<K1, V1, V2> streamJoined) {

        final StreamJoinedInternal<K1, V1, V2> streamJoinedInternal = new StreamJoinedInternal<>(streamJoined);
        final NamedInternal renamed = new NamedInternal(streamJoinedInternal.name());
        final String joinThisSuffix = rightOuter ? "-outer-this-join" : "-this-join";
        final String joinOtherSuffix = leftOuter ? "-outer-other-join" : "-other-join";

        final String thisWindowStreamName = renamed.suffixWithOrElseGet(
            "-this-windowed", kStreamImpl.builder, KStreamImpl.WINDOWED_NAME);
        final String otherWindowStreamName = renamed.suffixWithOrElseGet(
            "-other-windowed", kStreamImpl.builder, KStreamImpl.WINDOWED_NAME);

        final String joinThisGeneratedName = rightOuter ? kStreamImpl.builder.newProcessorName(KStreamImpl.OUTERTHIS_NAME) : kStreamImpl.builder.newProcessorName(KStreamImpl.JOINTHIS_NAME);
        final String joinOtherGeneratedName = leftOuter ? kStreamImpl.builder.newProcessorName(KStreamImpl.OUTEROTHER_NAME) : kStreamImpl.builder.newProcessorName(KStreamImpl.JOINOTHER_NAME);

        final String joinThisName = renamed.suffixWithOrElseGet(joinThisSuffix, joinThisGeneratedName);
        final String joinOtherName = renamed.suffixWithOrElseGet(joinOtherSuffix, joinOtherGeneratedName);

        final String joinMergeName = renamed.suffixWithOrElseGet(
            "-merge", kStreamImpl.builder, KStreamImpl.MERGE_NAME);

        final StreamsGraphNode thisStreamsGraphNode = ((AbstractStream) lhs).streamsGraphNode;
        final StreamsGraphNode otherStreamsGraphNode = ((AbstractStream) other).streamsGraphNode;

        final StoreBuilder<WindowStore<K1, V1>> thisWindowStore;
        final StoreBuilder<WindowStore<K1, V2>> otherWindowStore;
        final String userProvidedBaseStoreName = streamJoinedInternal.storeName();

        final String thisJoinStoreName = userProvidedBaseStoreName == null ? joinThisGeneratedName : userProvidedBaseStoreName + joinThisSuffix;
        final String otherJoinStoreName = userProvidedBaseStoreName == null ? joinOtherGeneratedName : userProvidedBaseStoreName + joinOtherSuffix;

        final WindowBytesStoreSupplier thisStoreSupplier = streamJoinedInternal.thisStoreSupplier();
        final WindowBytesStoreSupplier otherStoreSupplier = streamJoinedInternal.otherStoreSupplier();

        assertUniqueStoreNames(thisStoreSupplier, otherStoreSupplier);

        if (thisStoreSupplier == null) {
            thisWindowStore = KStreamImpl.joinWindowStoreBuilder(thisJoinStoreName, windows, streamJoinedInternal.keySerde(), streamJoinedInternal.valueSerde());
        } else {
            assertWindowSettings(thisStoreSupplier, windows);
            thisWindowStore = KStreamImpl.joinWindowStoreBuilderFromSupplier(thisStoreSupplier, streamJoinedInternal.keySerde(), streamJoinedInternal.valueSerde());
        }

        if (otherStoreSupplier == null) {
            otherWindowStore = KStreamImpl.joinWindowStoreBuilder(otherJoinStoreName, windows, streamJoinedInternal.keySerde(), streamJoinedInternal.otherValueSerde());
        } else {
            assertWindowSettings(otherStoreSupplier, windows);
            otherWindowStore = KStreamImpl.joinWindowStoreBuilderFromSupplier(otherStoreSupplier, streamJoinedInternal.keySerde(), streamJoinedInternal.otherValueSerde());
        }

        final KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.name());

        final ProcessorParameters<K1, V1> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamName);
        final ProcessorGraphNode<K1, V1> thisWindowedStreamsNode = new ProcessorGraphNode<>(thisWindowStreamName, thisWindowStreamProcessorParams);
        kStreamImpl.builder.addGraphNode(thisStreamsGraphNode, thisWindowedStreamsNode);

        final KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name());

        final ProcessorParameters<K1, V2> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamName);
        final ProcessorGraphNode<K1, V2> otherWindowedStreamsNode = new ProcessorGraphNode<>(otherWindowStreamName, otherWindowStreamProcessorParams);
        kStreamImpl.builder.addGraphNode(otherStreamsGraphNode, otherWindowedStreamsNode);

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
            AbstractStream.reverseJoiner(joiner),
            rightOuter
        );

        final KStreamPassThrough<K1, R> joinMerge = new KStreamPassThrough<>();

        final StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

        final ProcessorParameters<K1, V1> joinThisProcessorParams = new ProcessorParameters<>(joinThis, joinThisName);
        final ProcessorParameters<K1, V2> joinOtherProcessorParams = new ProcessorParameters<>(joinOther, joinOtherName);
        final ProcessorParameters<K1, R> joinMergeProcessorParams = new ProcessorParameters<>(joinMerge, joinMergeName);

        joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
                   .withJoinThisProcessorParameters(joinThisProcessorParams)
                   .withJoinOtherProcessorParameters(joinOtherProcessorParams)
                   .withThisWindowStoreBuilder(thisWindowStore)
                   .withOtherWindowStoreBuilder(otherWindowStore)
                   .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                   .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                   .withValueJoiner(joiner)
                   .withNodeName(joinMergeName);

        final StreamsGraphNode joinGraphNode = joinBuilder.build();

        kStreamImpl.builder.addGraphNode(Arrays.asList(thisStreamsGraphNode, otherStreamsGraphNode), joinGraphNode);

        final Set<String> allSourceNodes = new HashSet<>(((KStreamImpl<K1, V1>) lhs).sourceNodes);
        allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).sourceNodes);

        // do not have serde for joined result;
        // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
        return new KStreamImpl<>(joinMergeName, streamJoinedInternal.keySerde(), null, allSourceNodes, false, joinGraphNode, kStreamImpl.builder);
    }

    private void assertWindowSettings(final WindowBytesStoreSupplier supplier, final JoinWindows joinWindows) {
        final long joinGrace = joinWindows.gracePeriodMs() < 0 ? 0 : joinWindows.gracePeriodMs();
        final boolean allMatch = supplier.retentionPeriod() == (joinWindows.size() + joinGrace) &&
            supplier.windowSize() == joinWindows.size() && supplier.retainDuplicates();
        if (!allMatch) {
            throw new StreamsException(String.format("Window settings mismatch. WindowBytesStoreSupplier settings %s must match JoinWindows settings %s", supplier, joinWindows));
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
}
