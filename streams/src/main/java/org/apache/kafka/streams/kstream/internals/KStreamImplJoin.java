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

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.apache.kafka.streams.kstream.internals.graph.WindowedStreamProcessorNode;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StoreBuilderWrapper;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class KStreamImplJoin {

    private final InternalStreamsBuilder builder;
    private final boolean leftOuter;
    private final boolean rightOuter;

    static class TimeTrackerSupplier {
        private final Map<TaskId, TimeTracker> tracker = new ConcurrentHashMap<>();

        public TimeTracker get(final TaskId taskId) {
            return tracker.computeIfAbsent(taskId, taskId1 -> new TimeTracker());
        }

        public void remove(final TaskId taskId) {
            tracker.remove(taskId);
        }
    }

    static class TimeTracker {
        private long emitIntervalMs = 1000L;
        long streamTime = ConsumerRecord.NO_TIMESTAMP;
        long minTime = Long.MAX_VALUE;
        long nextTimeToEmit;

        public void setEmitInterval(final long emitIntervalMs) {
            this.emitIntervalMs = emitIntervalMs;
        }

        public void advanceStreamTime(final long recordTimestamp) {
            streamTime = Math.max(recordTimestamp, streamTime);
        }

        public void updatedMinTime(final long recordTimestamp) {
            minTime = Math.min(recordTimestamp, minTime);
        }

        public void advanceNextTimeToEmit() {
            nextTimeToEmit += emitIntervalMs;
        }
    }

    KStreamImplJoin(final InternalStreamsBuilder builder,
                    final boolean leftOuter,
                    final boolean rightOuter) {
        this.builder = builder;
        this.leftOuter = leftOuter;
        this.rightOuter = rightOuter;
    }

    public <K, V1, V2, VOut> KStream<K, VOut> join(final KStream<K, V1> lhs,
                                                   final KStream<K, V2> other,
                                                   final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends VOut> joiner,
                                                   final JoinWindows windows,
                                                   final StreamJoined<K, V1, V2> streamJoined) {

        final StreamJoinedInternal<K, V1, V2> streamJoinedInternal = new StreamJoinedInternal<>(streamJoined, builder);
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

        final StoreFactory thisWindowStore;
        final StoreFactory otherWindowStore;
        final String userProvidedBaseStoreName = streamJoinedInternal.storeName();

        final WindowBytesStoreSupplier thisStoreSupplier = streamJoinedInternal.thisStoreSupplier();
        final WindowBytesStoreSupplier otherStoreSupplier = streamJoinedInternal.otherStoreSupplier();

        assertUniqueStoreNames(thisStoreSupplier, otherStoreSupplier);

        // specific store suppliers takes precedence over the "dslStoreSuppliers", which is only used
        // if no specific store supplier is specified for this store
        if (thisStoreSupplier == null) {
            final String thisJoinStoreName = userProvidedBaseStoreName == null ? joinThisGeneratedName : userProvidedBaseStoreName + joinThisSuffix;
            thisWindowStore = new StreamJoinedStoreFactory<>(thisJoinStoreName, windows, streamJoinedInternal, StreamJoinedStoreFactory.Type.THIS);
        } else {
            assertWindowSettings(thisStoreSupplier, windows);
            thisWindowStore = joinWindowStoreBuilderFromSupplier(thisStoreSupplier, streamJoinedInternal.keySerde(), streamJoinedInternal.valueSerde());
        }

        if (otherStoreSupplier == null) {
            final String otherJoinStoreName = userProvidedBaseStoreName == null ? joinOtherGeneratedName : userProvidedBaseStoreName + joinOtherSuffix;
            otherWindowStore = new StreamJoinedStoreFactory<>(otherJoinStoreName, windows, streamJoinedInternal, StreamJoinedStoreFactory.Type.OTHER);
        } else {
            assertWindowSettings(otherStoreSupplier, windows);
            otherWindowStore = joinWindowStoreBuilderFromSupplier(otherStoreSupplier, streamJoinedInternal.keySerde(), streamJoinedInternal.otherValueSerde());
        }

        final KStreamJoinWindow<K, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.storeName());

        final ProcessorParameters<K, V1, ?, ?> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamProcessorName);
        final ProcessorGraphNode<K, V1> thisWindowedStreamsNode = new WindowedStreamProcessorNode<>(thisWindowStore.storeName(), thisWindowStreamProcessorParams);
        builder.addGraphNode(thisGraphNode, thisWindowedStreamsNode);

        final KStreamJoinWindow<K, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.storeName());

        final ProcessorParameters<K, V2, ?, ?> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamProcessorName);
        final ProcessorGraphNode<K, V2> otherWindowedStreamsNode = new WindowedStreamProcessorNode<>(otherWindowStore.storeName(), otherWindowStreamProcessorParams);
        builder.addGraphNode(otherGraphNode, otherWindowedStreamsNode);

        Optional<StoreFactory> outerJoinWindowStore = Optional.empty();
        if (leftOuter) {
            outerJoinWindowStore = Optional.of(new OuterStreamJoinStoreFactory<>(
                    joinThisGeneratedName,
                    streamJoinedInternal,
                    windows,
                    rightOuter ? OuterStreamJoinStoreFactory.Type.RIGHT : OuterStreamJoinStoreFactory.Type.LEFT)
            );
        }

        // Time-shared between joins to keep track of the maximum stream time
        final TimeTrackerSupplier sharedTimeTrackerSupplier = new TimeTrackerSupplier();

        final JoinWindowsInternal internalWindows = new JoinWindowsInternal(windows);
        final KStreamKStreamJoinLeftSide<K, V1, V2, VOut> joinThis = new KStreamKStreamJoinLeftSide<>(
            otherWindowStore.storeName(),
            internalWindows,
            joiner,
            leftOuter,
            outerJoinWindowStore.map(StoreFactory::storeName),
            sharedTimeTrackerSupplier
        );

        final KStreamKStreamJoinRightSide<K, V1, V2, VOut> joinOther = new KStreamKStreamJoinRightSide<>(
            thisWindowStore.storeName(),
            internalWindows,
            AbstractStream.reverseJoinerWithKey(joiner),
            rightOuter,
            outerJoinWindowStore.map(StoreFactory::storeName),
            sharedTimeTrackerSupplier
        );

        final KStreamKStreamSelfJoin<K, V1, V2, VOut> selfJoin = new KStreamKStreamSelfJoin<>(
            thisWindowStore.storeName(),
            internalWindows,
            joiner,
            windows.size() + windows.gracePeriodMs()
        );

        final PassThrough<K, VOut> joinMerge = new PassThrough<>();

        final StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K, V1, V2, VOut> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

        final ProcessorParameters<K, V1, ?, ?> joinThisProcessorParams = new ProcessorParameters<>(joinThis, joinThisName);
        final ProcessorParameters<K, V2, ?, ?> joinOtherProcessorParams = new ProcessorParameters<>(joinOther, joinOtherName);
        final ProcessorParameters<K, VOut, ?, ?> joinMergeProcessorParams = new ProcessorParameters<>(joinMerge, joinMergeName);
        final ProcessorParameters<K, V1, ?, ?> selfJoinProcessorParams = new ProcessorParameters<>(selfJoin, joinMergeName);

        joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
                   .withJoinThisProcessorParameters(joinThisProcessorParams)
                   .withJoinOtherProcessorParameters(joinOtherProcessorParams)
                   .withThisWindowStoreBuilder(thisWindowStore)
                   .withOtherWindowStoreBuilder(otherWindowStore)
                   .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                   .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                   .withOuterJoinWindowStoreBuilder(outerJoinWindowStore)
                   .withValueJoiner(joiner)
                   .withNodeName(joinMergeName)
                   .withSelfJoinProcessorParameters(selfJoinProcessorParams);

        if (internalWindows.spuriousResultFixEnabled()) {
            joinBuilder.withSpuriousResultFixEnabled();
        }

        final GraphNode joinGraphNode = joinBuilder.build();

        if (leftOuter || rightOuter) {
            joinGraphNode.addLabel(GraphNode.Label.NULL_KEY_RELAXED_JOIN);
        }
        builder.addGraphNode(Arrays.asList(thisGraphNode, otherGraphNode), joinGraphNode);

        final Set<String> allSourceNodes = new HashSet<>(((KStreamImpl<K, V1>) lhs).subTopologySourceNodes);
        allSourceNodes.addAll(((KStreamImpl<K, V2>) other).subTopologySourceNodes);

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

    private static <K, V> StoreFactory joinWindowStoreBuilderFromSupplier(final WindowBytesStoreSupplier storeSupplier,
                                                                          final Serde<K> keySerde,
                                                                          final Serde<V> valueSerde) {
        return StoreBuilderWrapper.wrapStoreBuilder(Stores.windowStoreBuilder(
            storeSupplier,
            keySerde,
            valueSerde
        ));
    }
}
