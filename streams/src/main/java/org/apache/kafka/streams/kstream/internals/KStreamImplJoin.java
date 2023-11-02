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
import org.apache.kafka.common.utils.Time;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSide;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.LeftOrRightValue;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSideSerde;
import org.apache.kafka.streams.state.internals.ListValueStoreBuilder;
import org.apache.kafka.streams.state.internals.LeftOrRightValueSerde;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;

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

        final StreamJoinedInternal<K, V1, V2> streamJoinedInternal = new StreamJoinedInternal<>(streamJoined);
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

        final StoreBuilder<WindowStore<K, V1>> thisWindowStore;
        final StoreBuilder<WindowStore<K, V2>> otherWindowStore;
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

        final KStreamJoinWindow<K, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.name());

        final ProcessorParameters<K, V1, ?, ?> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamProcessorName);
        final ProcessorGraphNode<K, V1> thisWindowedStreamsNode = new WindowedStreamProcessorNode<>(thisWindowStore.name(), thisWindowStreamProcessorParams);
        builder.addGraphNode(thisGraphNode, thisWindowedStreamsNode);

        final KStreamJoinWindow<K, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name());

        final ProcessorParameters<K, V2, ?, ?> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamProcessorName);
        final ProcessorGraphNode<K, V2> otherWindowedStreamsNode = new WindowedStreamProcessorNode<>(otherWindowStore.name(), otherWindowStreamProcessorParams);
        builder.addGraphNode(otherGraphNode, otherWindowedStreamsNode);

        Optional<StoreBuilder<KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<V1, V2>>>> outerJoinWindowStore = Optional.empty();
        if (leftOuter) {
            outerJoinWindowStore = Optional.of(sharedOuterJoinWindowStoreBuilder(windows, streamJoinedInternal, joinThisGeneratedName));
        }

        // Time-shared between joins to keep track of the maximum stream time
        final TimeTrackerSupplier sharedTimeTrackerSupplier = new TimeTrackerSupplier();

        final JoinWindowsInternal internalWindows = new JoinWindowsInternal(windows);
        final KStreamKStreamJoin<K, V1, V2, VOut> joinThis = new KStreamKStreamJoin<>(
            true,
            otherWindowStore.name(),
            internalWindows,
            joiner,
            leftOuter,
            outerJoinWindowStore.map(StoreBuilder::name),
            sharedTimeTrackerSupplier
        );

        final KStreamKStreamJoin<K, V2, V1, VOut> joinOther = new KStreamKStreamJoin<>(
            false,
            thisWindowStore.name(),
            internalWindows,
            AbstractStream.reverseJoinerWithKey(joiner),
            rightOuter,
            outerJoinWindowStore.map(StoreBuilder::name),
            sharedTimeTrackerSupplier
        );

        final KStreamKStreamSelfJoin<K, V1, V2, VOut> selfJoin = new KStreamKStreamSelfJoin<>(
            thisWindowStore.name(),
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

    private <K, V1, V2> String buildOuterJoinWindowStoreName(final StreamJoinedInternal<K, V1, V2> streamJoinedInternal, final String joinThisGeneratedName) {
        final String outerJoinSuffix = rightOuter ? "-outer-shared-join" : "-left-shared-join";

        if (streamJoinedInternal.thisStoreSupplier() != null && !streamJoinedInternal.thisStoreSupplier().name().isEmpty()) {
            return streamJoinedInternal.thisStoreSupplier().name() + outerJoinSuffix;
        } else if (streamJoinedInternal.storeName() != null) {
            return streamJoinedInternal.storeName() + outerJoinSuffix;
        } else {
            return KStreamImpl.OUTERSHARED_NAME
                + joinThisGeneratedName.substring(
                rightOuter
                    ? KStreamImpl.OUTERTHIS_NAME.length()
                    : KStreamImpl.JOINTHIS_NAME.length());
        }
    }

    private <K, V1, V2> StoreBuilder<KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<V1, V2>>> sharedOuterJoinWindowStoreBuilder(final JoinWindows windows,
                                                                                                                                              final StreamJoinedInternal<K, V1, V2> streamJoinedInternal,
                                                                                                                                              final String joinThisGeneratedName) {
        final boolean persistent = streamJoinedInternal.thisStoreSupplier() == null || streamJoinedInternal.thisStoreSupplier().get().persistent();
        final String storeName = buildOuterJoinWindowStoreName(streamJoinedInternal, joinThisGeneratedName) + "-store";

        // we are using a key-value store with list-values for the shared store, and have the window retention / grace period
        // handled totally on the processor node level, and hence here we are only validating these values but not using them at all
        final Duration retentionPeriod = Duration.ofMillis(windows.size() + windows.gracePeriodMs());
        final Duration windowSize = Duration.ofMillis(windows.size());
        final String rpMsgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        final long retentionMs = validateMillisecondDuration(retentionPeriod, rpMsgPrefix);
        final String wsMsgPrefix = prepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
        final long windowSizeMs = validateMillisecondDuration(windowSize, wsMsgPrefix);

        if (retentionMs < 0L) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        if (windowSizeMs < 0L) {
            throw new IllegalArgumentException("windowSize cannot be negative");
        }
        if (windowSizeMs > retentionMs) {
            throw new IllegalArgumentException("The retention period of the window store "
                    + storeName + " must be no smaller than its window size. Got size=["
                    + windowSizeMs + "], retention=[" + retentionMs + "]");
        }

        final TimestampedKeyAndJoinSideSerde<K> timestampedKeyAndJoinSideSerde = new TimestampedKeyAndJoinSideSerde<>(streamJoinedInternal.keySerde());
        final LeftOrRightValueSerde<V1, V2> leftOrRightValueSerde = new LeftOrRightValueSerde<>(streamJoinedInternal.valueSerde(), streamJoinedInternal.otherValueSerde());

        final StoreBuilder<KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<V1, V2>>> builder =
            new ListValueStoreBuilder<>(
                persistent ? Stores.persistentKeyValueStore(storeName) : Stores.inMemoryKeyValueStore(storeName),
                timestampedKeyAndJoinSideSerde,
                leftOrRightValueSerde,
                Time.SYSTEM
            );

        if (streamJoinedInternal.loggingEnabled()) {
            builder.withLoggingEnabled(streamJoinedInternal.logConfig());
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
