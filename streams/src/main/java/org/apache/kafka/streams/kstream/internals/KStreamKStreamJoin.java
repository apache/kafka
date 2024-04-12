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

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

import java.util.Optional;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTracker;
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTrackerSupplier;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.LeftOrRightValue;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSide;

abstract class KStreamKStreamJoin<K, VL, VR, VOut, VThis, VOther> implements ProcessorSupplier<K, VThis, K, VOut> {
    private final String otherWindowName;

    private final TimeTrackerSupplier sharedTimeTrackerSupplier;
    private final boolean enableSpuriousResultFix;
    private final Optional<String> outerJoinWindowName;

    protected final long joinBeforeMs;
    protected final long joinAfterMs;
    protected final long joinGraceMs;
    protected final long windowsBeforeMs;
    protected final long windowsAfterMs;
    protected final boolean outer;

    protected final ValueJoinerWithKey<? super K, ? super VThis, ? super VOther, ? extends VOut> joiner;

    KStreamKStreamJoin(final String otherWindowName, final TimeTrackerSupplier sharedTimeTrackerSupplier,
            final boolean enableSpuriousResultFix, final Optional<String> outerJoinWindowName, final long joinBeforeMs,
            final long joinAfterMs, final JoinWindowsInternal windows, final boolean outer,
            final ValueJoinerWithKey<? super K, ? super VThis, ? super VOther, ? extends VOut> joiner) {
        this.otherWindowName = otherWindowName;
        this.sharedTimeTrackerSupplier = sharedTimeTrackerSupplier;
        this.enableSpuriousResultFix = enableSpuriousResultFix;
        this.outerJoinWindowName = outerJoinWindowName;
        this.joinBeforeMs = joinBeforeMs;
        this.joinAfterMs = joinAfterMs;
        this.joinGraceMs = windows.gracePeriodMs();
        this.windowsBeforeMs = windows.beforeMs;
        this.windowsAfterMs = windows.afterMs;
        this.outer = outer;
        this.joiner = joiner;
    }

    protected abstract class KStreamKStreamJoinProcessor extends ContextualProcessor<K, VThis, K, VOut> {
        protected InternalProcessorContext<K, VOut> internalProcessorContext;
        protected Sensor droppedRecordsSensor;

        protected TimeTracker sharedTimeTracker;

        protected WindowStore<K, VOther> otherWindowStore;

        protected Optional<KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VL, VR>>> outerJoinStore = Optional.empty();


        @Override
        public void init(final ProcessorContext<K, VOut> context) {
            super.init(context);
            internalProcessorContext = (InternalProcessorContext<K, VOut>) context;
            final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            otherWindowStore = context.getStateStore(otherWindowName);
            sharedTimeTracker = sharedTimeTrackerSupplier.get(context.taskId());

            if (enableSpuriousResultFix) {
                outerJoinStore = outerJoinWindowName.map(context::getStateStore);

                sharedTimeTracker.setEmitInterval(
                        StreamsConfig.InternalConfig.getLong(
                                context.appConfigs(),
                                EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX,
                                1000L
                        )
                );
            }
        }

        @Override
        public void process(final Record<K, VThis> record) {

        }

        @Override
        public void close() {
            sharedTimeTrackerSupplier.remove(context().taskId());
        }
    }
}
