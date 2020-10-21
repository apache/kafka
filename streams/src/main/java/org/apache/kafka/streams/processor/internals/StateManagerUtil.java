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
package org.apache.kafka.streams.processor.internals;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.slf4j.Logger;

import static org.apache.kafka.streams.state.internals.RecordConverters.identity;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToTimestampedValue;
import static org.apache.kafka.streams.state.internals.WrappedStateStore.isTimestamped;

/**
 * Shared functions to handle state store registration and cleanup between
 * active and standby tasks.
 */
final class StateManagerUtil {
    static final String CHECKPOINT_FILE_NAME = ".checkpoint";
    static final long OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT = 10_000L;

    private StateManagerUtil() {}

    static RecordConverter converterForStore(final StateStore store) {
        return isTimestamped(store) ? rawValueToTimestampedValue() : identity();
    }

    static boolean checkpointNeeded(final boolean enforceCheckpoint,
                                    final Map<TopicPartition, Long> oldOffsetSnapshot,
                                    final Map<TopicPartition, Long> newOffsetSnapshot) {
        // we should always have the old snapshot post completing the register state stores;
        // if it is null it means the registration is not done and hence we should not overwrite the checkpoint
        if (oldOffsetSnapshot == null) {
            return false;
        }

        if (enforceCheckpoint)
            return true;

        // we can checkpoint if the the difference between the current and the previous snapshot is large enough
        long totalOffsetDelta = 0L;
        for (final Map.Entry<TopicPartition, Long> entry : newOffsetSnapshot.entrySet()) {
            totalOffsetDelta += entry.getValue() - oldOffsetSnapshot.getOrDefault(entry.getKey(), 0L);
        }

        // when enforcing checkpoint is required, we should overwrite the checkpoint if it is different from the old one;
        // otherwise, we only overwrite the checkpoint if it is largely different from the old one
        return totalOffsetDelta > OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT;
    }

    /**
     * @throws StreamsException If the store's changelog does not contain the partition
     */
    static void registerStateStores(final Logger log,
                                    final String logPrefix,
                                    final ProcessorTopology topology,
                                    final ProcessorStateManager stateMgr,
                                    final StateDirectory stateDirectory,
                                    final InternalProcessorContext processorContext) {
        if (topology.stateStores().isEmpty()) {
            return;
        }

        final TaskId id = stateMgr.taskId();
        try {
            if (!stateDirectory.lock(id)) {
                throw new LockException(String.format("%sFailed to lock the state directory for task %s", logPrefix, id));
            }
        } catch (final IOException e) {
            throw new StreamsException(
                String.format("%sFatal error while trying to lock the state directory for task %s", logPrefix, id),
                e
            );
        }
        log.debug("Acquired state directory lock");

        final boolean storeDirsEmpty = stateDirectory.directoryForTaskIsEmpty(id);

        stateMgr.registerStateStores(topology.stateStores(), processorContext);
        log.debug("Registered state stores");

        // We should only load checkpoint AFTER the corresponding state directory lock has been acquired and
        // the state stores have been registered; we should not try to load at the state manager construction time.
        // See https://issues.apache.org/jira/browse/KAFKA-8574
        stateMgr.initializeStoreOffsetsFromCheckpoint(storeDirsEmpty);
        log.debug("Initialized state stores");
    }

    /**
     * @throws ProcessorStateException if there is an error while closing the state manager
     */
    static void closeStateManager(final Logger log,
                                  final String logPrefix,
                                  final boolean closeClean,
                                  final boolean eosEnabled,
                                  final ProcessorStateManager stateMgr,
                                  final StateDirectory stateDirectory,
                                  final TaskType taskType) {
        // if EOS is enabled, wipe out the whole state store for unclean close since it is now invalid
        final boolean wipeStateStore = !closeClean && eosEnabled;

        final TaskId id = stateMgr.taskId();
        log.trace("Closing state manager for {} task {}", taskType, id);

        final AtomicReference<ProcessorStateException> firstException = new AtomicReference<>(null);
        try {
            if (stateDirectory.lock(id)) {
                try {
                    stateMgr.close();
                } catch (final ProcessorStateException e) {
                    firstException.compareAndSet(null, e);
                } finally {
                    try {
                        if (wipeStateStore) {
                            log.debug("Wiping state stores for {} task {}", taskType, id);
                            // we can just delete the whole dir of the task, including the state store images and the checkpoint files,
                            // and then we write an empty checkpoint file indicating that the previous close is graceful and we just
                            // need to re-bootstrap the restoration from the beginning
                            Utils.delete(stateMgr.baseDir());
                        }
                    } finally {
                        stateDirectory.unlock(id);
                    }
                }
            }
        } catch (final IOException e) {
            final ProcessorStateException exception = new ProcessorStateException(
                String.format("%sFatal error while trying to close the state manager for task %s", logPrefix, id), e
            );
            firstException.compareAndSet(null, exception);
        }

        final ProcessorStateException exception = firstException.get();
        if (exception != null) {
            throw exception;
        }
    }
}
