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

import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.io.IOException;

public final class TaskUtils {
    private TaskUtils() {}

    /**
     * Package-private for testing only
     *
     * @throws StreamsException If the store's change log does not contain the partition
     */
    static void registerStateStores(final ProcessorTopology topology,
                                    final StateDirectory stateDirectory,
                                    final TaskId id,
                                    final String logPrefix,
                                    final Logger log,
                                    final InternalProcessorContext processorContext,
                                    final ProcessorStateManager stateMgr) {
        if (topology.stateStores().isEmpty()) {
            return;
        }

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

        // We should only load checkpoint AFTER the corresponding state directory lock has been acquired and
        // the state stores have been registered; we should not try to load at the state manager construction time.
        // See https://issues.apache.org/jira/browse/KAFKA-8574
        for (final StateStore store : topology.stateStores()) {
            processorContext.uninitialize();
            store.init(processorContext, store);
            log.trace("Registered state store {}", store.name());
        }
        stateMgr.initializeStoreOffsetsFromCheckpoint();
        log.debug("Initialized state stores");
    }

    /**
     * @throws ProcessorStateException if there is an error while closing the state manager
     */
    static void closeStateManager(final TaskId id,
                                  final Logger log,
                                  final String logPrefix,
                                  final boolean closeClean,
                                  final ProcessorStateManager stateMgr,
                                  final StateDirectory stateDirectory) {
        ProcessorStateException exception = null;
        log.trace("Closing state manager");
        try {
            stateMgr.close();
        } catch (final ProcessorStateException e) {
            exception = e;
        } finally {
            try {
                stateDirectory.unlock(id);
            } catch (final IOException e) {
                if (exception == null) {
                    exception = new ProcessorStateException(String.format("%sFailed to release state dir lock", logPrefix), e);
                }
            }
        }

        if (exception != null) {
            if (closeClean)
                throw exception;
            else
                log.warn("Closing standby task " + id + " uncleanly throws an exception " + exception);
        }
    }
}
