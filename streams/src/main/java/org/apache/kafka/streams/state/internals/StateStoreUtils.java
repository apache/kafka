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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamThreadNotStartedException;
import org.apache.kafka.streams.errors.StreamThreadNotRunningException;
import org.apache.kafka.streams.errors.StreamThreadRebalancingException;
import org.apache.kafka.streams.errors.StateStoreMigratedException;
import org.apache.kafka.streams.errors.StateStoreNotAvailableException;
import org.apache.kafka.streams.errors.internals.StateStoreClosedException;
import org.apache.kafka.streams.errors.internals.StateStoreIsEmptyException;

public final class StateStoreUtils {

    private StateStoreUtils() {}

    public static void handleInvalidStateStoreException(final KafkaStreams streams, final InvalidStateStoreException e) {
        if (streams.state() == State.CREATED) {
            throw new StreamThreadNotStartedException(e.getMessage(), e);
        }
        if (e instanceof StreamThreadNotRunningException) {
            if (streams.state() == State.RUNNING || streams.state() == State.REBALANCING) {
                throw new StreamThreadRebalancingException(e.getMessage(), e);
            } else {    // PENDING_SHUTDOWN || NOT_RUNNING || ERROR
                throw e;
            }
        } else if (e instanceof StateStoreClosedException || e instanceof StateStoreIsEmptyException) {
            if (streams.state() == State.RUNNING || streams.state() == State.REBALANCING) {
                throw new StateStoreMigratedException(e.getMessage(), e);
            } else {    // PENDING_SHUTDOWN || NOT_RUNNING || ERROR
                throw new StateStoreNotAvailableException(e.getMessage(), e);
            }
        } else {
            throw e;
        }
    }

    public static void handleStateStoreClosedException(final KafkaStreams streams, final String storeName,
                                                       final StateStoreClosedException e) {
        if (streams.state() == State.RUNNING || streams.state() == State.REBALANCING || streams.state() == State.CREATED) {
            throw new StateStoreMigratedException("State store [" + storeName + "] is not available anymore " +
                                                  "and may have been migrated to another instance; " +
                                                  "please re-discover its location from the state metadata.");
        } else {    // PENDING_SHUTDOWN | NOT_RUNNING | ERROR
            throw new StateStoreNotAvailableException(e.getMessage(), e);
        }
    }
}
