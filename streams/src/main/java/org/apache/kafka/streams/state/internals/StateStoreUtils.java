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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamThreadNotRunningException;
import org.apache.kafka.streams.errors.StreamThreadRebalancingException;
import org.apache.kafka.streams.errors.StateStoreMigratedException;
import org.apache.kafka.streams.errors.StateStoreNotAvailableException;
import org.apache.kafka.streams.errors.internals.EmptyStateStoreException;
import org.apache.kafka.streams.errors.internals.StateStoreClosedException;

import java.util.Objects;

final public class StateStoreUtils {

    private StateStoreUtils() {
    }

    static public InvalidStateStoreException wrapExceptionFromStateStoreProvider(final KafkaStreams streams,
                                                                                 final InvalidStateStoreException e) {
        Objects.requireNonNull(streams);
        if (e instanceof StreamThreadNotRunningException) {
            if (streams.state() == KafkaStreams.State.RUNNING || streams.state() == KafkaStreams.State.REBALANCING) {
                return new StreamThreadRebalancingException(e.getMessage(), e);
            } else {
                return e;
            }
        } else if (e instanceof StateStoreClosedException || e instanceof EmptyStateStoreException) {
            if (streams.state() == KafkaStreams.State.RUNNING ||
                    streams.state() == KafkaStreams.State.REBALANCING) {
                return new StateStoreMigratedException(e.getMessage(), e);
            } else {    // PENDING_SHUTDOWN || NOT_RUNNING || ERROR
                return new StateStoreNotAvailableException(e.getMessage(), e);
            }
        } else {
            return e;
        }
    }

    static InvalidStateStoreException wrapStateStoreClosedException(final KafkaStreams streams,
                                                                    final StateStoreClosedException e,
                                                                    final String storeName) {
        Objects.requireNonNull(streams);
        if (streams.state() == KafkaStreams.State.RUNNING || streams.state() == KafkaStreams.State.REBALANCING) {
            return new StateStoreMigratedException("State store [" + storeName + "] is not available anymore" +
                                                   " and may have been migrated to another instance; " +
                                                   "please re-discover its location from the state metadata." +
                                                   "Original error message: " + e.toString());
        } else {    // PENDING_SHUTDOWN | NOT_RUNNING | ERROR
            return new StateStoreNotAvailableException(e.getMessage(), e);
        }
    }

}
