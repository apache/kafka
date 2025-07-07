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
package org.apache.kafka.streams.errors;

/**
 * Indicates that the state store being queried is already closed. This could happen when Kafka Streams is in
 * {@link org.apache.kafka.streams.KafkaStreams.State#PENDING_SHUTDOWN PENDING_SHUTDOWN} or
 * {@link org.apache.kafka.streams.KafkaStreams.State#NOT_RUNNING NOT_RUNNING} or
 * {@link org.apache.kafka.streams.KafkaStreams.State#ERROR ERROR} state.
 */
@SuppressWarnings("unused")
public class StateStoreNotAvailableException extends InvalidStateStoreException {

    private static final long serialVersionUID = 1L;

    public StateStoreNotAvailableException(final String message) {
        super(message);
    }

    public StateStoreNotAvailableException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

}
