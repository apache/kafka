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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;

/**
 * Indicates that Kafka Streams is in state {@link State CREATED} and thus state stores cannot be queries yet.
 * To query state stores, it's required to first start Kafka Streams via {@link KafkaStreams#start()}.
 * You can retry to query the state after the state transitioned to {@link State RUNNING}.
 */
public class StreamsNotStartedException extends InvalidStateStoreException {

    private static final long serialVersionUID = 1L;

    public StreamsNotStartedException(final String message) {
        super(message);
    }

    public StreamsNotStartedException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
