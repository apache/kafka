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
package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.errors.StreamsException;
import java.util.concurrent.ExecutionException;

public class AddNamedTopologyResult {

    private final KafkaFuture<Void> addTopologyFuture;

    public AddNamedTopologyResult(final KafkaFuture<Void> addTopologyFuture) {
        this.addTopologyFuture = addTopologyFuture;
    }

    /**
     * @return a {@link KafkaFuture} that completes successfully when all threads on this client have picked up the
     * new {@link NamedTopology}. Note that the new topology is not guaranteed to begin processing on this client or
     * any others until its addition has been completed by all instances of the application.
     */
    public KafkaFuture<Void> all() {
        return addTopologyFuture;
    }

    /**
     * Boiler plate to get the root cause as a StreamsException if completed exceptionally, otherwise returns {@code null}.
     * Non-blocking.
     */
    public StreamsException exceptionNow() {
        try {
            addTopologyFuture.getNow(null);
            return null;
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof StreamsException) {
                return (StreamsException) e.getCause();
            } else {
                return new StreamsException(e.getCause());
            }
        } catch (final InterruptedException e) {
            return null;
        }
    }
}