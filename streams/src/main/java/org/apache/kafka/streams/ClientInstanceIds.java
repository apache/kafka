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
package org.apache.kafka.streams;

import org.apache.kafka.common.Uuid;

import java.util.Map;

/**
 * Encapsulates the {@code client instance id} used for metrics collection by
 * producers, consumers, and the admin client used by Kafka Streams.
 */
public interface ClientInstanceIds {
    /**
     * Returns the {@code client instance id} of the admin client.
     *
     * @return the {@code client instance id} of the admin client
     *
     * @throws IllegalStateException If telemetry is disabled on the admin client.
     */
    @SuppressWarnings("unused")
    Uuid adminInstanceId();

    /**
     * Returns the {@code client instance id} of the consumers.
     *
     * @return a map from thread key to {@code client instance id}
     */
    @SuppressWarnings("unused")
    Map<String, Uuid> consumerInstanceIds();

    /**
     * Returns the {@code client instance id} of the producers.
     *
     * @return a map from thread key to {@code client instance id}
     */
    @SuppressWarnings("unused")
    Map<String, Uuid> producerInstanceIds();
}