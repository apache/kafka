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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.io.File;
import java.util.Map;
import java.util.Optional;

/**
 * State store context interface.
 */
public interface StateStoreContext {

    /**
     * Returns the application id.
     *
     * @return the application id
     */
    String applicationId();

    /**
     * Returns the task id.
     *
     * @return the task id
     */
    TaskId taskId();

    /**
     * Return the metadata of the current topic/partition/offset if available.
     * This is defined as the metadata of the record that is currently being
     * processed (or was last processed) by the StreamTask that holds the store.
     * <p>
     * Note that the metadata is not defined during all store interactions, for
     * example, while the StreamTask is running a punctuation.
     *
     * @return metadata of the current record
     */
    Optional<RecordMetadata> recordMetadata();

    /**
     * Returns the default key serde.
     *
     * @return the key serializer
     */
    Serde<?> keySerde();

    /**
     * Returns the default value serde.
     *
     * @return the value serializer
     */
    Serde<?> valueSerde();

    /**
     * Returns the state directory for the partition.
     *
     * @return the state directory
     */
    File stateDir();

    /**
     * Returns Metrics instance.
     *
     * @return StreamsMetrics
     */
    StreamsMetrics metrics();

    /**
     * Registers and possibly restores the specified storage engine.
     *
     * @param store the storage engine
     * @param stateRestoreCallback the restoration callback logic for log-backed state stores upon restart
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    void register(final StateStore store,
                  final StateRestoreCallback stateRestoreCallback);

    /**
     * Registers and possibly restores the specified storage engine.
     *
     * @param store the storage engine
     * @param stateRestoreCallback the restoration callback logic for log-backed state stores upon restart
     * @param commitCallback a callback to be invoked upon successful task commit, in case the store
     *                           needs to perform any state tracking when the task is known to be in
     *                           a consistent state. If the store has no such state to track, it may
     *                           use {@link StateStoreContext#register(StateStore, StateRestoreCallback)} instead.
     *                           Persistent stores provided by Kafka Streams use this method to save
     *                           their Position information to local disk, for example.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    @Evolving
    void register(final StateStore store,
                  final StateRestoreCallback stateRestoreCallback,
                  final CommitCallback commitCallback);

    /**
     * Returns all the application config properties as key/value pairs.
     *
     * <p> The config properties are defined in the {@link org.apache.kafka.streams.StreamsConfig}
     * object and associated to the StateStoreContext.
     *
     * <p> The type of the values is dependent on the {@link org.apache.kafka.common.config.ConfigDef.Type type} of the property
     * (e.g. the value of {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG DEFAULT_KEY_SERDE_CLASS_CONFIG}
     * will be of type {@link Class}, even if it was specified as a String to
     * {@link org.apache.kafka.streams.StreamsConfig#StreamsConfig(Map) StreamsConfig(Map)}).
     *
     * @return all the key/values from the StreamsConfig properties
     */
    Map<String, Object> appConfigs();

    /**
     * Returns all the application config properties with the given key prefix, as key/value pairs
     * stripping the prefix.
     *
     * <p> The config properties are defined in the {@link org.apache.kafka.streams.StreamsConfig}
     * object and associated to the StateStoreContext.
     *
     * @param prefix the properties prefix
     * @return the key/values matching the given prefix from the StreamsConfig properties.
     */
    Map<String, Object> appConfigsWithPrefix(final String prefix);

}
