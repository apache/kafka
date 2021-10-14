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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.io.File;
import java.time.Duration;
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
     * Return the metadata of the current record if available. Processors may be invoked to
     * process a source record from an input topic, to run a scheduled punctuation
     * (see {@link org.apache.kafka.streams.processor.api.ProcessorContext#schedule(Duration, PunctuationType, Punctuator)}),
     * or because a parent processor called {@link org.apache.kafka.streams.processor.api.ProcessorContext#forward(Record)}.
     * <p>
     * In the case of a punctuation, there is no source record, so this metadata would be
     * undefined. Note that when a punctuator invokes {@link ProcessorContext#forward(Record)},
     * downstream processors will receive the forwarded record as a regular
     * {@link Processor#process(Record)} invocation. In other words, it wouldn't be apparent to
     * downstream processors whether or not the record being processed came from an input topic
     * or punctuation and therefore whether or not this metadata is defined. This is why
     * the return type of this method is {@link Optional}.
     * <p>
     * If there is any possibility of punctuators upstream, any access
     * to this field should consider the case of
     * "<code>recordMetadata().isPresent() == false</code>".
     * Of course, it would be safest to always guard this condition.
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
