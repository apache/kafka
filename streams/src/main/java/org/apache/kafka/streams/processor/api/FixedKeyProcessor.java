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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;

import java.time.Duration;

/**
 * A processor of key-value pair records where keys are immutable.
 *
 * @param <KIn> the type of input keys
 * @param <VIn> the type of input values
 * @param <VOut> the type of output values
 */
public interface FixedKeyProcessor<KIn, VIn, VOut> {

    /**
     * Initialize this processor with the given context. The framework ensures this is called once per processor when the topology
     * that contains it is initialized. When the framework is done with the processor, {@link #close()} will be called on it; the
     * framework may later re-use the processor by calling {@code #init()} again.
     * <p>
     * The provided {@link FixedKeyProcessorContext context} can be used to access topology and record metadata, to
     * {@link FixedKeyProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule} a method to be
     * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link StateStore}s.
     *
     * @param context the context; may not be null
     */
    default void init(final FixedKeyProcessorContext<KIn, VOut> context) {}

    /**
     * Process the record. Note that record metadata is undefined in cases such as a forward call from a punctuator.
     *
     * @param record the record to process
     */
    void process(FixedKeyRecord<KIn, VIn> record);

    /**
     * Close this processor and clean up any resources. Be aware that {@code #close()} is called after an internal cleanup.
     * Thus, it is not possible to write anything to Kafka as underlying clients are already closed. The framework may
     * later re-use this processor by calling {@code #init()} on it again.
     * <p>
     * Note: Do not close any streams managed resources, like {@link StateStore}s here, as they are managed by the library.
     */
    default void close() {}
}
