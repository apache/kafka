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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;

import java.time.Duration;

/**
 * The {@code ValueTransformerWithKey} interface for stateful mapping of a value to a new value
 * (with possible new type). This is a stateful record-by-record operation, i.e, {@link
 * #transform(Record)} is invoked individually for each record of a stream and can access and modify
 * a state that is available beyond a single call of {@link #transform(Record)} (cf. {@link
 * ValueMapper} for stateless value transformation). Additionally, this {@code
 * ValueTransformerWithKey} can {@link ProcessorContext#schedule(Duration, PunctuationType,
 * Punctuator) schedule} a method to be {@link Punctuator#punctuate(long) called periodically} with
 * the provided context. Note that the key is read-only and should not be modified, as this can lead
 * to corrupt partitioning. If {@code ValueTransformerWithKey} is applied to a {@link KeyValue} pair
 * record the record's key is preserved.
 * <p>
 * Use {@link ValueTransformerWithKeySupplier} to provide new instances of {@link
 * ValueTransformer} to Kafka Stream's runtime.
 * <p>
 * If a record's key and value should be modified {@link Transformer} can be used.
 *
 * @param <K>    key type
 * @param <V>    value type
 * @param <VOut> transformed value type
 * @see org.apache.kafka.streams.kstream.ValueTransformer
 * @see ValueTransformerWithKeySupplier
 * @see KStream#transformValues(ValueTransformerSupplier, String...)
 * @see KStream#transformValues(ValueTransformerWithKeySupplier, String...)
 * @see Transformer
 */

public interface ValueTransformer<K, V, VOut> {

    /**
     * Initialize this transformer. This is called once per instance when the topology gets
     * initialized.
     * <p>
     * The provided {@link ProcessorContext context} can be used to access topology and record meta
     * data, to {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule} a
     * method to be {@link Punctuator#punctuate(long) called periodically} and to access attached
     * {@link StateStore}s.
     * <p>
     * Note that {@link ProcessorContext} is updated in the background with the current record's
     * meta data. Thus, it only contains valid record meta data when accessed within {@link
     * #transform(Record)}.
     * <p>
     * Note that using {@link ProcessorContext#forward(Record)} is not allowed within any method of
     * {@code ValueTransformerWithKey} and will result in an {@link StreamsException exception}.
     *
     * @param context the context
     * @throws IllegalStateException If store gets registered after initialization is already
     *                               finished
     * @throws StreamsException      if the store's change log does not contain the partition
     */
    void init(final ProcessorContext<?, ?> context);

    /**
     * Transform the given [key and ]value to a new value. Additionally, any {@link StateStore} that
     * is {@link KStream#transformValues(ValueTransformerWithKeySupplier, String...) attached} to
     * this operator can be accessed and modified arbitrarily (cf. {@link
     * ProcessorContext#getStateStore(String)}).
     * <p>
     * Note, that using {@link ProcessorContext#forward(Record)} is not allowed within {@code
     * transform} and will result in an {@link StreamsException exception}.
     *
     * @param record the value to be transformed
     * @return the new value
     */
    <K1 extends K, V1 extends V> VOut transform(Record<K1, V1> record);

    /**
     * Close this processor and clean up any resources.
     * <p>
     * It is not possible to return any new output records within {@code close()}. Using {@link
     * ProcessorContext#forward(Record)} will result in an {@link StreamsException exception}.
     */
    void close();

}
