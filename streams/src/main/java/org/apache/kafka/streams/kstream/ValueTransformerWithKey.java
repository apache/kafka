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
package org.apache.kafka.streams.kstream;

import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.To;

/**
 * The {@code ValueTransformerWithKey} interface for stateful mapping of a value to a new value (with possible new type).
 * This is a stateful record-by-record operation, i.e, {@link #transform(Object, Object)} is invoked individually for each
 * record of a stream and can access and modify a state that is available beyond a single call of
 * {@link #transform(Object, Object)} (cf. {@link ValueMapper} for stateless value transformation).
 * Additionally, this {@code ValueTransformerWithKey} can
 * {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule} a method to be
 * {@link Punctuator#punctuate(long) called periodically} with the provided context.
 * Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
 * If {@code ValueTransformerWithKey} is applied to a {@link KeyValue} pair record the record's key is preserved.
 * <p>
 * Use {@link ValueTransformerWithKeySupplier} to provide new instances of {@link ValueTransformerWithKey} to
 * Kafka Stream's runtime.
 * <p>
 * If a record's key and value should be modified {@link Transformer} can be used.
 *
 * @param <K>  key type
 * @param <V>  value type
 * @param <VR> transformed value type
 * @see ValueTransformer
 * @see ValueTransformerWithKeySupplier
 * @see KStream#transformValues(ValueTransformerSupplier, String...)
 * @see KStream#transformValues(ValueTransformerWithKeySupplier, String...)
 * @see Transformer
 */

public interface ValueTransformerWithKey<K, V, VR> {

    /**
     * Initialize this transformer.
     * This is called once per instance when the topology gets initialized.
     * <p>
     * The provided {@link ProcessorContext context} can be used to access topology and record meta data, to
     * {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule} a method to be
     * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link StateStore}s.
     * <p>
     * Note that {@link ProcessorContext} is updated in the background with the current record's meta data.
     * Thus, it only contains valid record meta data when accessed within {@link #transform(Object, Object)}.
     * <p>
     * Note that using {@link ProcessorContext#forward(Object, Object)} or
     * {@link ProcessorContext#forward(Object, Object, To)} is not allowed within any method of
     * {@code ValueTransformerWithKey} and will result in an {@link StreamsException exception}.
     *
     * @param context the context
     * @throws IllegalStateException If store gets registered after initialization is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    void init(final ProcessorContext context);

    /**
     * Transform the given [key and] value to a new value.
     * Additionally, any {@link StateStore} that is {@link KStream#transformValues(ValueTransformerWithKeySupplier, String...)
     * attached} to this operator can be accessed and modified arbitrarily (cf.
     * {@link ProcessorContext#getStateStore(String)}).
     * <p>
     * Note that using {@link ProcessorContext#forward(Object, Object)} or
     * {@link ProcessorContext#forward(Object, Object, To)} is not allowed within {@code transform} and
     * will result in an {@link StreamsException exception}.
     * <p>
     * Note that if a {@code ValueTransformerWithKey} is used in a {@link KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (or any other overload of {@code KTable#transformValues(...)}) operation,
     * then the provided {@link ProcessorContext} from {@link #init(ProcessorContext)}
     * does not guarantee that all context information will be available when {@code transform()}
     * is executed, as it might be executed "out-of-band" due to some internal optimizations
     * applied by the Kafka Streams DSL.
     *
     * @param readOnlyKey the read-only key
     * @param value       the value to be transformed
     * @return the new value
     */
    VR transform(final K readOnlyKey, final V value);

    /**
     * Close this processor and clean up any resources.
     * <p>
     * It is not possible to return any new output records within {@code close()}.
     * Using {@link ProcessorContext#forward(Object, Object)} or {@link ProcessorContext#forward(Object, Object, To)},
     * will result in an {@link StreamsException exception}.
     */
    void close();
}
