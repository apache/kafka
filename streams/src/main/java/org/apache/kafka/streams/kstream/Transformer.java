/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * The {@code Transformer} interface for stateful mapping of an input record to zero, one, or multiple new output
 * records (both key and value type can be altered arbitrarily).
 * This is a stateful record-by-record operation, i.e, {@link #transform(Object, Object)} is invoked individually for
 * each record of a stream and can access and modify a state that is available beyond a single call of
 * {@link #transform(Object, Object)} (cf. {@link KeyValueMapper} for stateless record transformation).
 * Additionally, the interface can be called in regular intervals based on the processing progress
 * (cf. {@link #punctuate(long)}.
 * <p>
 * Use {@link TransformerSupplier} to provide new instances of {@code Transformer} to Kafka Stream's runtime.
 * <p>
 * If only a record's value should be modified {@link ValueTransformer} can be used.
 *
 * @param <K> key type
 * @param <V> value type
 * @param <R> {@link KeyValue} return type (both key and value type can be set
 *            arbitrarily)
 * @see TransformerSupplier
 * @see KStream#transform(TransformerSupplier, String...)
 * @see ValueTransformer
 * @see KStream#map(KeyValueMapper)
 * @see KStream#flatMap(KeyValueMapper)
 */
@InterfaceStability.Unstable
public interface Transformer<K, V, R> {

    /**
     * Initialize this transformer.
     * This is called once per instance when the topology gets initialized.
     * <p>
     * The provided {@link ProcessorContext context} can be used to access topology and record meta data, to
     * {@link ProcessorContext#schedule(long) schedule itself} for periodical calls (cf. {@link #punctuate(long)}), and
     * to access attached {@link StateStore}s.
     * <p>
     * Note, that {@link ProcessorContext} is updated in the background with the current record's meta data.
     * Thus, it only contains valid record meta data when accessed within {@link #transform(Object, Object)}.
     *
     * @param context the context
     */
    void init(final ProcessorContext context);

    /**
     * Transform the record with the given key and value.
     * Additionally, any {@link StateStore state} that is {@link KStream#transform(TransformerSupplier, String...)
     * attached} to this operator can be accessed and modified
     * arbitrarily (cf. {@link ProcessorContext#getStateStore(String)}).
     * <p>
     * If more than one output record should be forwarded downstream {@link ProcessorContext#forward(Object, Object)},
     * {@link ProcessorContext#forward(Object, Object, int)}, and
     * {@link ProcessorContext#forward(Object, Object, String)} can be used.
     * If not record should be forwarded downstream, {@code transform} can return {@code null}.
     *
     * @param key the key for the record
     * @param value the value for the record
     * @return new {@link KeyValue} pair&mdash;if {@code null} no key-value pair will
     * be forwarded to down stream
     */
    R transform(final K key, final V value);

    /**
     * Perform any periodic operations and possibly generate new {@link KeyValue} pairs if this processor
     * {@link ProcessorContext#schedule(long) schedules itself} with the context during
     * {@link #init(ProcessorContext) initialization}.
     * <p>
     * To generate new {@link KeyValue} pairs {@link ProcessorContext#forward(Object, Object)},
     * {@link ProcessorContext#forward(Object, Object, int)}, and
     * {@link ProcessorContext#forward(Object, Object, String)} can be used.
     * <p>
     * Note that {@code punctuate} is called based on <it>stream time</it> (i.e., time progresses with regard to
     * timestamps return by the used {@link TimestampExtractor})
     * and not based on wall-clock time.
     *
     * @param timestamp the stream time when {@code punctuate} is being called
     * @return must return {@code null}&mdash;otherwise, a {@link StreamsException exception} will be thrown
     */
    R punctuate(final long timestamp);

    /**
     * Close this processor and clean up any resources.
     * <p>
     * To generate new {@link KeyValue} pairs {@link ProcessorContext#forward(Object, Object)},
     * {@link ProcessorContext#forward(Object, Object, int)}, and
     * {@link ProcessorContext#forward(Object, Object, String)} can be used.
     */
    void close();

}
