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

import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * A stateful {@link ValueTransformer} interface to transform a value into a new value.
 *
 * @param <V>   value type
 * @param <R>   return type
 */
public interface ValueTransformer<V, R> {

    /**
     * Initialize this transformer with the given context. The framework ensures this is called once per processor when the topology
     * that contains it is initialized.
     * <p>
     * If this transformer is to be {@link #punctuate(long) called periodically} by the framework, then this method should
     * {@link ProcessorContext#schedule(long) schedule itself} with the provided context.
     *
     * @param context the context; may not be null
     */
    void init(ProcessorContext context);

    /**
     * Transform the record with the given key and value.
     *
     * @param value the value for the record
     * @return new value
     */
    R transform(V value);

    /**
     * Perform any periodic operations and possibly return a new value, if this processor {@link ProcessorContext#schedule(long) schedule itself} with the context
     * during {@link #init(ProcessorContext) initialization}.
     *
     * @param timestamp the stream time when this method is being called
     * @return new value; if null it will not be forwarded to down stream
     */
    R punctuate(long timestamp);

    /**
     * Close this processor and clean up any resources.
     */
    void close();

}
