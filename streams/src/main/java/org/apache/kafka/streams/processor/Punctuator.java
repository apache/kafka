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

import java.time.Duration;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A functional interface used as an argument to {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator)}.
 *
 * @see Cancellable
 */
public interface Punctuator {

    /**
     * Perform the scheduled periodic operation.
     *
     * <p> If this method accesses {@link ProcessorContext} or
     * {@link org.apache.kafka.streams.processor.api.ProcessorContext}, record metadata like topic,
     * partition, and offset or {@link org.apache.kafka.streams.processor.api.RecordMetadata} won't
     * be available.
     *
     * <p> Furthermore, for any record that is sent downstream via {@link ProcessorContext#forward(Object, Object)}
     * or {@link org.apache.kafka.streams.processor.api.ProcessorContext#forward(Record)}, there
     * won't be any record metadata. If {@link ProcessorContext#forward(Object, Object)} is used,
     * it's also not possible to set records headers.
     *
     * @param timestamp when the operation is being called, depending on {@link PunctuationType}
     */
    void punctuate(long timestamp);
}
