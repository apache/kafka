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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Objects;

class KStreamMap<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final KeyValueMapper<? super KIn, ? super VIn, ? extends KeyValue<? extends KOut, ? extends VOut>> mapper;

    public KStreamMap(final KeyValueMapper<? super KIn, ? super VIn, ? extends KeyValue<? extends KOut, ? extends VOut>> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return new KStreamMapProcessor();
    }

    private class KStreamMapProcessor extends ContextualProcessor<KIn, VIn, KOut, VOut> {

        @Override
        public void process(final Record<KIn, VIn> record) {
            final KeyValue<? extends KOut, ? extends VOut> newPair =
                mapper.apply(record.key(), record.value());
            Objects.requireNonNull(newPair, "The provided KeyValueMapper returned null which is not allowed.");
            context().forward(record.withKey(newPair.key).withValue(newPair.value));
        }
    }
}
