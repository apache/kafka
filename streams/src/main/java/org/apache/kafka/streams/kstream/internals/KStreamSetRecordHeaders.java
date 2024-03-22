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

import org.apache.kafka.streams.header.Headers;
import org.apache.kafka.streams.kstream.RecordHeadersMapper;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

public class KStreamSetRecordHeaders<K, V> implements ProcessorSupplier<K, V, K, V> {

    final RecordHeadersMapper<K, V> mapper;

    public KStreamSetRecordHeaders(final RecordHeadersMapper<K, V> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new KStreamSetRecordHeadersProcessor();
    }

    private class KStreamSetRecordHeadersProcessor extends ContextualProcessor<K, V, K, V> {

        @Override
        public void process(final Record<K, V> record) {
            final Headers headers = mapper.get(record.key(), record.value());
            context().forward(record.withHeaders(headers.unwrap()));
        }
    }
}
