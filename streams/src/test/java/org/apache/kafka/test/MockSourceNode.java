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
package org.apache.kafka.test;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.SourceNode;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MockSourceNode<KIn, VIn> extends SourceNode<KIn, VIn> {

    private static final String NAME = "MOCK-SOURCE-";
    private static final AtomicInteger INDEX = new AtomicInteger(1);

    public int numReceived = 0;
    public final ArrayList<KIn> keys = new ArrayList<>();
    public final ArrayList<VIn> values = new ArrayList<>();
    public boolean initialized;
    public boolean closed;

    public MockSourceNode(final Deserializer<KIn> keyDeserializer, final Deserializer<VIn> valDeserializer) {
        super(NAME + INDEX.getAndIncrement(), keyDeserializer, valDeserializer);
    }

    @Override
    public void process(final Record<KIn, VIn> record) {
        numReceived++;
        keys.add(record.key());
        values.add(record.value());
    }

    @Override
    public void init(final InternalProcessorContext<KIn, VIn> context) {
        super.init(context);
        initialized = true;
    }

    @Override
    public void close() {
        super.close();
        closed = true;
    }
}
