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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ChangedDeserializer<T> implements Deserializer<Change<T>> {

    private static final int NEWFLAG_SIZE = 1;

    private Deserializer<T> inner;

    public ChangedDeserializer(Deserializer<T> inner) {
        this.inner = inner;
    }

    public Deserializer<T> inner() {
        return inner;
    }

    public void setInner(Deserializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public Change<T> deserialize(String topic, byte[] data) {

        byte[] bytes = new byte[data.length - NEWFLAG_SIZE];

        System.arraycopy(data, 0, bytes, 0, bytes.length);

        if (ByteBuffer.wrap(data).get(data.length - NEWFLAG_SIZE) != 0) {
            return new Change<>(inner.deserialize(topic, bytes), null);
        } else {
            return new Change<>(null, inner.deserialize(topic, bytes));
        }
    }


    @Override
    public void close() {
        inner.close();
    }
}
