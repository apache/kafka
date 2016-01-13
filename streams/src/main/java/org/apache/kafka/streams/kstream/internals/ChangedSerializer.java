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

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class ChangedSerializer<T> implements Serializer<Change<T>> {

    private static final int NEWFLAG_SIZE = 1;

    private final Serializer<T> inner;

    public ChangedSerializer(Serializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, Change<T> data) {
        // only one of the old / new values would be not null
        byte[] serializedKey = inner.serialize(topic, data.newValue != null ? data.newValue : data.oldValue);

        ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + NEWFLAG_SIZE);
        buf.put(serializedKey);
        buf.put((byte) (data.newValue != null ? 1 : 0));

        return buf.array();
    }


    @Override
    public void close() {
        inner.close();
    }
}
