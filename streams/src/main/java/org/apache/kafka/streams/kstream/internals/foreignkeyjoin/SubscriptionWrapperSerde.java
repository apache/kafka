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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SubscriptionWrapperSerde implements Serde {
    private final SubscriptionWrapperSerializer serializer;
    private final SubscriptionWrapperDeserializer deserializer;

    public SubscriptionWrapperSerde(final SubscriptionWrapperSerializer serializer,
                                    final SubscriptionWrapperDeserializer deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public SubscriptionWrapperSerde() {
        this.serializer = new SubscriptionWrapperSerializer();
        this.deserializer = new SubscriptionWrapperDeserializer();
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return serializer;
    }

    @Override
    public Deserializer deserializer() {
        return deserializer;
    }

    public class SubscriptionWrapperSerializer implements Serializer<SubscriptionWrapper> {
        public SubscriptionWrapperSerializer() {
        }

        @Override
        public void configure(Map configs, boolean isKey) {
            //Do nothing
        }

        @Override
        public byte[] serialize(String topic, SubscriptionWrapper data) {
            //{16-bytes Hash}{1-byte boolean propagate}
            final ByteBuffer buf = ByteBuffer.allocate(17);
            long[] elem = data.getHash();
            buf.putLong(elem[0]);
            buf.putLong(elem[1]);
            buf.put((byte) (data.isPropagate() ? 1 : 0 ));
            return buf.array();
        }

        @Override
        public void close() {
            //Do nothing
        }
    }

    public class SubscriptionWrapperDeserializer implements Deserializer<SubscriptionWrapper> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            //Do nothing
        }

        @Override
        public SubscriptionWrapper deserialize(String topic, byte[] data) {
            //{16-bytes Hash}{1-byte boolean propagate}
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final long[] hash = new long[2];
            hash[0] = buf.getLong();
            hash[1] = buf.getLong();
            boolean propagate = true;
            if (buf.get(16) == 0x00) {
                propagate = false;
            }
            return new SubscriptionWrapper(hash, propagate);
        }

        @Override
        public void close() {
            //Do nothing
        }
    }
}
