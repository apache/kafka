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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.internals.WindowKeySchema;

/**
 * The inner serde class can be specified by setting the property
 * {@link StreamsConfig#DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS} or
 * {@link StreamsConfig#DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS}
 * if the no-arg constructor is called and hence it is not passed during initialization.
 */
public class TimeWindowedChangelogSerializer<T> extends TimeWindowedSerializer<T> {

    private Serializer<T> inner;

    // Default constructor needed by Kafka
    public TimeWindowedChangelogSerializer() {
    }

    public TimeWindowedChangelogSerializer(final Serializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public byte[] serialize(final String topic, final Windowed<T> data) {
        if (data == null)
            return null;

        Bytes serializedKey = Bytes.wrap(serializeBaseKey(topic, data));
        // We hard-code seqnum to 0 for now as a placeholder, but need to figure out best way to pass in option
        // to decide whether or not to include duplicates for serializing the changelog entry.
        return WindowKeySchema.toStoreKeyBinary(serializedKey, data.window().start(), 0).get();
    }

    @Override
    public byte[] serializeBaseKey(final String topic, final Windowed<T> data) {
        return inner.serialize(topic, data.key());
    }
}

