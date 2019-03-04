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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;

public final class RecordConverters {
    private static final RecordConverter IDENTITY_INSTANCE = record -> record;

    @SuppressWarnings("deprecation")
    private static final RecordConverter RAW_TO_TIMESTAMED_INSTANCE = record -> {
        final byte[] rawValue = record.value();
        final long timestamp = record.timestamp();
        return new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            timestamp,
            record.timestampType(),
            record.checksum(),
            record.serializedKeySize(),
            record.serializedValueSize(),
            record.key(),
            ByteBuffer
                .allocate(8 + rawValue.length)
                .putLong(timestamp)
                .put(rawValue)
                .array(),
            record.headers(),
            record.leaderEpoch()
        );
    };

    // privatize the constructor so the class cannot be instantiated (only used for its static members)
    private RecordConverters() {}

    public static RecordConverter rawValueToTimestampedValue() {
        return RAW_TO_TIMESTAMED_INSTANCE;
    }

    public static RecordConverter identity() {
        return IDENTITY_INSTANCE;
    }
}
