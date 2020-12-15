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
package org.apache.kafka.streams.processor.internals.testutil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

public final class ConsumerRecordUtil {
    private ConsumerRecordUtil() {}

    public static <K, V> ConsumerRecord<K, V> record(final String topic,
                                                     final int partition,
                                                     final long offset,
                                                     final K key,
                                                     final V value) {
        // the no-time constructor in ConsumerRecord initializes the
        // timestamp to -1, which is an invalid configuration. Here,
        // we initialize it to 0.
        return new ConsumerRecord<>(
            topic,
            partition,
            offset,
            0L,
            TimestampType.CREATE_TIME,
            0L,
            0,
            0,
            key,
            value
        );
    }
}
