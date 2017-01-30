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

package org.apache.kafka.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.KTable;

/**
 * An interface that allows the Kafka Streams framework to extract a timestamp from an instance of {@link ConsumerRecord}.
 * The extracted timestamp is defined as milliseconds.
 */
public interface TimestampExtractor {

    /**
     * Extracts a timestamp from a record. The timestamp must be positive to be considered a valid timestamp.
     * Returning a negative timestamp will cause the record not to be processed but rather silently skipped.
     * <p>
     * The extracted timestamp MUST represent the milliseconds since midnight, January 1, 1970 UTC.
     * <p>
     * It is important to note that this timestamp may become the message timestamp for any messages sent to changelogs updated by {@link KTable}s
     * and joins. The message timestamp is used for log retention and log rolling, so using nonsensical values may result in
     * excessive log rolling and therefore broker performance degradation.
     *
     *
     * @param record a data record
     * @param previousTimestamp the latest extracted valid timestamp of the current record's partition˙ (could be -1 if unknown)
     * @return the timestamp of the record
     */
    long extract(ConsumerRecord<Object, Object> record, long previousTimestamp);
}
