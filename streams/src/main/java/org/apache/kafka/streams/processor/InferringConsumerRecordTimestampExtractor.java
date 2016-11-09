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
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves embedded metadata timestamps from Kafka messages.
 * Embedded metadata timestamp was introduced in "KIP-32: Add timestamps to Kafka message" for the new
 * 0.10+ Kafka message format.
 * <p>
 * Here, "embedded metadata" refers to the fact that compatible Kafka producer clients automatically and
 * transparently embed such timestamps into message metadata they send to Kafka, which can then be retrieved
 * via this timestamp extractor.
 * <p>
 * If the embedded metadata timestamp represents <i>CreateTime</i> (cf. Kafka broker setting
 * {@code message.timestamp.type} and Kafka topic setting {@code log.message.timestamp.type}),
 * this extractor effectively provides <i>event-time</i> semantics.
 * If <i>LogAppendTime</i> is used as broker/topic setting to define the embedded metadata timestamps,
 * using this extractor effectively provides <i>ingestion-time</i> semantics.
 * <p>
 * If you need <i>processing-time</i> semantics, use {@link WallclockTimestampExtractor}.
 * <p>
 * If a record has a negative (invalid) timestamp value a new timestamp will be inferred as the current
 * Streams time.
 *
 * @see ConsumerRecordTimestampExtractor
 * @see RobustConsumerRecordTimestampExtractor
 * @see WallclockTimestampExtractor
 */
public class InferringConsumerRecordTimestampExtractor implements TimestampExtractor {

    /**
     * Extracts the embedded metadata timestamp from the given {@link ConsumerRecord}.
     * If no valid timestamp can be extracted, a new timestamp will be inferred as the current Streams time.
     *
     * @param record a data record
     * @param currentStreamsTime the current value of the internally tracked Streams time (could be -1 if unknown)
     * @return the embedded metadata timestamp of the given {@link ConsumerRecord} or current Streams time
     */
    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long currentStreamsTime) {
        final long timestamp = record.timestamp();

        if (timestamp < 0) {
            if (currentStreamsTime == -1) {
                throw new StreamsException("Could not infer new timestamp for input record " + record
                        + " because current internal Streams time in unknown");
            }
            return currentStreamsTime;
        }

        return timestamp;
    }

}
