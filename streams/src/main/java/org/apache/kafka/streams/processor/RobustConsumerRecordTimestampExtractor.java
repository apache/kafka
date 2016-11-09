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
 * Retrieves built-in timestamps from Kafka messages (introduced in KIP-32: Add timestamps to Kafka message).
 * <p>
 * Here, "built-in" refers to the fact that compatible Kafka producer clients automatically and
 * transparently embed such timestamps into messages they sent to Kafka, which can then be retrieved
 * via this timestamp extractor.
 * <p>
 * If <i>CreateTime</i> is used to define the built-in timestamps, using this extractor effectively provide
 * <i>event-time</i> semantics. If <i>LogAppendTime</i> is used to define the built-in timestamps, using
 * this extractor effectively provides <i>ingestion-time</i> semantics.
 * <p>
 * If you need <i>processing-time</i> semantics, use {@link WallclockTimestampExtractor}.
 * <p>
 * If a record has a negative timestamp value, a log message is written and the timestamp is returned.
 * This results in dropping the record, i.e., the record will not be processed.
 *
 * @see ConsumerRecordTimestampExtractor
 * @see WallclockTimestampExtractor
 */
public class RobustConsumerRecordTimestampExtractor implements TimestampExtractor {
    private static final Logger log = LoggerFactory.getLogger(RobustConsumerRecordTimestampExtractor.class);

    /**
     * Extracts the embedded meta data timestamp from the given {@link ConsumerRecord}
     * and writes a log warn message if the extracted timestamp is negative,
     * because the record will no be processed but dropped.
     *
     * @param record a data record
     * @return the embedded meta-data timestamp of the given {@link ConsumerRecord}
     */
    @Override
    public long extract(ConsumerRecord<Object, Object> record) {
        final long timestamp = record.timestamp();

        if (timestamp < 0) {
            log.warn("Dropping input record {} because it has an invalid (negative) timestamp.", record);
        }

        return timestamp;
    }
}
