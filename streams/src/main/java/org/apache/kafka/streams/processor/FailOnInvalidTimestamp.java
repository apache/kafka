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
package org.apache.kafka.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves embedded metadata timestamps from Kafka messages.
 * If a record has a negative (invalid) timestamp value, this extractor raises an exception.
 * <p>
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
 *
 * @see LogAndSkipOnInvalidTimestamp
 * @see UsePreviousTimeOnInvalidTimestamp
 * @see WallclockTimestampExtractor
 */
@InterfaceStability.Evolving
public class FailOnInvalidTimestamp extends ExtractRecordMetadataTimestamp {
    private static final Logger log = LoggerFactory.getLogger(FailOnInvalidTimestamp.class);

    /**
     * Raises an exception on every call.
     *
     * @param record a data record
     * @param recordTimestamp the timestamp extractor from the record
     * @param previousTimestamp the latest extracted valid timestamp of the current record's partition˙ (could be -1 if unknown)
     * @return nothing; always raises an exception
     * @throws StreamsException on every invocation
     */
    @Override
    public long onInvalidTimestamp(final ConsumerRecord<Object, Object> record,
                                   final long recordTimestamp,
                                   final long previousTimestamp)
            throws StreamsException {

        final String message = "Input record " + record + " has invalid (negative) timestamp. " +
            "Possibly because a pre-0.10 producer client was used to write this record to Kafka without embedding " +
            "a timestamp, or because the input topic was created before upgrading the Kafka cluster to 0.10+. " +
            "Use a different TimestampExtractor to process this data.";

        log.error(message);
        throw new StreamsException(message);
    }

}
