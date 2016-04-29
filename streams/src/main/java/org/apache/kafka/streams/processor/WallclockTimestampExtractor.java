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

/**
 * Retrieves current wall clock timestamps as {@link System#currentTimeMillis()}.
 *
 * Using this extractor effectively provides <i>processing-time</i> semantics.
 *
 * If you need <i>event-time</i> semantics, use {@link ConsumerRecordTimestampExtractor} with
 * built-in <i>CreateTime</i> or <i>LogAppendTime</i> timestamp (see KIP-32: Add timestamps to Kafka message for details).
 */
public class WallclockTimestampExtractor implements TimestampExtractor {

    /**
     * Return the current wall clock time as timestamp.
     *
     * @param record  a data record
     * @return        the current wall clock time, expressed in milliseconds since midnight, January 1, 1970 UTC
     */
    @Override
    public long extract(ConsumerRecord<Object, Object> record) {
        return System.currentTimeMillis();
    }
}
