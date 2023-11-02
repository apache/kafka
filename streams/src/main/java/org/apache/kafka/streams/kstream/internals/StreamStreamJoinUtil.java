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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.slf4j.Logger;

public final class StreamStreamJoinUtil {

    private StreamStreamJoinUtil(){
    }

    public static <KIn, VIn, KOut, VOut> boolean skipRecord(
        final Record<KIn, VIn> record, final Logger logger,
        final Sensor droppedRecordsSensor,
        final ProcessorContext<KOut, VOut> context) {
        // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
        //
        // we also ignore the record if value is null, because in a key-value data model a null-value indicates
        // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
        // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
        // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
        if (record.key() == null || record.value() == null) {
            if (context.recordMetadata().isPresent()) {
                final RecordMetadata recordMetadata = context.recordMetadata().get();
                logger.warn(
                    "Skipping record due to null key or value. "
                        + "topic=[{}] partition=[{}] offset=[{}]",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                );
            } else {
                logger.warn(
                    "Skipping record due to null key or value. Topic, partition, and offset not known."
                );
            }
            droppedRecordsSensor.record();
            return true;
        } else {
            return false;
        }
    }
}
