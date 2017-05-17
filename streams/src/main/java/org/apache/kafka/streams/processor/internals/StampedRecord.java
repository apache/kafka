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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class StampedRecord extends Stamped<ConsumerRecord<Object, Object>> {

    public StampedRecord(ConsumerRecord<Object, Object> record, long timestamp) {
        super(record, timestamp);
    }

    public String topic() {
        return value.topic();
    }

    public int partition() {
        return value.partition();
    }

    public Object key() {
        return value.key();
    }

    public Object value() {
        return value.value();
    }

    public long offset() {
        return value.offset();
    }

    @Override
    public String toString() {
        return value.toString() + ", timestamp = " + timestamp;
    }
}
