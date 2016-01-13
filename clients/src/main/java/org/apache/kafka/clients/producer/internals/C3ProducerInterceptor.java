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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.Crc32;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TEMPORARY CLASS TO QUICKLY DO PR FOR POC PRODUCER INTERCEPTOR AND IF WE CAN SUPPORT COLLECTING C3 METRICS WITH IT.
 * NOT GOING TO CHECK THIS IN.
 * Interceptor for collecting and recording audit metrics
 */
public class C3ProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private class MessageMetrics {
        public int sizeInBytes;
        public long crc;

        public MessageMetrics(int sizeInBytes, long crc) {
            this.sizeInBytes = sizeInBytes;
            this.crc = crc;
        }
    }

    private final ConcurrentMap<Long, MessageMetrics> inflightRecords;

    public C3ProducerInterceptor() {
        this.inflightRecords = new ConcurrentHashMap<>();
    }

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public SerializedKeyValue onProduceStart(TopicPartition tp, ProducerRecord<K, V> record, SerializedKeyValue serializedKeyValue, long correlationId) {
        // get audit metrics

        // message byte count
        int recordBytes = Record.recordSize(serializedKeyValue.serializedKey, serializedKeyValue.serializedValue);

        // message CRC
        Crc32 crc = new Crc32();
        // update for the key
        if (serializedKeyValue.serializedKey == null) {
            crc.updateInt(-1);
        } else {
            crc.updateInt(serializedKeyValue.serializedKey.length);
            crc.update(serializedKeyValue.serializedKey, 0, serializedKeyValue.serializedKey.length);
        }
        // update for the value
        if (serializedKeyValue.serializedValue== null) {
            crc.updateInt(-1);
        } else {
            crc.updateInt(serializedKeyValue.serializedValue.length);
            crc.update(serializedKeyValue.serializedValue, 0, serializedKeyValue.serializedValue.length);
        }
        long recordCrc = crc.getValue();

        System.out.println("onProduceStart: " + tp + " ID " + correlationId + ", message size = " + recordBytes + " bytes, CRC = " + recordCrc);

        // add producer record metrics to the map so that we can record the metric when we get ack
        if (this.inflightRecords.containsKey(correlationId)) {
            // not supposed to happen, will deal with it later
        } else {
            this.inflightRecords.put(correlationId, new MessageMetrics(recordBytes, recordCrc));
        }

        return serializedKeyValue;
    }

    @Override
    public void onProduceComplete(RecordMetadata metadata, Exception exception, long correlationId) {
        // record a metrics for this record based on timestamp in 'metadata'
        MessageMetrics messageMetrics = this.inflightRecords.remove(correlationId);
        if (messageMetrics == null) {
            // we did not get corresponding onProduceStart for this record -- probably just log an error ?
        } else if (metadata == null) {
            // we got exception
            // in the future, we may want to record an error, but V1 will not record anything
        } else {
            // success, record the metric
            System.out.println("onProduceComplete: " + metadata.topic() + "-" + metadata.partition() + " ID " + correlationId + ", message size = " + messageMetrics.sizeInBytes + " bytes, CRC = " + messageMetrics.crc);
            // here we will call record() using timestmap in 'metadata'
        }
    }

}