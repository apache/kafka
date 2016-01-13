/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

/**
 * A plugin interface to allow things to intercept Producer events such as sending producer record or getting an acknowledgement
 * when records get published
 */
public interface ProducerInterceptor<K, V> extends Configurable {
    class SerializedKeyValue {
        public byte[] serializedKey;
        public byte[] serializedValue;

        public SerializedKeyValue(byte[] serializedKey, byte[] serializedValue) {
            this.serializedKey = serializedKey;
            this.serializedValue = serializedValue;
        }
    }

    /**
     * This is called just after KafkaProducer assigns partition (if needed) and serializes key and value.
     * The implementation can modify serialized key and value for publishing to topic/partition. In that case,
     * the modified key and value must be returned from this method, otherwise 'serializedKeyValue' passed to this
     * method is returned.
     * @param tp topic/partition to send record to
     * @param record the record from client
     * @param serializedKeyValue serialized key and value
     * @param correlationId unique number to correlate producer events for the same record
     * @return serialized key and value to send to topic/partition
     */
    public SerializedKeyValue onProduceStart(TopicPartition tp, ProducerRecord<K, V> record, SerializedKeyValue serializedKeyValue, long correlationId);

    /**
     * This is called when the send has been acknowledged
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     * @param correlationId unique number that matches 'correlationId' passed in onProduceStart() for this record
     */
    public void onProduceComplete(RecordMetadata metadata, Exception exception, long correlationId);
}