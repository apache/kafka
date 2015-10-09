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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Properties;


public class CommittedTimeoutKafkaConsumer {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = makeConsumer();
        System.out.println("COMMITTED: " + consumer.committed(new TopicPartition("t", 0)));
    }

    public static KafkaConsumer<String, String> makeConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "okaraman-ld1.linkedin.biz:9092");
        props.put("group.id", System.currentTimeMillis() + "");
        props.put("partition.assignment.strategy", "roundrobin");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("session.timeout.ms", 30 * 1000);
        return new KafkaConsumer<>(props);
    }
}