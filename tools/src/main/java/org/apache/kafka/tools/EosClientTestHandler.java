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
package org.apache.kafka.tools;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;

import net.sourceforge.argparse4j.inf.Namespace;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.tools.KafkaClientPerformance.generateProps;

class EosClientTestHandler<K, V> implements ClientTestHandler<K, V> {

    private final Consumer<K, V> consumer;
    private List<ProducerRecord<K, V>> recordsToBeSent;
    private final long pollTimeout;

    private final String outputTopic;

    EosClientTestHandler(Namespace res) throws IOException {
        List<String> consumerProps = res.getList("consumerConfig");
        String consumerConfigFile = res.getString("consumerConfigFile");
        pollTimeout = res.getLong("pollTimeout");

        String inputTopic = res.getString("inputTopic");
        outputTopic = res.getString("topic");

        String applicationId = "Customized-EOS-Performance-" + UUID.randomUUID();
        Properties consumerProperties = generateProps(consumerProps, consumerConfigFile);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, applicationId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(inputTopic));

        recordsToBeSent = new LinkedList<>();
    }

    @Override
    public ProducerRecord<K, V> getRecord() {
        if (recordsToBeSent.isEmpty()) {
            ConsumerRecords<K, V> consumedRecords = consumer.poll(Duration.ofMillis(pollTimeout));
            for (ConsumerRecord<K, V> consumerRecord : consumedRecords) {
                recordsToBeSent.add(new ProducerRecord<>(outputTopic, consumerRecord.value()));
            }
        }
        return recordsToBeSent.remove(0);
    }

    @Override
    public void commit() {
        consumer.commitSync();
    }
}
