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
package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.WorkerUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;

public class RoundTripWorker extends RoundTripWorkerBase {

    KafkaConsumer<byte[], byte[]> consumer;

    RoundTripWorker(String id, RoundTripWorkloadSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void initializeConsumer(HashSet<TopicPartition> partitions) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spec.bootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer." + id);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 105000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 100000);
        // user may over-write the defaults with common client config and consumer config
        WorkerUtils.addConfigsToProperties(props, spec.commonClientConf(), spec.consumerConf());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "round-trip-consumer-group-" + id);
        consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
        consumer.assign(partitions);
    }

    @Override
    protected ConsumerRecords<byte[], byte[]> fetchRecords(Duration duration) {
        return consumer.poll(duration);
    }

    @Override
    protected void shutdownConsumer() {
        Utils.closeQuietly(consumer, "consumer");
        consumer = null;
    }
}
