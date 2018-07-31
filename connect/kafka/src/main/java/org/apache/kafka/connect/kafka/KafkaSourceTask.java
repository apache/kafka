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
package org.apache.kafka.connect.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.time.Duration;


public class KafkaSourceTask extends SourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceTask.class);
    public static final String TOPIC_PARTITION_KEY = "topic:partition";
    public static final String OFFSET_KEY = "offset";

    // Used to ensure we can be nice and call consumer.close() on shutdown
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    // Flag to the poll() loop that we are awaiting shutdown so it can clean up.
    private AtomicBoolean stop = new AtomicBoolean(false);
    // Flag to the stop() function that it needs to wait for poll() to wrap up before trying to close the kafka consumer.
    private AtomicBoolean poll = new AtomicBoolean(false);
    // Used to enforce synchronized access to stop and poll
    private final Object stopLock = new Object();

    // Settings
    private int maxShutdownWait;
    private int pollTimeout;
    private String topicPrefix;
    private boolean includeHeaders;

    // Consumer
    private KafkaConsumer<byte[], byte[]> consumer;

    public void start(Map<String, String> opts) {
        LOG.info("{}: task is starting.", this);
        KafkaSourceConnectorConfig sourceConnectorConfig = new KafkaSourceConnectorConfig(opts);
        maxShutdownWait = sourceConnectorConfig.getInt(KafkaSourceConnectorConfig.MAX_SHUTDOWN_WAIT_MS_CONFIG);
        pollTimeout = sourceConnectorConfig.getInt(KafkaSourceConnectorConfig.POLL_LOOP_TIMEOUT_MS_CONFIG);
        topicPrefix = sourceConnectorConfig.getString(KafkaSourceConnectorConfig.DESTINATION_TOPIC_PREFIX_CONFIG);
        includeHeaders = sourceConnectorConfig.getBoolean(KafkaSourceConnectorConfig.INCLUDE_MESSAGE_HEADERS_CONFIG);
        String unknownOffsetResetPosition = sourceConnectorConfig.getString(KafkaSourceConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG);
        // Get the leader topic partitions to work with
        List<LeaderTopicPartition> leaderTopicPartitions = Arrays.asList(opts.get(KafkaSourceConnectorConfig.TASK_LEADER_TOPIC_PARTITION_CONFIG)
            .split(","))
            .stream()
            .map(LeaderTopicPartition::fromString)
            .collect(Collectors.toList());
        // retrieve the existing offsets (if any) for the configured partitions
        List<Map<String, String>> offsetLookupPartitions = leaderTopicPartitions.stream()
                .map(leaderTopicPartition -> Collections.singletonMap(TOPIC_PARTITION_KEY, leaderTopicPartition.toTopicPartitionString()))
                .collect(Collectors.toList());
        Map<String, Long> topicPartitionStringsOffsets = context.offsetStorageReader().offsets(offsetLookupPartitions)
            .entrySet()
            .stream()
            .filter(e -> e != null && e.getKey() != null && e.getKey().get(TOPIC_PARTITION_KEY) != null && e.getValue() != null && e.getValue().get(OFFSET_KEY) != null)
            .collect(Collectors.toMap(e -> e.getKey().get(TOPIC_PARTITION_KEY), e -> (long) e.getValue().get(OFFSET_KEY)));
        // Set up Kafka consumer
        consumer = new KafkaConsumer<byte[], byte[]>(sourceConnectorConfig.getKafkaConsumerProperties());
        // Get topic partitions and offsets so we can seek() to them
        Map<TopicPartition, Long> topicPartitionOffsets = new HashMap<>();
        List<TopicPartition> topicPartitionsWithUnknownOffset = new ArrayList<>();
        for (LeaderTopicPartition leaderTopicPartition : leaderTopicPartitions) {
            String topicPartitionString = leaderTopicPartition.toTopicPartitionString();
            TopicPartition topicPartition = leaderTopicPartition.toTopicPartition();
            if (topicPartitionStringsOffsets.containsKey(topicPartitionString)) {
                topicPartitionOffsets.put(topicPartition, topicPartitionStringsOffsets.get(topicPartitionString));
            } else {
                // No stored offset? No worries, we will place it it the list to lookup
                topicPartitionsWithUnknownOffset.add(topicPartition);
            }
        }
        // Set default offsets for partitions without stored offsets
        if (topicPartitionsWithUnknownOffset.size() > 0) {
            Map<TopicPartition, Long> defaultOffsets;
            LOG.info("The following partitions do not have existing offset data: {}", topicPartitionsWithUnknownOffset);
            if (unknownOffsetResetPosition.equals("earliest")) {
                LOG.info("Using earliest offsets for partitions without existing offset data.");
                defaultOffsets = consumer.beginningOffsets(topicPartitionsWithUnknownOffset);
            } else if (unknownOffsetResetPosition.equals("latest")) {
                LOG.info("Using latest offsets for partitions without existing offset data.");
                defaultOffsets = consumer.endOffsets(topicPartitionsWithUnknownOffset);
            } else {
                LOG.warn("Config value {}, is set to an unknown value: {}. Partitions without existing offset data will not be consumed.", KafkaSourceConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG, unknownOffsetResetPosition);
                defaultOffsets = new HashMap<>();
            }
            topicPartitionOffsets.putAll(defaultOffsets);
        }
        // List of topic partitions to assign
        List<TopicPartition> topicPartitionsToAssign = new ArrayList<>(topicPartitionOffsets.keySet());
        consumer.assign(topicPartitionsToAssign);
        // Seek to desired offset for each partition
        topicPartitionOffsets.forEach((key, value) -> consumer.seek(key, value));
    }


    @Override
    public List<SourceRecord> poll() {
        if (LOG.isDebugEnabled()) LOG.debug("{}: poll()", this);
        synchronized (stopLock) {
            if (!stop.get())
                poll.set(true);
        }
        ArrayList<SourceRecord> records = new ArrayList<>();
        if (poll.get()) {
            try {
                ConsumerRecords<byte[], byte[]> krecords = consumer.poll(Duration.ofMillis(pollTimeout));
                if (LOG.isDebugEnabled()) LOG.debug("{}: Got {} records from source.", this, krecords.count());
                for (ConsumerRecord<byte[], byte[]> krecord : krecords) {
                    Map<String, String> sourcePartition = Collections.singletonMap(TOPIC_PARTITION_KEY, krecord.topic().concat(":").concat(Integer.toString(krecord.partition())));
                    Map<String, Long> sourceOffset = Collections.singletonMap(OFFSET_KEY, krecord.offset());
                    String destinationTopic = topicPrefix.concat(krecord.topic());
                    if (LOG.isDebugEnabled()) {
                        LOG.trace(
                                "Task: sourceTopic:{} sourcePartition:{} sourceOffSet:{} destinationTopic:{}, key:{}, valueSize:{}",
                                krecord.topic(), krecord.partition(), krecord.offset(), destinationTopic, krecord.key(), krecord.serializedValueSize()
                        );
                    }
                    if (includeHeaders) {
                        // Mapping from source type: org.apache.kafka.common.header.Headers, to destination type: org.apache.kafka.connect.Headers
                        Headers sourceHeaders = krecord.headers();
                        ConnectHeaders destinationHeaders = new ConnectHeaders();
                        for (Header header: sourceHeaders) {
                            if (header != null) {
                                destinationHeaders.add(header.key(), header.value(), Schema.OPTIONAL_BYTES_SCHEMA);
                            }
                        }
                        records.add(new SourceRecord(sourcePartition, sourceOffset, destinationTopic, null, Schema.OPTIONAL_BYTES_SCHEMA, krecord.key(), Schema.OPTIONAL_BYTES_SCHEMA, krecord.value(), krecord.timestamp(), destinationHeaders));
                    } else {
                        records.add(new SourceRecord(sourcePartition, sourceOffset, destinationTopic, null, Schema.OPTIONAL_BYTES_SCHEMA, krecord.key(), Schema.OPTIONAL_BYTES_SCHEMA, krecord.value(), krecord.timestamp()));
                    }
                }
            } catch (WakeupException e) {
                LOG.info("{}: Caught WakeupException. Probably shutting down.", this);
            }
        }
        poll.set(false);
        // If stop has been set  processing, then stop the consumer.
        if (stop.get()) {
            LOG.debug("{}: stop flag set during poll(), opening stopLatch", this);
            stopLatch.countDown();
        }
        if (LOG.isDebugEnabled()) LOG.debug("{}: Returning {} records to connect", this, records.size());
        return records;
    }

    @Override
    public synchronized void stop() {
        long startWait = System.currentTimeMillis();
        synchronized (stopLock) {
            stop.set(true);
            LOG.info("{}: stop() called. Waking up consumer and shutting down", this);
            consumer.wakeup();
            if (poll.get()) {
                LOG.info("{}: poll() active, awaiting for consumer to wake before attempting to shut down consumer", this);
                try {
                    stopLatch.await(Math.max(0, maxShutdownWait - (System.currentTimeMillis() - startWait)), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.warn("{}: Got InterruptedException while waiting on stopLatch", this);
                }
            }
            LOG.info("{}: Shutting down consumer.", this);
            consumer.close(Duration.ofMillis(Math.max(0, maxShutdownWait - (System.currentTimeMillis() - startWait))));
        }
        LOG.info("{}: task has been stopped", this);
    }



    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    public String toString() {
        return "KafkaSourceTask@" + Integer.toHexString(hashCode());
    }


}