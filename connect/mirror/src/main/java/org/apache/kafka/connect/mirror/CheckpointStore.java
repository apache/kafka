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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.mirror.MirrorCheckpointConfig.CHECKPOINTS_TARGET_CONSUMER_ROLE;

/**
 * Reads once the Kafka log for checkpoints and populates a map of
 * checkpoints per consumer group.
 *
 * The Kafka log is closed after the initial load and only the in memory map is
 * used after start.
 */
public class CheckpointStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CheckpointStore.class);

    private final MirrorCheckpointTaskConfig config;
    private final Set<String> consumerGroups;

    private TopicAdmin cpAdmin = null;
    private KafkaBasedLog<byte[], byte[]> backingStore = null;
    // accessible for testing
    Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup;

    private volatile boolean loadSuccess = false;
    private volatile boolean isInitialized = false;
    private volatile boolean initializationStopped = false;

    // Guards cpConsumer, so that stopInitialization() cannot wake up a consumer which the thread
    // reading the checkpoints topic is closing at the same time.
    private final Object consumerLock = new Object();
    private Consumer<byte[], byte[]> cpConsumer = null;

    public CheckpointStore(MirrorCheckpointTaskConfig config, Set<String> consumerGroups) {
        this.config = config;
        this.consumerGroups = new HashSet<>(consumerGroups);
    }

    // constructor for testing only
    CheckpointStore(Map<String, Map<TopicPartition, Checkpoint>> checkpointsPerConsumerGroup) {
        this.config = null; //ignored by tests
        this.consumerGroups = null; //ignored by tests
        this.checkpointsPerConsumerGroup = checkpointsPerConsumerGroup;
        isInitialized = true;
        loadSuccess =  true;
    }

    // potentially long running
    public boolean start()  {
        checkpointsPerConsumerGroup = readCheckpoints();
        isInitialized = true;
        if (log.isTraceEnabled()) {
            log.trace("CheckpointStore started, load success={}, map={}", loadSuccess, checkpointsPerConsumerGroup);
        } else {
            log.debug("CheckpointStore started, load success={}, map.size={}", loadSuccess, checkpointsPerConsumerGroup.size());
        }
        return loadSuccess;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    /**
     * Stop an initialization which is in progress, and prevent one which has not begun from starting, so that
     * {@link #close()} does not have to wait for the whole checkpoints topic to be read.
     * <p>This is the only method of this class which may be called by a thread other than the one which calls
     * {@link #start()}. {@link #close()} must not be called until that thread has returned from {@link #start()},
     * because the consumer used to read the checkpoints topic is not safe for multi-threaded access.
     */
    void stopInitialization() {
        synchronized (consumerLock) {
            initializationStopped = true;
            if (cpConsumer != null) {
                // Consumer::wakeup is the only consumer method which is safe to call from a thread other than
                // the one using the consumer. It makes the read performed by KafkaBasedLog::start give up with
                // a WakeupException instead of running all the way to the end of the topic.
                cpConsumer.wakeup();
            }
        }
    }

    public void update(String group, Map<TopicPartition, Checkpoint> newCheckpoints) {
        Map<TopicPartition, Checkpoint> oldCheckpoints = checkpointsPerConsumerGroup.computeIfAbsent(group, ignored -> new HashMap<>());
        oldCheckpoints.putAll(newCheckpoints);
    }

    public Map<TopicPartition, Checkpoint> get(String group) {
        Map<TopicPartition, Checkpoint> result = checkpointsPerConsumerGroup.get(group);
        return result == null ? null : Map.copyOf(result);
    }

    public Map<String, Map<TopicPartition, OffsetAndMetadata>> computeConvertedUpstreamOffset() {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> result = new HashMap<>();

        for (Map.Entry<String, Map<TopicPartition, Checkpoint>> entry : checkpointsPerConsumerGroup.entrySet()) {
            String consumerId = entry.getKey();
            Map<TopicPartition, OffsetAndMetadata> convertedUpstreamOffset = new HashMap<>();
            for (Checkpoint checkpoint : entry.getValue().values()) {
                convertedUpstreamOffset.put(checkpoint.topicPartition(), checkpoint.offsetAndMetadata());
            }
            result.put(consumerId, convertedUpstreamOffset);
        }
        return result;
    }

    @Override
    public void close() {
        releaseResources();
    }

    private void releaseResources() {
        synchronized (consumerLock) {
            cpConsumer = null;
        }
        Utils.closeQuietly(backingStore != null ? backingStore::stop : null, "backing store for previous Checkpoints");
        Utils.closeQuietly(cpAdmin, "admin client for previous Checkpoints");
        cpAdmin = null;
        backingStore = null;
    }

    // read the checkpoints topic to initialize the checkpointsPerConsumerGroup state
    // the callback may only handle errors thrown by consumer.poll in KafkaBasedLog
    // e.g. unauthorized to read from topic (non-retriable)
    // if any are encountered, treat the loading of Checkpoints as failed.
    private Map<String, Map<TopicPartition, Checkpoint>> readCheckpoints() {
        Map<String, Map<TopicPartition, Checkpoint>> checkpoints = new HashMap<>();
        Callback<ConsumerRecord<byte[], byte[]>> consumedCallback = (error, cpRecord) -> {
            if (error != null) {
                // if there is no authorization to READ from the topic, we must throw an error
                // to stop the KafkaBasedLog forever looping attempting to read to end
                checkpoints.clear();
                if (error instanceof RuntimeException) {
                    throw (RuntimeException) error;
                } else {
                    throw new RuntimeException(error);
                }
            } else {
                try {
                    Checkpoint cp = Checkpoint.deserializeRecord(cpRecord);
                    if (consumerGroups.contains(cp.consumerGroupId())) {
                        Map<TopicPartition, Checkpoint> cps = checkpoints.computeIfAbsent(cp.consumerGroupId(), ignored1 -> new HashMap<>());
                        cps.put(cp.topicPartition(), cp);
                    }
                } catch (SchemaException ex) {
                    log.warn("Ignored invalid checkpoint record at offset {}", cpRecord.offset(), ex);
                }
            }
        };

        if (initializationStopped) {
            log.debug("Not reading checkpoints topic {} because the task is shutting down", config.checkpointsTopic());
            loadSuccess = false;
            return checkpoints;
        }

        try {
            long startTime = System.currentTimeMillis();
            readCheckpointsImpl(config, consumedCallback);
            log.debug("starting+stopping KafkaBasedLog took {}ms", System.currentTimeMillis() - startTime);
            // a read which was stopped before it reached the end of the topic has not loaded all of the checkpoints
            loadSuccess = !initializationStopped;
        } catch (Exception error) {
            loadSuccess = false;
            if (initializationStopped) {
                log.debug("Stopped reading checkpoints topic {} because the task is shutting down",
                        config.checkpointsTopic(), error);
            } else if (error instanceof AuthorizationException) {
                log.warn("Not authorized to access checkpoints topic {} - " +
                        "this may degrade offset translation as only checkpoints " +
                        "for offsets which were mirrored after the task started will be emitted",
                        config.checkpointsTopic(), error);
            } else {
                log.info("Exception encountered loading checkpoints topic {} - " +
                        "this may degrade offset translation as only checkpoints " +
                        "for offsets which were mirrored after the task started will be emitted",
                        config.checkpointsTopic(), error);
            }
        }
        return checkpoints;
    }

    // accessible for testing
    void readCheckpointsImpl(MirrorCheckpointTaskConfig config, Callback<ConsumerRecord<byte[], byte[]>> consumedCallback) {
        try {
            cpAdmin = new TopicAdmin(
                    config.targetAdminConfig("checkpoint-target-admin"),
                    config.forwardingAdmin(config.targetAdminConfig("checkpoint-target-admin")));

            Consumer<byte[], byte[]> consumer =
                    MirrorUtils.newConsumer(config.targetConsumerConfig(CHECKPOINTS_TARGET_CONSUMER_ROLE));
            synchronized (consumerLock) {
                cpConsumer = consumer;
                if (initializationStopped) {
                    // A stop which arrived before the consumer existed could not wake it up, so wake it up here:
                    // the first blocking call made by the backing store then gives up immediately.
                    consumer.wakeup();
                }
            }

            backingStore = KafkaBasedLog.withExistingClients(
                    config.checkpointsTopic(),
                    consumer,
                    null,
                    cpAdmin,
                    consumedCallback,
                    Time.SYSTEM,
                    ignored -> {
                    },
                    topicPartition -> topicPartition.partition() == 0);

            backingStore.start(true);
            backingStore.stop();
        } finally {
            releaseResources();
        }
    }
}
