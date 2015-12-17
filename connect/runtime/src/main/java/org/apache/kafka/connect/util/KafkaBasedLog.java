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
 **/

package org.apache.kafka.connect.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;

import static java.util.Collections.singleton;

/**
 * <p>
 *     KafkaBasedLog provides a generic implementation of a shared, compacted log of records stored in Kafka that all
 *     clients need to consume and, at times, agree on their offset / that they have read to the end of the log.
 * </p>
 * <p>
 *     This functionality is useful for storing different types of data that all clients may need to agree on --
 *     offsets or config for example. This class runs a consumer in a background thread to continuously tail the target
 *     topic, accepts write requests which it writes to the topic using an internal producer, and provides some helpful
 *     utilities like checking the current log end offset and waiting until the current end of the log is reached.
 * </p>
 * <p>
 *     To support different use cases, this class works with either single- or multi-partition topics.
 * </p>
 * <p>
 *     Since this class is generic, it delegates the details of data storage via a callback that is invoked for each
 *     record that is consumed from the topic. The invocation of callbacks is guaranteed to be serialized -- if the
 *     calling class keeps track of state based on the log and only writes to it when consume callbacks are invoked
 *     and only reads it in {@link #readToEnd(Callback)} callbacks then no additional synchronization will be required.
 * </p>
 */
public class KafkaBasedLog<K, V> {
    private static final Logger log = LoggerFactory.getLogger(KafkaBasedLog.class);
    private static final long CREATE_TOPIC_TIMEOUT_MS = 30000;

    private Time time;
    private final String topic;
    private final Map<String, Object> producerConfigs;
    private final Map<String, Object> consumerConfigs;
    private final Callback<ConsumerRecord<K, V>> consumedCallback;
    private Consumer<K, V> consumer;
    private Producer<K, V> producer;

    private Thread thread;
    private boolean stopRequested;
    private Queue<Callback<Void>> readLogEndOffsetCallbacks;

    /**
     * Create a new KafkaBasedLog object. This does not start reading the log and writing is not permitted until
     * {@link #start()} is invoked.
     *
     * @param topic the topic to treat as a log
     * @param producerConfigs configuration options to use when creating the internal producer. At a minimum this must
     *                        contain compatible serializer settings for the generic types used on this class. Some
     *                        setting, such as the number of acks, will be overridden to ensure correct behavior of this
     *                        class.
     * @param consumerConfigs configuration options to use when creating the internal consumer. At a minimum this must
     *                        contain compatible serializer settings for the generic types used on this class. Some
     *                        setting, such as the auto offset reset policy, will be overridden to ensure correct
     *                        behavior of this class.
     * @param consumedCallback callback to invoke for each {@link ConsumerRecord} consumed when tailing the log
     * @param time Time interface
     */
    public KafkaBasedLog(String topic,
                         Map<String, Object> producerConfigs,
                         Map<String, Object> consumerConfigs,
                         Callback<ConsumerRecord<K, V>> consumedCallback,
                         Time time) {
        this.topic = topic;
        this.producerConfigs = producerConfigs;
        this.consumerConfigs = consumerConfigs;
        this.consumedCallback = consumedCallback;
        this.stopRequested = false;
        this.readLogEndOffsetCallbacks = new ArrayDeque<>();
        this.time = time;
    }

    public void start() {
        log.info("Starting KafkaBasedLog with topic " + topic);

        producer = createProducer();
        consumer = createConsumer();

        List<TopicPartition> partitions = new ArrayList<>();

        // Until we have admin utilities we can use to check for the existence of this topic and create it if it is missing,
        // we rely on topic auto-creation
        List<PartitionInfo> partitionInfos = null;
        long started = time.milliseconds();
        while (partitionInfos == null && time.milliseconds() - started < CREATE_TOPIC_TIMEOUT_MS) {
            partitionInfos = consumer.partitionsFor(topic);
            Utils.sleep(Math.min(time.milliseconds() - started, 1000));
        }
        if (partitionInfos == null)
            throw new ConnectException("Could not look up partition metadata for offset backing store topic in" +
                    " allotted period. This could indicate a connectivity issue, unavailable topic partitions, or if" +
                    " this is your first use of the topic it may have taken too long to create.");

        for (PartitionInfo partition : partitionInfos)
            partitions.add(new TopicPartition(partition.topic(), partition.partition()));
        consumer.assign(partitions);

        readToLogEnd();

        thread = new WorkThread();
        thread.start();

        log.info("Finished reading KafkaBasedLog for topic " + topic);

        log.info("Started KafkaBasedLog for topic " + topic);
    }

    public void stop() {
        log.info("Stopping KafkaBasedLog for topic " + topic);

        synchronized (this) {
            stopRequested = true;
        }
        consumer.wakeup();

        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new ConnectException("Failed to stop KafkaBasedLog. Exiting without cleanly shutting " +
                    "down it's producer and consumer.", e);
        }

        try {
            producer.close();
        } catch (KafkaException e) {
            log.error("Failed to stop KafkaBasedLog producer", e);
        }

        try {
            consumer.close();
        } catch (KafkaException e) {
            log.error("Failed to stop KafkaBasedLog consumer", e);
        }

        log.info("Stopped KafkaBasedLog for topic " + topic);
    }

    /**
     * Flushes any outstanding writes and then reads to the current end of the log and invokes the specified callback.
     * Note that this checks the current, offsets, reads to them, and invokes the callback regardless of whether
     * additional records have been written to the log. If the caller needs to ensure they have truly reached the end
     * of the log, they must ensure there are no other writers during this period.
     *
     * This waits until the end of all partitions has been reached.
     *
     * This method is asynchronous. If you need a synchronous version, pass an instance of
     * {@link org.apache.kafka.connect.util.FutureCallback} as the {@param callback} parameter and wait on it to block.
     *
     * @param callback the callback to invoke once the end of the log has been reached.
     */
    public void readToEnd(Callback<Void> callback) {
        log.trace("Starting read to end log for topic {}", topic);
        producer.flush();
        synchronized (this) {
            readLogEndOffsetCallbacks.add(callback);
        }
        consumer.wakeup();
    }

    /**
     * Flush the underlying producer to ensure that all pending writes have been sent.
     */
    public void flush() {
        producer.flush();
    }

    /**
     * Same as {@link #readToEnd(Callback)} but provides a {@link Future} instead of using a callback.
     * @return the future associated with the operation
     */
    public Future<Void> readToEnd() {
        FutureCallback<Void> future = new FutureCallback<>(null);
        readToEnd(future);
        return future;
    }

    public void send(K key, V value) {
        send(key, value, null);
    }

    public void send(K key, V value, org.apache.kafka.clients.producer.Callback callback) {
        producer.send(new ProducerRecord<>(topic, key, value), callback);
    }


    private Producer<K, V> createProducer() {
        // Always require producer acks to all to ensure durable writes
        producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");

        // Don't allow more than one in-flight request to prevent reordering on retry (if enabled)
        producerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return new KafkaProducer<>(producerConfigs);
    }

    private Consumer<K, V> createConsumer() {
        // Always force reset to the beginning of the log since this class wants to consume all available log data
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Turn off autocommit since we always want to consume the full log
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(consumerConfigs);
    }

    private void poll(long timeoutMs) {
        try {
            ConsumerRecords<K, V> records = consumer.poll(timeoutMs);
            for (ConsumerRecord<K, V> record : records)
                consumedCallback.onCompletion(null, record);
        } catch (WakeupException e) {
            // Expected on get() or stop(). The calling code should handle this
            throw e;
        } catch (KafkaException e) {
            log.error("Error polling: " + e);
        }
    }

    private void readToLogEnd() {
        log.trace("Reading to end of offset log");

        Set<TopicPartition> assignment = consumer.assignment();

        // This approach to getting the current end offset is hacky until we have an API for looking these up directly
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (TopicPartition tp : assignment) {
            long offset = consumer.position(tp);
            offsets.put(tp, offset);
            consumer.seekToEnd(singleton(tp));
        }

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        try {
            poll(0);
        } finally {
            // If there is an exception, even a possibly expected one like WakeupException, we need to make sure
            // the consumers position is reset or it'll get into an inconsistent state.
            for (TopicPartition tp : assignment) {
                long startOffset = offsets.get(tp);
                long endOffset = consumer.position(tp);
                if (endOffset > startOffset) {
                    endOffsets.put(tp, endOffset);
                    consumer.seek(tp, startOffset);
                }
                log.trace("Reading to end of log for {}: starting offset {} to ending offset {}", tp, startOffset, endOffset);
            }
        }

        while (!endOffsets.isEmpty()) {
            poll(Integer.MAX_VALUE);

            Iterator<Map.Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<TopicPartition, Long> entry = it.next();
                if (consumer.position(entry.getKey()) >= entry.getValue())
                    it.remove();
                else
                    break;
            }
        }
    }


    private class WorkThread extends Thread {
        public WorkThread() {
            super("KafkaBasedLog Work Thread - " + topic);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    int numCallbacks;
                    synchronized (KafkaBasedLog.this) {
                        if (stopRequested)
                            break;
                        numCallbacks = readLogEndOffsetCallbacks.size();
                    }

                    if (numCallbacks > 0) {
                        try {
                            readToLogEnd();
                            log.trace("Finished read to end log for topic {}", topic);
                        } catch (WakeupException e) {
                            // Either received another get() call and need to retry reading to end of log or stop() was
                            // called. Both are handled by restarting this loop.
                            continue;
                        }
                    }

                    synchronized (KafkaBasedLog.this) {
                        // Only invoke exactly the number of callbacks we found before triggering the read to log end
                        // since it is possible for another write + readToEnd to sneak in in the meantime
                        for (int i = 0; i < numCallbacks; i++) {
                            Callback<Void> cb = readLogEndOffsetCallbacks.poll();
                            cb.onCompletion(null, null);
                        }
                    }

                    try {
                        poll(Integer.MAX_VALUE);
                    } catch (WakeupException e) {
                        // See previous comment, both possible causes of this wakeup are handled by starting this loop again
                        continue;
                    }
                }
            } catch (Throwable t) {
                log.error("Unexpected exception in KafkaBasedLog's work thread", t);
            }
        }
    }
}
