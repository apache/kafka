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

package org.apache.kafka.trogdor.common;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utilities for Trogdor TaskWorkers.
 */
public final class WorkerUtils {
    /**
     * Handle an exception in a TaskWorker.
     *
     * @param log               The logger to use.
     * @param what              The component that had the exception.
     * @param exception         The exception.
     * @param doneFuture        The TaskWorker's doneFuture
     * @throws KafkaException   A wrapped version of the exception.
     */
    public static void abort(Logger log, String what, Throwable exception,
            KafkaFutureImpl<String> doneFuture) throws KafkaException {
        log.warn("{} caught an exception: ", what, exception);
        doneFuture.complete(exception.getMessage());
        throw new KafkaException(exception);
    }

    /**
     * Convert a rate expressed per second to a rate expressed per the given period.
     *
     * @param perSec            The per-second rate.
     * @param periodMs          The new period to use.
     * @return                  The rate per period.  This will never be less than 1.
     */
    public static int perSecToPerPeriod(float perSec, long periodMs) {
        float period = ((float) periodMs) / 1000.0f;
        float perPeriod = perSec * period;
        perPeriod = Math.max(1.0f, perPeriod);
        return (int) perPeriod;
    }

    private static final int CREATE_TOPICS_REQUEST_TIMEOUT_MS = 25000;
    private static final int CREATE_TOPICS_CALL_TIMEOUT_MS = 360000;
    private static final int MAX_CREATE_TOPICS_BATCH_SIZE = 500;

    private static class NewTopicCreation {
        final NewTopic newTopic;
        final String errorMessage;

        NewTopicCreation(NewTopic newTopic, String errorMessage) {
            this.newTopic = newTopic;
            this.errorMessage = errorMessage;
        }
    }

    /**
     * Create some Kafka topics.
     *
     * @param log               The logger to use.
     * @param bootstrapServers  The bootstrap server list.
     * @param newTopics         The new topics to create.
     */
    public static void createTopics(final Logger log, String bootstrapServers,
            final Collection<NewTopic> newTopics) throws Throwable {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, CREATE_TOPICS_REQUEST_TIMEOUT_MS);
        final ConcurrentLinkedDeque<NewTopicCreation> pendingCreations = new ConcurrentLinkedDeque<>();
        for (NewTopic newTopic : newTopics) {
            pendingCreations.offer(new NewTopicCreation(newTopic, ""));
        }
        final AtomicInteger remainingCreations = new AtomicInteger(newTopics.size());
        final AtomicInteger topicsCreated = new AtomicInteger(0);
        final AtomicInteger topicsVerified = new AtomicInteger(0);
        final KafkaFutureImpl<Void> doneFuture = new KafkaFutureImpl<>();
        try (AdminClient adminClient = AdminClient.create(props)) {
            final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("createTopicsThread", false));
            try {
                executorService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try {
                            while (true) {
                                final List<NewTopicCreation> batch = getBatch(MAX_CREATE_TOPICS_BATCH_SIZE);
                                if (batch.isEmpty()) {
                                    break;
                                }
                                TreeMap<String, List<String>> descriptions = new TreeMap<>();
                                final HashMap<String, NewTopic> topics = new HashMap<>();
                                for (NewTopicCreation newTopicCreation : batch) {
                                    List<String> list = descriptions.get(newTopicCreation.errorMessage);
                                    if (list == null) {
                                        list = new ArrayList<>();
                                        descriptions.put(newTopicCreation.errorMessage, list);
                                    }
                                    list.add(newTopicCreation.newTopic.name());
                                    topics.put(newTopicCreation.newTopic.name(), newTopicCreation.newTopic);
                                }
                                for (Map.Entry<String, List<String>> entry : descriptions.entrySet()) {
                                    if (!entry.getKey().isEmpty()) {
                                        log.info("Got error {} when attempting to create topic(s): {}",
                                            entry.getKey(), Utils.join(entry.getValue(), ", "));
                                    }
                                }
                                log.info("Creating topic(s) {}", Utils.join(topics.values(), ", "));
                                CreateTopicsResult result = adminClient.createTopics(topics.values());
                                for (final Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
                                    final String topicName = entry.getKey();
                                    final KafkaFuture<Void> future = entry.getValue();
                                    future.whenComplete(new KafkaFuture.BiConsumer<Void, Throwable>() {
                                        @Override
                                        public void accept(Void v, Throwable e) {
                                            if (e == null) {
                                                log.trace("Successfully created topic {}", topicName);
                                                topicsCreated.incrementAndGet();
                                                if (remainingCreations.decrementAndGet() <= 0) {
                                                    doneFuture.complete(null);
                                                }
                                            } else if (e instanceof TopicExistsException) {
                                                log.trace("Topic {} already exists.", topicName);
                                                topicsVerified.incrementAndGet();
                                                if (remainingCreations.decrementAndGet() <= 0) {
                                                    doneFuture.complete(null);
                                                }
                                            } else {
                                                log.trace("Failed to create {}: {}", topicName, e.getMessage());
                                                pendingCreations.add(new NewTopicCreation(topics.get(topicName),
                                                    e.getClass().getSimpleName()));
                                            }
                                        }
                                    });
                                }
                            }
                            executorService.schedule(this,
                                2 * CREATE_TOPICS_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        } catch (Throwable t) {
                            log.error("Exception while preparing AdminClient operations", t);
                            doneFuture.completeExceptionally(t);
                        }
                        return null;
                    }

                    private List<NewTopicCreation> getBatch(int maxSize) {
                        List<NewTopicCreation> batch = new ArrayList<>();
                        do {
                            NewTopicCreation newTopicCreation = pendingCreations.poll();
                            if (newTopicCreation == null) {
                                break;
                            }
                            batch.add(newTopicCreation);
                        } while (batch.size() < maxSize);
                        return batch;
                    }
                });
                doneFuture.get(CREATE_TOPICS_CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                log.info("CreateTopics completed.  topicsCreated = {}, topicsVerified = {}",
                    topicsCreated.get(), topicsVerified.get());
            } finally {
                try {
                    executorService.shutdownNow();
                    executorService.awaitTermination(1, TimeUnit.DAYS);
                } catch (Throwable e) {
                    log.error("Error shutting down executorService", e);
                }
            }
        }
    }
}
