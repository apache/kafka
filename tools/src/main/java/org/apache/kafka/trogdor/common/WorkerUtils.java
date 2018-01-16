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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

    private static final int CREATE_TOPICS_REQUEST_TIMEOUT = 25000;
    private static final int CREATE_TOPICS_CALL_TIMEOUT = 90000;
    private static final int MAX_CREATE_TOPICS_BATCH_SIZE = 10;

            //Map<String, Map<Integer, List<Integer>>> topics) throws Throwable {

    /**
     * Create some Kafka topics.
     *
     * @param log               The logger to use.
     * @param bootstrapServers  The bootstrap server list.
     * @param topics            Maps topic names to partition assignments.
     */
    public static void createTopics(Logger log, String bootstrapServers,
            Collection<NewTopic> topics) throws Throwable {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, CREATE_TOPICS_REQUEST_TIMEOUT);
        try (AdminClient adminClient = AdminClient.create(props)) {
            long startMs = Time.SYSTEM.milliseconds();
            int tries = 0;

            Map<String, NewTopic> newTopics = new HashMap<>();
            for (NewTopic newTopic : topics) {
                newTopics.put(newTopic.name(), newTopic);
            }
            List<String> topicsToCreate = new ArrayList<>(newTopics.keySet());
            while (true) {
                log.info("Attemping to create {} topics (try {})...", topicsToCreate.size(), ++tries);
                Map<String, Future<Void>> creations = new HashMap<>();
                while (!topicsToCreate.isEmpty()) {
                    List<NewTopic> newTopicsBatch = new ArrayList<>();
                    for (int i = 0; (i < MAX_CREATE_TOPICS_BATCH_SIZE) &&
                            !topicsToCreate.isEmpty(); i++) {
                        String topicName = topicsToCreate.remove(0);
                        newTopicsBatch.add(newTopics.get(topicName));
                    }
                    creations.putAll(adminClient.createTopics(newTopicsBatch).values());
                }
                // We retry cases where the topic creation failed with a
                // timeout.  This is a workaround for KAFKA-6368.
                for (Map.Entry<String, Future<Void>> entry : creations.entrySet()) {
                    String topicName = entry.getKey();
                    Future<Void> future = entry.getValue();
                    try {
                        future.get();
                        log.debug("Successfully created {}.", topicName);
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof TimeoutException) {
                            log.warn("Timed out attempting to create {}: {}", topicName, e.getCause().getMessage());
                            topicsToCreate.add(topicName);
                        } else {
                            log.warn("Failed to create {}", topicName, e.getCause());
                            throw e.getCause();
                        }
                    }
                }
                if (topicsToCreate.isEmpty()) {
                    break;
                }
                if (Time.SYSTEM.milliseconds() > startMs + CREATE_TOPICS_CALL_TIMEOUT) {
                    String str = "Unable to create topic(s): " +
                            Utils.join(topicsToCreate, ", ") + "after " + tries + " attempt(s)";
                    log.warn(str);
                    throw new TimeoutException(str);
                }
            }
        }
    }
}
