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
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
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
        try (AdminClient adminClient = createAdminClient(log, bootstrapServers)) {
            createTopics(log, adminClient, topics);
        }
    }

    static void createTopics(Logger log, AdminClient adminClient,
                             Collection<NewTopic> topics) throws Throwable {
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

    /**
     * Verifies that topics exist with the same number of partitions. If any of the topics do not
     * exist, the method will create them. If any topic exists, but has a different number of
     * partitions, the method throws an exception.
     *
     * @param log                  The logger to use.
     * @param bootstrapServers     The bootstrap server list.
     * @param topics               topic name to topic description map representing a list of
     *                             topics to verify/create
     */
    public static void verifyTopicsAndCreateNonExistingTopics(
        Logger log, String bootstrapServers, Map<String, NewTopic> topics) throws Throwable {
        try (AdminClient adminClient = createAdminClient(log, bootstrapServers)) {
            verifyTopicsAndCreateNonExistingTopics(log, adminClient, topics);
        }
    }

    static void verifyTopicsAndCreateNonExistingTopics(
        Logger log, AdminClient adminClient, Map<String, NewTopic> topics) throws Throwable {
        if (topics.isEmpty()) {
            log.warn("Request to create topics has an empty topic list.");
            return;
        }

        ListTopicsOptions listTopicsOpts =
            new ListTopicsOptions().timeoutMs(CREATE_TOPICS_REQUEST_TIMEOUT);
        ListTopicsResult result = adminClient.listTopics(listTopicsOpts);
        Collection<String> topicNames = result.names().get();

        // verify that existing topics have the same number of partitions that was requested
        topicNames.retainAll(topics.keySet());
        verifyTopicsPartitions(log, adminClient, topicNames, topics);

        // create topics that do not exist
        Collection<NewTopic> topicsToCreate = new ArrayList<>();
        for (Map.Entry<String, NewTopic> topic: topics.entrySet()) {
            if (!topicNames.contains(topic.getKey())) {
                topicsToCreate.add(topic.getValue());
            }
        }
        if (topicsToCreate.size() > 0) {
            createTopics(log, adminClient, topicsToCreate);
        }
    }

    /**
     * Verifies that all topics in the list have given number of partitions
     * @param log                     The logger to use.
     * @param existingTopics          List of topics to verify
     * @param requestedTopicConfigs   Topic name to topic description. This map must contain all
     *                                topics in existingTopics list
     */
    private static void verifyTopicsPartitions(
        Logger log, AdminClient adminClient, Collection<String> existingTopics,
        Map<String, NewTopic> requestedTopicConfigs) throws Exception {
        if (existingTopics.isEmpty()) {
            return;
        }
        DescribeTopicsResult topicsResult = adminClient.describeTopics(
            existingTopics, new DescribeTopicsOptions().timeoutMs(CREATE_TOPICS_REQUEST_TIMEOUT));
        Map<String, TopicDescription> topicDescriptionMap = topicsResult.all().get();
        for (TopicDescription desc: topicDescriptionMap.values()) {
            // map will always contain the topic since we get the intersection of requested
            // topics and existing topics before calling this method.
            int partitions = requestedTopicConfigs.get(desc.name()).numPartitions();
            if (desc.partitions().size() != partitions) {
                String str = "Topic '" + desc.name() + "' exists, but has "
                             + desc.partitions().size() + " partitions, while requested "
                             + " number of partitions is " + partitions;
                log.warn(str);
                throw new IllegalArgumentException(str);
            }
        }
    }

    private static AdminClient createAdminClient(Logger log, String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, CREATE_TOPICS_REQUEST_TIMEOUT);
        return AdminClient.create(props);
    }
}
