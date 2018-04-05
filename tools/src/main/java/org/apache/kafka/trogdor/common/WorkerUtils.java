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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
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

    /**
     * Adds all properties from commonConf and then from clientConf to given 'props' (in
     * that order, over-writing properties with the same keys).
     * @param props              Properties object that may contain zero or more properties
     * @param commonConf         Map with common client properties
     * @param clientConf         Map with client properties
     */
    public static void addConfigsToProperties(
        Properties props, Map<String, String> commonConf, Map<String, String> clientConf) {
        for (Map.Entry<String, String> commonEntry : commonConf.entrySet()) {
            props.setProperty(commonEntry.getKey(), commonEntry.getValue());
        }
        for (Map.Entry<String, String> entry : clientConf.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
    }

    private static final int CREATE_TOPICS_REQUEST_TIMEOUT = 25000;
    private static final int CREATE_TOPICS_CALL_TIMEOUT = 180000;
    private static final int MAX_CREATE_TOPICS_BATCH_SIZE = 10;

            //Map<String, Map<Integer, List<Integer>>> topics) throws Throwable {

    /**
     * Create some Kafka topics.
     *
     * @param log               The logger to use.
     * @param bootstrapServers  The bootstrap server list.
     * @param commonClientConf  Common client config
     * @param adminClientConf   AdminClient config. This config has precedence over fields in
     *                          common client config.
     * @param topics            Maps topic names to partition assignments.
     * @param failOnExisting    If true, the method will throw TopicExistsException if one or
     *                          more topics already exist. Otherwise, the existing topics are
     *                          verified for number of partitions. In this case, if number of
     *                          partitions of an existing topic does not match the requested
     *                          number of partitions, the method throws RuntimeException.
     */
    public static void createTopics(
        Logger log, String bootstrapServers, Map<String, String> commonClientConf,
        Map<String, String> adminClientConf,
        Map<String, NewTopic> topics, boolean failOnExisting) throws Throwable {
        // this method wraps the call to createTopics() that takes admin client, so that we can
        // unit test the functionality with MockAdminClient. The exception is caught and
        // re-thrown so that admin client is closed when the method returns.
        try (AdminClient adminClient
                 = createAdminClient(bootstrapServers, commonClientConf, adminClientConf)) {
            createTopics(log, adminClient, topics, failOnExisting);
        } catch (Exception e) {
            log.warn("Failed to create or verify topics {}", topics, e);
            throw e;
        }
    }

    /**
     * The actual create topics functionality is separated into this method and called from the
     * above method to be able to unit test with mock adminClient.
     */
    static void createTopics(
        Logger log, AdminClient adminClient,
        Map<String, NewTopic> topics, boolean failOnExisting) throws Throwable {
        if (topics.isEmpty()) {
            log.warn("Request to create topics has an empty topic list.");
            return;
        }

        Collection<String> topicsExists = createTopics(log, adminClient, topics.values());
        if (!topicsExists.isEmpty()) {
            if (failOnExisting) {
                log.warn("Topic(s) {} already exist.", topicsExists);
                throw new TopicExistsException("One or more topics already exist.");
            } else {
                verifyTopics(log, adminClient, topicsExists, topics);
            }
        }
    }

    /**
     * Creates Kafka topics and returns a list of topics that already exist
     * @param log             The logger to use
     * @param adminClient     AdminClient
     * @param topics          List of topics to create
     * @return                Collection of topics names that already exist.
     * @throws Throwable if creation of one or more topics fails (except for topic exists case).
     */
    private static Collection<String> createTopics(Logger log, AdminClient adminClient,
                                                   Collection<NewTopic> topics) throws Throwable {
        long startMs = Time.SYSTEM.milliseconds();
        int tries = 0;
        List<String> existingTopics = new ArrayList<>();

        Map<String, NewTopic> newTopics = new HashMap<>();
        for (NewTopic newTopic : topics) {
            newTopics.put(newTopic.name(), newTopic);
        }
        List<String> topicsToCreate = new ArrayList<>(newTopics.keySet());
        while (true) {
            log.info("Attempting to create {} topics (try {})...", topicsToCreate.size(), ++tries);
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
                } catch (Exception e) {
                    if ((e.getCause() instanceof TimeoutException)
                        || (e.getCause() instanceof NotEnoughReplicasException)) {
                        log.warn("Attempt to create topic `{}` failed: {}", topicName,
                                 e.getCause().getMessage());
                        topicsToCreate.add(topicName);
                    } else if (e.getCause() instanceof TopicExistsException) {
                        log.info("Topic {} already exists.", topicName);
                        existingTopics.add(topicName);
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
        return existingTopics;
    }

    /**
     * Verifies that topics in 'topicsToVerify' list have the same number of partitions as
     * described in 'topicsInfo'
     * @param log                The logger to use
     * @param adminClient        AdminClient
     * @param topicsToVerify     List of topics to verify
     * @param topicsInfo         Map of topic name to topic description, which includes topics in
     *                           'topicsToVerify' list.
     * @throws RuntimeException  If one or more topics have different number of partitions than
     * described in 'topicsInfo'
     */
    private static void verifyTopics(
        Logger log, AdminClient adminClient,
        Collection<String> topicsToVerify, Map<String, NewTopic> topicsInfo) throws Throwable {
        DescribeTopicsResult topicsResult = adminClient.describeTopics(
            topicsToVerify, new DescribeTopicsOptions().timeoutMs(CREATE_TOPICS_REQUEST_TIMEOUT));
        Map<String, TopicDescription> topicDescriptionMap = topicsResult.all().get();
        for (TopicDescription desc: topicDescriptionMap.values()) {
            // map will always contain the topic since all topics in 'topicsExists' are in given
            // 'topics' map
            int partitions = topicsInfo.get(desc.name()).numPartitions();
            if (desc.partitions().size() != partitions) {
                String str = "Topic '" + desc.name() + "' exists, but has "
                             + desc.partitions().size() + " partitions, while requested "
                             + " number of partitions is " + partitions;
                log.warn(str);
                throw new RuntimeException(str);
            }
        }
    }

    private static AdminClient createAdminClient(
        String bootstrapServers, Map<String, String> commonClientConf,
        Map<String, String> adminClientConf) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, CREATE_TOPICS_REQUEST_TIMEOUT);
        // first add common client config, and then admin client config to properties, possibly
        // over-writing default or common properties.
        addConfigsToProperties(props, commonClientConf, adminClientConf);
        return AdminClient.create(props);
    }
}
