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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class PartitionMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionMonitor.class);

    // No need to make these options configurable.

    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private AdminClient partitionMonitorClient;
    private Pattern topicWhitelistPattern;
    private volatile Set<LeaderTopicPartition> currentLeaderTopicPartitions = new HashSet<>();

    private int maxShutdownWaitMs;
    private int topicRequestTimeoutMs;
    private boolean reconfigureTasksOnLeaderChange;
    private Runnable pollThread;
    private int topicPollIntervalMs;
    private ScheduledExecutorService pollExecutorService;
    private ScheduledFuture<?> pollHandle;

    PartitionMonitor(ConnectorContext connectorContext, KafkaSourceConnectorConfig sourceConnectorConfig) {
        topicWhitelistPattern = sourceConnectorConfig.getTopicWhitelistPattern();
        reconfigureTasksOnLeaderChange = sourceConnectorConfig.getBoolean(KafkaSourceConnectorConfig.RECONFIGURE_TASKS_ON_LEADER_CHANGE_CONFIG);
        topicPollIntervalMs = sourceConnectorConfig.getInt(KafkaSourceConnectorConfig.TOPIC_LIST_POLL_INTERVAL_MS_CONFIG);
        maxShutdownWaitMs = sourceConnectorConfig.getInt(KafkaSourceConnectorConfig.MAX_SHUTDOWN_WAIT_MS_CONFIG);
        topicRequestTimeoutMs = sourceConnectorConfig.getInt(KafkaSourceConnectorConfig.TOPIC_LIST_TIMEOUT_MS_CONFIG);
        partitionMonitorClient = AdminClient.create(sourceConnectorConfig.getAdminClientProperties());
        // Thread to periodically poll the kafka cluster for changes in topics or partitions
        pollThread = new Runnable() {
            @Override
            public void run() {
                if (!shutdown.get()) {
                    LOG.info("Fetching latest topic partitions.");
                    try {
                        Set<LeaderTopicPartition> retrievedLeaderTopicPartitions = retrieveLeaderTopicPartitions(topicRequestTimeoutMs);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("retrievedLeaderTopicPartitions: {}", retrievedLeaderTopicPartitions);
                            LOG.debug("currentLeaderTopicPartitions: {}", getCurrentLeaderTopicPartitions());
                        }
                        boolean requestTaskReconfiguration = false;
                        if (reconfigureTasksOnLeaderChange) {
                            if (!retrievedLeaderTopicPartitions.equals(getCurrentLeaderTopicPartitions())) {
                                LOG.info("Retrieved leaders and topic partitions do not match currently stored leaders and topic partitions, will request task reconfiguration");
                                requestTaskReconfiguration = true;
                            }
                        } else {
                            Set<TopicPartition> retrievedTopicPartitions = retrievedLeaderTopicPartitions.stream()
                                    .map(LeaderTopicPartition::toTopicPartition)
                                    .collect(Collectors.toSet());
                            if (LOG.isDebugEnabled())
                                LOG.debug("retrievedTopicPartitions: {}", retrievedTopicPartitions);
                            Set<TopicPartition> currentTopicPartitions = getCurrentLeaderTopicPartitions().stream()
                                    .map(LeaderTopicPartition::toTopicPartition)
                                    .collect(Collectors.toSet());
                            if (LOG.isDebugEnabled())
                                LOG.debug("currentTopicPartitions: {}", currentTopicPartitions);
                            if (!retrievedTopicPartitions.equals(currentTopicPartitions)) {
                                LOG.info("Retrieved topic partitions do not match currently stored topic partitions, will request task reconfiguration");
                                requestTaskReconfiguration = true;
                            }
                        }
                        setCurrentLeaderTopicPartitions(retrievedLeaderTopicPartitions);
                        if (requestTaskReconfiguration)
                            connectorContext.requestTaskReconfiguration();
                        else
                            LOG.info("No partition changes which require reconfiguration have been detected.");
                    } catch (TimeoutException e) {
                        LOG.error("Timeout while waiting for AdminClient to return topic list. This indicates a (possibly transient) connection issue, or is an indicator that the timeout is set too low. {}", e);
                    } catch (ExecutionException e) {
                        LOG.error("Unexpected ExecutionException. {}", e);
                    } catch (InterruptedException e) {
                        LOG.error("InterruptedException. Probably shutting down. {}, e");
                    }
                }
            }
        };
    }

    public void start() {
        // On start, block until we retrieve the initial list of topic partitions (or at least until timeout)
        try {
            // This will block while waiting to retrieve data form kafka. Timeout is set so that we don't hang the kafka connect herder if an invalid configuration causes us to retry infinitely.
            LOG.info("Retrieving initial topic list from kafka.");
            setCurrentLeaderTopicPartitions(retrieveLeaderTopicPartitions(topicRequestTimeoutMs));
        } catch (TimeoutException e) {
            LOG.error("Timeout while waiting for AdminClient to return topic list. This likely indicates a (possibly transient) connection issue, but could be an indicator that the timeout is set too low. {}", e);
            throw new ConnectException("Timeout while waiting for AdminClient to return topic list. This likely indicates a (possibly transient) connection issue, but could be an indicator that the timeout is set too low.");
        } catch (ExecutionException e) {
            LOG.error("Unexpected ExecutionException. {}", e);
            throw new ConnectException("Unexpected  while starting PartitionMonitor.");
        } catch (InterruptedException e) {
            LOG.error("InterruptedException. {}, e");
            throw new ConnectException("Unexpected InterruptedException while starting PartitionMonitor.");
        }
        // Schedule a task to periodically run to poll for new data
        pollExecutorService = Executors.newSingleThreadScheduledExecutor();
        pollHandle = pollExecutorService.scheduleWithFixedDelay(pollThread, topicPollIntervalMs, topicPollIntervalMs, TimeUnit.MILLISECONDS);
    }

    private boolean matchedTopicFilter(String topic) {
        return topicWhitelistPattern.matcher(topic).matches();
    }

    private synchronized void setCurrentLeaderTopicPartitions(Set<LeaderTopicPartition> leaderTopicPartitions) {
        currentLeaderTopicPartitions = leaderTopicPartitions;
    }

    public synchronized Set<LeaderTopicPartition> getCurrentLeaderTopicPartitions() {
        return currentLeaderTopicPartitions;
    }

    // Allow the main thread a chance to shut down gracefully
    public void shutdown() {
        LOG.info("Shutdown called.");
        long startWait = System.currentTimeMillis();
        shutdown.set(true);
        partitionMonitorClient.close(maxShutdownWaitMs - (System.currentTimeMillis() - startWait), TimeUnit.MILLISECONDS);
        // Cancel our scheduled task, but wait for an existing task to complete if running
        pollHandle.cancel(false);
        // Ask nicely to shut down the partition monitor executor service if it hasn't already
        if (!pollExecutorService.isShutdown()) {
            try {
                pollExecutorService.awaitTermination(maxShutdownWaitMs - (System.currentTimeMillis() - startWait), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("Got InterruptedException while waiting for pollExecutorService to shutdown, shutdown will be forced.");
            }
        }
        if (!pollExecutorService.isShutdown()) {
            pollExecutorService.shutdownNow();
        }
        LOG.info("Shutdown Complete.");
    }


    // Retrieve a list of LeaderTopicPartitions that match our topic filter
    private synchronized Set<LeaderTopicPartition> retrieveLeaderTopicPartitions(int requestTimeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
        long startWait = System.currentTimeMillis();

        ListTopicsOptions listTopicsOptions = new ListTopicsOptions().listInternal(false).timeoutMs((int) (requestTimeoutMs - (System.currentTimeMillis() - startWait)));
        Set<String> retrievedTopicSet = partitionMonitorClient.listTopics(listTopicsOptions).names().get(requestTimeoutMs - (System.currentTimeMillis() - startWait), TimeUnit.MILLISECONDS);
        LOG.debug("Server topic list: {}", retrievedTopicSet);
        Set<String> matchedTopicSet = retrievedTopicSet.stream()
            .filter(topic -> matchedTopicFilter(topic))
            .collect(Collectors.toSet());
        LOG.debug("Matched topic list: {}", matchedTopicSet);

        DescribeTopicsOptions describeTopicsOptions = new DescribeTopicsOptions().timeoutMs((int) (requestTimeoutMs - (System.currentTimeMillis() - startWait)));
        Map<String, TopicDescription> retrievedTopicDescriptions = partitionMonitorClient.describeTopics(matchedTopicSet, describeTopicsOptions).all().get(requestTimeoutMs - (System.currentTimeMillis() - startWait), TimeUnit.MILLISECONDS);
        return retrievedTopicDescriptions.values().stream()
            .map(topicDescription ->
                topicDescription.partitions().stream()
                .map(partitionInfo -> new LeaderTopicPartition(partitionInfo.leader().id(), topicDescription.name(), partitionInfo.partition()))
            )
            .flatMap(Function.identity())
            .collect(Collectors.toSet());
    }

}
