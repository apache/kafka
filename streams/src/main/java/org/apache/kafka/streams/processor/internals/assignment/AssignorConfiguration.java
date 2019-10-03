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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.internals.QuietStreamsConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_ONE;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_TWO;

public final class AssignorConfiguration {
    private final String logPrefix;
    private final Logger log;
    private final Integer numStandbyReplicas;
    @SuppressWarnings("deprecation")
    private final org.apache.kafka.streams.processor.PartitionGrouper partitionGrouper;
    private final String userEndPoint;
    private final TaskManager taskManager;
    private final InternalTopicManager internalTopicManager;
    private final CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    private final StreamsConfig streamsConfig;

    @SuppressWarnings("deprecation")
    public AssignorConfiguration(final Map<String, ?> configs) {
        streamsConfig = new QuietStreamsConfig(configs);

        // Setting the logger with the passed in client thread name
        logPrefix = String.format("stream-thread [%s] ", streamsConfig.getString(CommonClientConfigs.CLIENT_ID_CONFIG));
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        numStandbyReplicas = streamsConfig.getInt(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);

        partitionGrouper = streamsConfig.getConfiguredInstance(
            StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG,
            org.apache.kafka.streams.processor.PartitionGrouper.class
        );

        final String configuredUserEndpoint = streamsConfig.getString(StreamsConfig.APPLICATION_SERVER_CONFIG);
        if (configuredUserEndpoint != null && !configuredUserEndpoint.isEmpty()) {
            try {
                final String host = getHost(configuredUserEndpoint);
                final Integer port = getPort(configuredUserEndpoint);

                if (host == null || port == null) {
                    throw new ConfigException(
                        String.format(
                            "%s Config %s isn't in the correct format. Expected a host:port pair but received %s",
                            logPrefix, StreamsConfig.APPLICATION_SERVER_CONFIG, configuredUserEndpoint
                        )
                    );
                }
            } catch (final NumberFormatException nfe) {
                throw new ConfigException(
                    String.format("%s Invalid port supplied in %s for config %s: %s",
                                  logPrefix, configuredUserEndpoint, StreamsConfig.APPLICATION_SERVER_CONFIG, nfe)
                );
            }
            userEndPoint = configuredUserEndpoint;
        } else {
            userEndPoint = null;
        }

        final Object o = configs.get(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR);
        if (o == null) {
            final KafkaException fatalException = new KafkaException("TaskManager is not specified");
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        if (!(o instanceof TaskManager)) {
            final KafkaException fatalException = new KafkaException(
                String.format("%s is not an instance of %s", o.getClass().getName(), TaskManager.class.getName())
            );
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        taskManager = (TaskManager) o;

        internalTopicManager = new InternalTopicManager(taskManager.adminClient(), streamsConfig);

        copartitionedTopicsEnforcer = new CopartitionedTopicsEnforcer(logPrefix);
    }

    public AtomicInteger getAssignmentErrorCode(final Map<String, ?> configs) {
        final Object ai = configs.get(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE);
        if (ai == null) {
            final KafkaException fatalException = new KafkaException("assignmentErrorCode is not specified");
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        if (!(ai instanceof AtomicInteger)) {
            final KafkaException fatalException = new KafkaException(
                String.format("%s is not an instance of %s", ai.getClass().getName(), AtomicInteger.class.getName())
            );
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }
        return (AtomicInteger) ai;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    public RebalanceProtocol rebalanceProtocol() {
        final String upgradeFrom = streamsConfig.getString(StreamsConfig.UPGRADE_FROM_CONFIG);
        if (upgradeFrom != null) {
            switch (upgradeFrom) {
                case StreamsConfig.UPGRADE_FROM_0100:
                case StreamsConfig.UPGRADE_FROM_0101:
                case StreamsConfig.UPGRADE_FROM_0102:
                case StreamsConfig.UPGRADE_FROM_0110:
                case StreamsConfig.UPGRADE_FROM_10:
                case StreamsConfig.UPGRADE_FROM_11:
                case StreamsConfig.UPGRADE_FROM_20:
                case StreamsConfig.UPGRADE_FROM_21:
                case StreamsConfig.UPGRADE_FROM_22:
                case StreamsConfig.UPGRADE_FROM_23:
                    log.info("Turning off cooperative rebalancing for upgrade from {}.x", upgradeFrom);
                    return RebalanceProtocol.EAGER;
                default:
                    throw new IllegalArgumentException("Unknown configuration value for parameter 'upgrade.from': " + upgradeFrom);
            }
        }

        return RebalanceProtocol.COOPERATIVE;
    }

    public String logPrefix() {
        return logPrefix;
    }

    public int configuredMetadataVersion(final int priorVersion) {
        final String upgradeFrom = streamsConfig.getString(StreamsConfig.UPGRADE_FROM_CONFIG);
        if (upgradeFrom != null) {
            switch (upgradeFrom) {
                case StreamsConfig.UPGRADE_FROM_0100:
                    log.info(
                        "Downgrading metadata version from {} to 1 for upgrade from 0.10.0.x.",
                        LATEST_SUPPORTED_VERSION
                    );
                    return VERSION_ONE;
                case StreamsConfig.UPGRADE_FROM_0101:
                case StreamsConfig.UPGRADE_FROM_0102:
                case StreamsConfig.UPGRADE_FROM_0110:
                case StreamsConfig.UPGRADE_FROM_10:
                case StreamsConfig.UPGRADE_FROM_11:
                    log.info(
                        "Downgrading metadata version from {} to 2 for upgrade from {}.x.",
                        LATEST_SUPPORTED_VERSION,
                        upgradeFrom
                    );
                    return VERSION_TWO;
                case StreamsConfig.UPGRADE_FROM_20:
                case StreamsConfig.UPGRADE_FROM_21:
                case StreamsConfig.UPGRADE_FROM_22:
                case StreamsConfig.UPGRADE_FROM_23:
                    // These configs are for cooperative rebalancing and should not affect the metadata version
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Unknown configuration value for parameter 'upgrade.from': " + upgradeFrom
                    );
            }
        }
        return priorVersion;
    }

    public int getNumStandbyReplicas() {
        return numStandbyReplicas;
    }

    @SuppressWarnings("deprecation")
    public org.apache.kafka.streams.processor.PartitionGrouper getPartitionGrouper() {
        return partitionGrouper;
    }

    public String getUserEndPoint() {
        return userEndPoint;
    }

    public InternalTopicManager getInternalTopicManager() {
        return internalTopicManager;
    }

    public CopartitionedTopicsEnforcer getCopartitionedTopicsEnforcer() {
        return copartitionedTopicsEnforcer;
    }
}
