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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.internals.QuietStreamsConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;

public final class AssignorConfiguration {
    public static final String HIGH_AVAILABILITY_ENABLED_CONFIG = "internal.high.availability.enabled";
    private final boolean highAvailabilityEnabled;

    private final String logPrefix;
    private final Logger log;
    private final AssignmentConfigs assignmentConfigs;
    @SuppressWarnings("deprecation")
    private final org.apache.kafka.streams.processor.PartitionGrouper partitionGrouper;
    private final String userEndPoint;
    private final TaskManager taskManager;
    private final StreamsMetadataState streamsMetadataState;
    private final Admin adminClient;
    private final int adminClientTimeout;
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

        assignmentConfigs = new AssignmentConfigs(streamsConfig);

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

        {
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
        }

        {
            final Object o = configs.get(StreamsConfig.InternalConfig.STREAMS_METADATA_STATE_FOR_PARTITION_ASSIGNOR);
            if (o == null) {
                final KafkaException fatalException = new KafkaException("StreamsMetadataState is not specified");
                log.error(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            if (!(o instanceof StreamsMetadataState)) {
                final KafkaException fatalException = new KafkaException(
                    String.format("%s is not an instance of %s", o.getClass().getName(), StreamsMetadataState.class.getName())
                );
                log.error(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            streamsMetadataState = (StreamsMetadataState) o;
        }

        {
            final Object o = configs.get(StreamsConfig.InternalConfig.STREAMS_ADMIN_CLIENT);
            if (o == null) {
                final KafkaException fatalException = new KafkaException("Admin is not specified");
                log.error(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            if (!(o instanceof Admin)) {
                final KafkaException fatalException = new KafkaException(
                    String.format("%s is not an instance of %s", o.getClass().getName(), Admin.class.getName())
                );
                log.error(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            adminClient = (Admin) o;
            internalTopicManager = new InternalTopicManager(adminClient, streamsConfig);
        }

        adminClientTimeout = streamsConfig.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);

        copartitionedTopicsEnforcer = new CopartitionedTopicsEnforcer(logPrefix);

        {
            final Object o = configs.get(HIGH_AVAILABILITY_ENABLED_CONFIG);
            if (o == null) {
                highAvailabilityEnabled = false;
            } else {
                highAvailabilityEnabled = (Boolean) o;
            }
        }
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

    public AtomicLong getNextProbingRebalanceMs(final Map<String, ?> configs) {
        final Object al = configs.get(InternalConfig.NEXT_PROBING_REBALANCE_MS);
        if (al == null) {
            final KafkaException fatalException = new KafkaException("nextProbingRebalanceMs is not specified");
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        if (!(al instanceof AtomicLong)) {
            final KafkaException fatalException = new KafkaException(
                String.format("%s is not an instance of %s", al.getClass().getName(), AtomicLong.class.getName())
            );
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        return (AtomicLong) al;
    }

    public Time getTime(final Map<String, ?> configs) {
        final Object t = configs.get(InternalConfig.TIME);
        if (t == null) {
            final KafkaException fatalException = new KafkaException("time is not specified");
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        if (!(t instanceof Time)) {
            final KafkaException fatalException = new KafkaException(
                String.format("%s is not an instance of %s", t.getClass().getName(), Time.class.getName())
            );
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        return (Time) t;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    public StreamsMetadataState getStreamsMetadataState() {
        return streamsMetadataState;
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
                    log.info("Eager rebalancing enabled now for upgrade from {}.x", upgradeFrom);
                    return RebalanceProtocol.EAGER;
                default:
                    throw new IllegalArgumentException("Unknown configuration value for parameter 'upgrade.from': " + upgradeFrom);
            }
        }
        log.info("Cooperative rebalancing enabled now");
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
                    return 1;
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
                    return 2;
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

    @SuppressWarnings("deprecation")
    public org.apache.kafka.streams.processor.PartitionGrouper getPartitionGrouper() {
        return partitionGrouper;
    }

    public String getUserEndPoint() {
        return userEndPoint;
    }

    public Admin getAdminClient() {
        return adminClient;
    }

    public int getAdminClientTimeout() {
        return adminClientTimeout;
    }

    public InternalTopicManager getInternalTopicManager() {
        return internalTopicManager;
    }

    public CopartitionedTopicsEnforcer getCopartitionedTopicsEnforcer() {
        return copartitionedTopicsEnforcer;
    }

    public AssignmentConfigs getAssignmentConfigs() {
        return assignmentConfigs;
    }

    public boolean isHighAvailabilityEnabled() {
        return highAvailabilityEnabled;
    }

    public static class AssignmentConfigs {
        public final long acceptableRecoveryLag;
        public final int balanceFactor;
        public final int maxWarmupReplicas;
        public final int numStandbyReplicas;
        public final long probingRebalanceIntervalMs;

        private AssignmentConfigs(final StreamsConfig configs) {
            this(
                configs.getLong(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG),
                configs.getInt(StreamsConfig.BALANCE_FACTOR_CONFIG),
                configs.getInt(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG),
                configs.getInt(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG),
                configs.getLong(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG)
            );
        }

        AssignmentConfigs(final Long acceptableRecoveryLag,
                          final Integer balanceFactor,
                          final Integer maxWarmupReplicas,
                          final Integer numStandbyReplicas,
                          final Long probingRebalanceIntervalMs) {
            this.acceptableRecoveryLag = acceptableRecoveryLag;
            this.balanceFactor = balanceFactor;
            this.maxWarmupReplicas = maxWarmupReplicas;
            this.numStandbyReplicas = numStandbyReplicas;
            this.probingRebalanceIntervalMs = probingRebalanceIntervalMs;
        }
    }
}
