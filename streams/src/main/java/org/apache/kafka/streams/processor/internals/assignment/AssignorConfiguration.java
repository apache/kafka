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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.internals.UpgradeFromValues;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.internals.ClientUtils;
import org.apache.kafka.streams.processor.internals.InternalTopicManager;

import org.slf4j.Logger;

import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;

public final class AssignorConfiguration {
    private final String internalTaskAssignorClass;

    private final String logPrefix;
    private final Logger log;
    private final ReferenceContainer referenceContainer;

    private final StreamsConfig streamsConfig;
    private final Map<String, ?> internalConfigs;

    public AssignorConfiguration(final Map<String, ?> configs) {
        // NOTE: If you add a new config to pass through to here, be sure to test it in a real
        // application. Since we filter out some configurations, we may have to explicitly copy
        // them over when we construct the Consumer.
        streamsConfig = new ClientUtils.QuietStreamsConfig(configs);
        internalConfigs = configs;

        // Setting the logger with the passed in client thread name
        logPrefix = String.format("stream-thread [%s] ", streamsConfig.getString(CommonClientConfigs.CLIENT_ID_CONFIG));
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        {
            final Object o = configs.get(InternalConfig.REFERENCE_CONTAINER_PARTITION_ASSIGNOR);
            if (o == null) {
                final KafkaException fatalException = new KafkaException("ReferenceContainer is not specified");
                log.error(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            if (!(o instanceof ReferenceContainer)) {
                final KafkaException fatalException = new KafkaException(
                    String.format("%s is not an instance of %s", o.getClass().getName(), ReferenceContainer.class.getName())
                );
                log.error(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            referenceContainer = (ReferenceContainer) o;
        }

        {
            final String o = (String) configs.get(INTERNAL_TASK_ASSIGNOR_CLASS);
            if (o == null) {
                internalTaskAssignorClass = HighAvailabilityTaskAssignor.class.getName();
            } else {
                internalTaskAssignorClass = o;
            }
        }
    }

    public ReferenceContainer referenceContainer() {
        return referenceContainer;
    }

    public RebalanceProtocol rebalanceProtocol() {
        final String upgradeFrom = streamsConfig.getString(StreamsConfig.UPGRADE_FROM_CONFIG);
        if (upgradeFrom != null) {
            switch (UpgradeFromValues.fromString(upgradeFrom)) {
                case UPGRADE_FROM_0100:
                case UPGRADE_FROM_0101:
                case UPGRADE_FROM_0102:
                case UPGRADE_FROM_0110:
                case UPGRADE_FROM_10:
                case UPGRADE_FROM_11:
                case UPGRADE_FROM_20:
                case UPGRADE_FROM_21:
                case UPGRADE_FROM_22:
                case UPGRADE_FROM_23:
                    // ATTENTION: The following log messages is used for verification in system test
                    // streams/streams_cooperative_rebalance_upgrade_test.py::StreamsCooperativeRebalanceUpgradeTest.test_upgrade_to_cooperative_rebalance
                    // If you change it, please do also change the system test accordingly and
                    // verify whether the test passes.
                    log.info("Eager rebalancing protocol is enabled now for upgrade from {}.x", upgradeFrom);
                    log.warn("The eager rebalancing protocol is deprecated and will stop being supported in a future release." +
                        " Please be prepared to remove the 'upgrade.from' config soon.");
                    return RebalanceProtocol.EAGER;
                case UPGRADE_FROM_24:
                case UPGRADE_FROM_25:
                case UPGRADE_FROM_26:
                case UPGRADE_FROM_27:
                case UPGRADE_FROM_28:
                case UPGRADE_FROM_30:
                case UPGRADE_FROM_31:
                case UPGRADE_FROM_32:
                case UPGRADE_FROM_33:
                case UPGRADE_FROM_34:
                case UPGRADE_FROM_35:
                case UPGRADE_FROM_36:
                case UPGRADE_FROM_37:
                case UPGRADE_FROM_38:
                case UPGRADE_FROM_39:
                    // we need to add new version when new "upgrade.from" values become available

                    // This config is for explicitly sending FK response to a requested partition
                    // and should not affect the rebalance protocol
                    break;
                default:
                    throw new IllegalArgumentException("Unknown configuration value for parameter 'upgrade.from': " + upgradeFrom);
            }
        }
        // ATTENTION: The following log messages is used for verification in system test
        // streams/streams_cooperative_rebalance_upgrade_test.py::StreamsCooperativeRebalanceUpgradeTest.test_upgrade_to_cooperative_rebalance
        // If you change it, please do also change the system test accordingly and
        // verify whether the test passes.
        log.info("Cooperative rebalancing protocol is enabled now");
        return RebalanceProtocol.COOPERATIVE;
    }

    public String logPrefix() {
        return logPrefix;
    }

    public int configuredMetadataVersion(final int priorVersion) {
        final String upgradeFrom = streamsConfig.getString(StreamsConfig.UPGRADE_FROM_CONFIG);
        if (upgradeFrom != null) {
            switch (UpgradeFromValues.fromString(upgradeFrom)) {
                case UPGRADE_FROM_0100:
                    log.info(
                        "Downgrading metadata.version from {} to 1 for upgrade from 0.10.0.x.",
                        LATEST_SUPPORTED_VERSION
                    );
                    return 1;
                case UPGRADE_FROM_0101:
                case UPGRADE_FROM_0102:
                case UPGRADE_FROM_0110:
                case UPGRADE_FROM_10:
                case UPGRADE_FROM_11:
                    log.info(
                        "Downgrading metadata.version from {} to 2 for upgrade from {}.x.",
                        LATEST_SUPPORTED_VERSION,
                        upgradeFrom
                    );
                    return 2;
                case UPGRADE_FROM_20:
                case UPGRADE_FROM_21:
                case UPGRADE_FROM_22:
                case UPGRADE_FROM_23:
                    // These configs are for cooperative rebalancing and should not affect the metadata version
                    break;
                case UPGRADE_FROM_24:
                case UPGRADE_FROM_25:
                case UPGRADE_FROM_26:
                case UPGRADE_FROM_27:
                case UPGRADE_FROM_28:
                case UPGRADE_FROM_30:
                case UPGRADE_FROM_31:
                case UPGRADE_FROM_32:
                case UPGRADE_FROM_33:
                case UPGRADE_FROM_34:
                case UPGRADE_FROM_35:
                case UPGRADE_FROM_36:
                case UPGRADE_FROM_37:
                case UPGRADE_FROM_38:
                case UPGRADE_FROM_39:
                    // we need to add new version when new "upgrade.from" values become available

                    // This config is for explicitly sending FK response to a requested partition
                    // and should not affect the metadata version
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Unknown configuration value for parameter 'upgrade.from': " + upgradeFrom
                    );
            }
        }
        return priorVersion;
    }

    public String userEndPoint() {
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
            return configuredUserEndpoint;
        } else {
            return null;
        }
    }

    public InternalTopicManager internalTopicManager() {
        return new InternalTopicManager(referenceContainer.time, referenceContainer.adminClient, streamsConfig);
    }

    public CopartitionedTopicsEnforcer copartitionedTopicsEnforcer() {
        return new CopartitionedTopicsEnforcer(logPrefix);
    }

    public AssignmentConfigs assignmentConfigs() {
        return AssignmentConfigs.of(streamsConfig);
    }

    public LegacyTaskAssignor taskAssignor() {
        try {
            return Utils.newInstance(internalTaskAssignorClass, LegacyTaskAssignor.class);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(
                "Expected an instantiable class name for " + INTERNAL_TASK_ASSIGNOR_CLASS,
                e
            );
        }
    }

    public Optional<org.apache.kafka.streams.processor.assignment.TaskAssignor> customTaskAssignor() {
        final String userTaskAssignorClassname = streamsConfig.getString(StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG);
        if (userTaskAssignorClassname == null) {
            log.info("No custom task assignors found, defaulting to internal task assignment with {}", INTERNAL_TASK_ASSIGNOR_CLASS);
            return Optional.empty();
        }
        try {
            final org.apache.kafka.streams.processor.assignment.TaskAssignor assignor = Utils.newInstance(userTaskAssignorClassname,
                org.apache.kafka.streams.processor.assignment.TaskAssignor.class);
            log.info("Instantiated {} as the task assignor.", userTaskAssignorClassname);
            assignor.configure(streamsConfig.originals());
            return Optional.of(assignor);
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException(
                "Expected an instantiable class name for " + StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG + " but got " + userTaskAssignorClassname,
                e
            );
        }
    }

    public AssignmentListener assignmentListener() {
        final Object o = internalConfigs.get(InternalConfig.ASSIGNMENT_LISTENER);
        if (o == null) {
            return stable -> { };
        }

        if (!(o instanceof AssignmentListener)) {
            final KafkaException fatalException = new KafkaException(
                String.format("%s is not an instance of %s", o.getClass().getName(), AssignmentListener.class.getName())
            );
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        return (AssignmentListener) o;
    }

    public interface AssignmentListener {
        void onAssignmentComplete(final boolean stable);
    }
}
