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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.LogLevelConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.logger.LoggingController;

import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = {Type.KRAFT})
public class AdminLoggerConfigTest {

    private static final String CONTROLLER_SERVER_LOGGER = "kafka.server.ControllerServer";
    private static final String LOG_CLEANER_LOGGER = "kafka.log.LogCleaner";
    private static final String REPLICA_MANAGER_LOGGER = "kafka.server.ReplicaManager";
    private static final String REQUEST_HANDLER_LOGGER = "kafka.server.KafkaRequestHandler";
    private static final String SOCKET_SERVER_LOGGER = "kafka.network.SocketServer";

    private final ClusterInstance clusterInstance;
    private ConfigResource brokerLoggerResource;

    AdminLoggerConfigTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setUp() {
        // The brokers run in this JVM, so logger levels altered by one test would leak into the next one.
        Configurator.reconfigure();
        brokerLoggerResource = new ConfigResource(
            ConfigResource.Type.BROKER_LOGGER,
            String.valueOf(clusterInstance.brokerIds().iterator().next()));
    }

    @ClusterTest
    public void testDescribeConfigsForLog4jLogLevels() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            LoggerFactory.getLogger("kafka.cluster.Replica").trace("Message to create the logger");
            Config loggerConfig = describeBrokerLoggers(admin);
            String kafkaLogLevel = loggerConfig.get("kafka").value();
            ConfigEntry clusterReplicaLogLevel = loggerConfig.get("kafka.cluster.Replica");
            // we expect the log level to be inherited from the first ancestor with a level configured
            assertEquals(kafkaLogLevel, clusterReplicaLogLevel.value());
            assertEquals("kafka.cluster.Replica", clusterReplicaLogLevel.name());
            assertEquals(ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG, clusterReplicaLogLevel.source());
            assertFalse(clusterReplicaLogLevel.isReadOnly());
            assertFalse(clusterReplicaLogLevel.isSensitive());
            assertTrue(clusterReplicaLogLevel.synonyms().isEmpty());
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevels() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String ancestorLogger = "kafka";
            Config initialLoggerConfig = describeBrokerLoggers(admin);
            String initialAncestorLogLevel = initialLoggerConfig.get(ancestorLogger).value();
            String initialControllerServerLogLevel = initialLoggerConfig.get(CONTROLLER_SERVER_LOGGER).value();
            String initialLogCleanerLogLevel = initialLoggerConfig.get(LOG_CLEANER_LOGGER).value();
            String initialReplicaManagerLogLevel = initialLoggerConfig.get(REPLICA_MANAGER_LOGGER).value();

            String newAncestorLogLevel = LogLevelConfig.DEBUG_LOG_LEVEL;
            List<AlterConfigOp> alterAncestorLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(ancestorLogger, newAncestorLogLevel), AlterConfigOp.OpType.SET));
            // Test validateOnly does not change anything
            alterBrokerLoggers(admin, alterAncestorLoggerEntry, true);
            Config validatedLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialAncestorLogLevel, validatedLoggerConfig.get(ancestorLogger).value());
            assertEquals(initialControllerServerLogLevel, validatedLoggerConfig.get(CONTROLLER_SERVER_LOGGER).value());
            assertEquals(initialLogCleanerLogLevel, validatedLoggerConfig.get(LOG_CLEANER_LOGGER).value());
            assertEquals(initialReplicaManagerLogLevel, validatedLoggerConfig.get(REPLICA_MANAGER_LOGGER).value());

            // test that we can change them and unset loggers still use the root's log level
            alterBrokerLoggers(admin, alterAncestorLoggerEntry, false);
            Config changedAncestorLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get(ancestorLogger).value());
            assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get(CONTROLLER_SERVER_LOGGER).value());
            assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get(LOG_CLEANER_LOGGER).value());
            assertEquals(newAncestorLogLevel, changedAncestorLoggerConfig.get(REPLICA_MANAGER_LOGGER).value());

            // alter the LogCleaner's logger so we can later test resetting it
            List<AlterConfigOp> alterLogCleanerLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(LOG_CLEANER_LOGGER, LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, alterLogCleanerLoggerEntry, false);
            Config changedBrokerLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(LogLevelConfig.ERROR_LOG_LEVEL, changedBrokerLoggerConfig.get(LOG_CLEANER_LOGGER).value());

            // properly test various set operations and one delete
            List<AlterConfigOp> alterLogLevelsEntries = List.of(
                new AlterConfigOp(new ConfigEntry(CONTROLLER_SERVER_LOGGER, LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(LOG_CLEANER_LOGGER, LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(REPLICA_MANAGER_LOGGER, LogLevelConfig.TRACE_LOG_LEVEL), AlterConfigOp.OpType.SET)
            );
            alterBrokerLoggers(admin, alterLogLevelsEntries, false);
            Config alteredLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(newAncestorLogLevel, alteredLoggerConfig.get(ancestorLogger).value());
            assertEquals(LogLevelConfig.INFO_LOG_LEVEL, alteredLoggerConfig.get(CONTROLLER_SERVER_LOGGER).value());
            assertEquals(LogLevelConfig.ERROR_LOG_LEVEL, alteredLoggerConfig.get(LOG_CLEANER_LOGGER).value());
            assertEquals(LogLevelConfig.TRACE_LOG_LEVEL, alteredLoggerConfig.get(REPLICA_MANAGER_LOGGER).value());
        }
    }

    /**
     * 1. Assume kafka logger == TRACE
     * 2. Change kafka.server.ControllerServer logger to INFO
     * 3. Unset kafka.server.ControllerServer via AlterConfigOp.OpType.DELETE (resets it to the kafka logger - TRACE)
     * 4. Change kafka logger to ERROR
     * 5. Ensure the kafka.server.ControllerServer logger's level is ERROR (the current kafka logger level)
     */
    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevelsCanResetLoggerToCurrentRoot() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String ancestorLogger = "kafka";
            // step 1 - configure kafka logger
            String initialAncestorLogLevel = LogLevelConfig.TRACE_LOG_LEVEL;
            List<AlterConfigOp> alterAncestorLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(ancestorLogger, initialAncestorLogLevel), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, alterAncestorLoggerEntry, false);
            Config initialLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialAncestorLogLevel, initialLoggerConfig.get(ancestorLogger).value());
            assertEquals(initialAncestorLogLevel, initialLoggerConfig.get(CONTROLLER_SERVER_LOGGER).value());

            // step 2 - change ControllerServer logger to INFO
            List<AlterConfigOp> alterControllerLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(CONTROLLER_SERVER_LOGGER, LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, alterControllerLoggerEntry, false);
            Config changedControllerLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialAncestorLogLevel, changedControllerLoggerConfig.get(ancestorLogger).value());
            assertEquals(LogLevelConfig.INFO_LOG_LEVEL, changedControllerLoggerConfig.get(CONTROLLER_SERVER_LOGGER).value());

            // step 3 - unset ControllerServer logger
            List<AlterConfigOp> deleteControllerLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(CONTROLLER_SERVER_LOGGER, ""), AlterConfigOp.OpType.DELETE));
            alterBrokerLoggers(admin, deleteControllerLoggerEntry, false);
            Config deletedControllerLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialAncestorLogLevel, deletedControllerLoggerConfig.get(ancestorLogger).value());
            assertEquals(initialAncestorLogLevel, deletedControllerLoggerConfig.get(CONTROLLER_SERVER_LOGGER).value());

            // step 4 - change kafka logger to ERROR
            String newAncestorLogLevel = LogLevelConfig.ERROR_LOG_LEVEL;
            List<AlterConfigOp> newAlterAncestorLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(ancestorLogger, newAncestorLogLevel), AlterConfigOp.OpType.SET));
            alterBrokerLoggers(admin, newAlterAncestorLoggerEntry, false);
            Config newAncestorLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(newAncestorLogLevel, newAncestorLoggerConfig.get(ancestorLogger).value());
            assertEquals(newAncestorLogLevel, newAncestorLoggerConfig.get(CONTROLLER_SERVER_LOGGER).value());
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevelsCanSetToRootLogger() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            Config initialLoggerConfig = describeBrokerLoggers(admin);
            String initialRootLogLevel = initialLoggerConfig.get(LoggingController.ROOT_LOGGER).value();
            String newRootLogLevel = LogLevelConfig.DEBUG_LOG_LEVEL;

            List<AlterConfigOp> alterRootLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(LoggingController.ROOT_LOGGER, newRootLogLevel), AlterConfigOp.OpType.SET));

            alterBrokerLoggers(admin, alterRootLoggerEntry, true);
            Config validatedRootLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(initialRootLogLevel, validatedRootLoggerConfig.get(LoggingController.ROOT_LOGGER).value());

            alterBrokerLoggers(admin, alterRootLoggerEntry, false);
            Config changedRootLoggerConfig = describeBrokerLoggers(admin);
            assertEquals(newRootLogLevel, changedRootLoggerConfig.get(LoggingController.ROOT_LOGGER).value());
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevelsCannotResetRootLogger() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            List<AlterConfigOp> deleteRootLoggerEntry = List.of(
                new AlterConfigOp(new ConfigEntry(LoggingController.ROOT_LOGGER, ""), AlterConfigOp.OpType.DELETE));

            assertFutureThrows(InvalidRequestException.class, incrementalAlterBrokerLoggers(admin, deleteRootLoggerEntry, false));
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsForLog4jLogLevelsDoesNotWorkWithInvalidConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            ConfigEntry expectedValidLoggerLogLevel = describeBrokerLoggers(admin).get(REQUEST_HANDLER_LOGGER);

            List<AlterConfigOp> appendLogLevelEntries = List.of(
                new AlterConfigOp(new ConfigEntry(REQUEST_HANDLER_LOGGER, LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
                new AlterConfigOp(new ConfigEntry(SOCKET_SERVER_LOGGER, LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.APPEND) // append is not supported
            );
            assertFutureThrows(InvalidRequestException.class, incrementalAlterBrokerLoggers(admin, appendLogLevelEntries, false));
            assertLogLevelDidNotChange(admin, expectedValidLoggerLogLevel);

            List<AlterConfigOp> subtractLogLevelEntries = List.of(
                new AlterConfigOp(new ConfigEntry(REQUEST_HANDLER_LOGGER, LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
                new AlterConfigOp(new ConfigEntry(SOCKET_SERVER_LOGGER, LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SUBTRACT) // subtract is not supported
            );
            assertFutureThrows(InvalidRequestException.class, incrementalAlterBrokerLoggers(admin, subtractLogLevelEntries, false));
            assertLogLevelDidNotChange(admin, expectedValidLoggerLogLevel);

            List<AlterConfigOp> invalidLogLevelLogLevelEntries = List.of(
                new AlterConfigOp(new ConfigEntry(REQUEST_HANDLER_LOGGER, LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
                new AlterConfigOp(new ConfigEntry(SOCKET_SERVER_LOGGER, "OFF"), AlterConfigOp.OpType.SET) // OFF is not a valid log level
            );
            assertFutureThrows(InvalidConfigurationException.class, incrementalAlterBrokerLoggers(admin, invalidLogLevelLogLevelEntries, false));
            assertLogLevelDidNotChange(admin, expectedValidLoggerLogLevel);

            List<AlterConfigOp> invalidLoggerNameLogLevelEntries = List.of(
                new AlterConfigOp(new ConfigEntry(REQUEST_HANDLER_LOGGER, LogLevelConfig.INFO_LOG_LEVEL), AlterConfigOp.OpType.SET), // valid
                new AlterConfigOp(new ConfigEntry("Some Other LogCleaner", LogLevelConfig.ERROR_LOG_LEVEL), AlterConfigOp.OpType.SET) // invalid logger name is not supported
            );
            assertFutureThrows(InvalidConfigurationException.class, incrementalAlterBrokerLoggers(admin, invalidLoggerNameLogLevelEntries, false));
            assertLogLevelDidNotChange(admin, expectedValidLoggerLogLevel);
        }
    }

    private void assertLogLevelDidNotChange(Admin admin, ConfigEntry expectedValidLoggerLogLevel) throws Exception {
        assertEquals(expectedValidLoggerLogLevel, describeBrokerLoggers(admin).get(REQUEST_HANDLER_LOGGER));
    }

    private KafkaFuture<Void> incrementalAlterBrokerLoggers(
        Admin admin,
        Collection<AlterConfigOp> entries,
        boolean validateOnly
    ) {
        return admin.incrementalAlterConfigs(
            Map.of(brokerLoggerResource, entries),
            new AlterConfigsOptions().validateOnly(validateOnly)
        ).values().get(brokerLoggerResource);
    }

    private void alterBrokerLoggers(
        Admin admin,
        Collection<AlterConfigOp> entries,
        boolean validateOnly
    ) throws ExecutionException, InterruptedException {
        incrementalAlterBrokerLoggers(admin, entries, validateOnly).get();
    }

    private Config describeBrokerLoggers(Admin admin) throws ExecutionException, InterruptedException {
        return admin.describeConfigs(List.of(brokerLoggerResource)).values().get(brokerLoggerResource).get();
    }
}
