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

package kafka.server.logger;

import kafka.utils.Log4jController;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.config.LogLevelConfig;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource;
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER_LOGGER;

/**
 * Manages runtimes changes to slf4j settings.
 */
public class RuntimeLoggerManager {
    static final String VALID_LOG_LEVELS_STRING;

    static {
        ArrayList<String> logLevels = new ArrayList<>(LogLevelConfig.VALID_LOG_LEVELS);
        logLevels.sort(String::compareTo);
        VALID_LOG_LEVELS_STRING = String.join(", ", logLevels);
    }

    private final int nodeId;
    private final Logger log;

    public RuntimeLoggerManager(int nodeId,  Logger log) {
        this.nodeId = nodeId;
        this.log = log;
    }

    public void applyChangesForResource(
        boolean authorizedForClusterResource,
        boolean validateOnly,
        AlterConfigsResource resource
    ) {
        if (!authorizedForClusterResource) {
            throw new ClusterAuthorizationException(Errors.CLUSTER_AUTHORIZATION_FAILED.message());
        }
        validateResourceNameIsNodeId(resource.resourceName());
        validateLogLevelConfigs(resource.configs());
        if (!validateOnly) {
            alterLogLevelConfigs(resource.configs());
        }
    }

    void alterLogLevelConfigs(Collection<AlterableConfig> ops) {
        ops.forEach(op -> {
            String loggerName = op.name();
            String logLevel = op.value();
            switch (OpType.forId(op.configOperation())) {
                case SET:
                    if (Log4jController.logLevel(loggerName, logLevel)) {
                        log.warn("Updated the log level of {} to {}", loggerName, logLevel);
                    } else {
                        log.error("Failed to update the log level of {} to {}", loggerName, logLevel);
                    }
                    break;
                case DELETE:
                    if (Log4jController.unsetLogLevel(loggerName)) {
                        log.warn("Unset the log level of {}", loggerName);
                    } else {
                        log.error("Failed to unset the log level of {}", loggerName);
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                        "Invalid log4j configOperation: " + op.configOperation());
            }
        });
    }

    void validateResourceNameIsNodeId(String resourceName) {
        int requestId;
        try {
            requestId = Integer.parseInt(resourceName);
        } catch (NumberFormatException e) {
            throw new InvalidRequestException("Node id must be an integer, but it is: " +
                resourceName);
        }
        if (requestId != nodeId) {
            throw new InvalidRequestException("Unexpected node id. Expected " + nodeId +
                ", but received " + nodeId);
        }
    }

    void validateLoggerNameExists(String loggerName) {
        if (!Log4jController.loggerExists(loggerName)) {
            throw new InvalidConfigurationException("Logger " + loggerName + " does not exist!");
        }
    }

    void validateLogLevelConfigs(Collection<AlterableConfig> ops) {
        ops.forEach(op -> {
            String loggerName = op.name();
            switch (OpType.forId(op.configOperation())) {
                case SET:
                    validateLoggerNameExists(loggerName);
                    String logLevel = op.value();
                    if (!LogLevelConfig.VALID_LOG_LEVELS.contains(logLevel)) {
                        throw new InvalidConfigurationException("Cannot set the log level of " +
                            loggerName + " to " + logLevel + " as it is not a supported log level. " +
                            "Valid log levels are " + VALID_LOG_LEVELS_STRING);
                    }
                    break;
                case DELETE:
                    validateLoggerNameExists(loggerName);
                    if (loggerName.equals(Log4jController.ROOT_LOGGER())) {
                        throw new InvalidRequestException("Removing the log level of the " +
                            Log4jController.ROOT_LOGGER() + " logger is not allowed");
                    }
                    break;
                case APPEND:
                    throw new InvalidRequestException(OpType.APPEND +
                        " operation is not allowed for the " + BROKER_LOGGER + " resource");
                case SUBTRACT:
                    throw new InvalidRequestException(OpType.SUBTRACT +
                        " operation is not allowed for the " + BROKER_LOGGER + " resource");
                default:
                    throw new InvalidRequestException("Unknown operation type " +
                        (int) op.configOperation() + " is not allowed for the " +
                        BROKER_LOGGER + " resource");
            }
        });
    }
}
