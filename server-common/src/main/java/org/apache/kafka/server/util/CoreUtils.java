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
package org.apache.kafka.server.util;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.SocketServerConfigs;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * General helper functions!
 * <p>
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 * <p>
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
public class CoreUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreUtils.class);

    /**
     * Do the given action and log any exceptions thrown without rethrowing them.
     *
     * @param action   The action to execute.
     * @param logging  The logging instance to use for logging the thrown exception.
     * @param logLevel The log level to use for logging.
     */
    public static void swallow(Runnable action, Logger logging, Level logLevel) {
        try {
            action.run();
        } catch (Throwable e) {
            switch (logLevel) {
                case ERROR -> logging.error(e.getMessage(), e);
                case INFO -> logging.info(e.getMessage(), e);
                case DEBUG -> logging.debug(e.getMessage(), e);
                case TRACE -> logging.trace(e.getMessage(), e);
                case WARN -> logging.warn(e.getMessage(), e);
            }
        }
    }

    /**
     * Do the given action and log any exceptions thrown without rethrowing them.
     * Uses {@link Level#WARN} as the default logging level.
     *
     * @param action  The action to execute.
     * @param logging The logging instance to use for logging the thrown exception.
     */
    public static void swallow(Runnable action, Logger logging) {
        swallow(action, logging, Level.WARN);
    }

    /**
     * Recursively delete the list of files/directories and any subfiles (if any exist)
     *
     * @param files list of files to be deleted
     */
    public static void delete(List<String> files) throws IOException {
        for (String file : files) {
            Utils.delete(new File(file));
        }
    }

    /**
     * Register the given mbean with the platform mbean server,
     * unregistering any mbean that was there before. Note,
     * this method will not throw an exception if the registration
     * fails (since there is nothing you can do, and it isn't fatal),
     * instead it just returns false indicating the registration failed.
     *
     * @param mbean The object to register as a mbean
     * @param name  The name to register this mbean with
     * @return true if the registration succeeded
     */
    public static boolean registerMBean(Object mbean, String name) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            synchronized (mbs) {
                ObjectName objName = new ObjectName(name);
                if (mbs.isRegistered(objName)) {
                    mbs.unregisterMBean(objName);
                }
                mbs.registerMBean(mbean, objName);
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to register Mbean with name {}", name, e);
            return false;
        }
    }

    public static List<Endpoint> listenerListToEndPoints(List<String> listeners, Map<ListenerName, SecurityProtocol> securityProtocolMap) {
        return listenerListToEndPoints(listeners, securityProtocolMap, true);
    }

    public static List<Endpoint> listenerListToEndPoints(List<String> listeners, Map<ListenerName, SecurityProtocol> securityProtocolMap, boolean requireDistinctPorts) {
        List<Endpoint> endPoints;
        try {
            endPoints = SocketServerConfigs.listenerListToEndPoints(listeners, securityProtocolMap);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error creating broker listeners from %s: %s".formatted(listeners, e.getMessage()));
        }
        validate(endPoints, listeners, requireDistinctPorts);
        return endPoints;
    }

    private static void validate(List<Endpoint> endPoints, List<String> listeners, boolean requireDistinctPorts) {
        if (hasDuplicateListeners(endPoints)) {
            throw new IllegalArgumentException("Each listener must have a different name, listeners: %s".formatted(listeners));
        }
        Map<Integer, List<Endpoint>> endpointsByPort = endPoints.stream()
                .filter(x -> x.port() != 0) // filter port 0 for unit tests
                .collect(Collectors.groupingBy(Endpoint::port));

        // Exception case, let's allow duplicate ports if one host is on IPv4 and the other one is on IPv6
        Map<Integer, EndpointHostPartition> duplicatePortsPartitionedByValidIps = endpointsByPort.entrySet().stream()
                .filter(x -> x.getValue().size() > 1) // filter if no duplicated hosts
                .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().stream()
                                        .collect(
                                                EndpointHostPartition::new,
                                                CoreUtils::partitionByHostValidity,
                                                CoreUtils::mergeResults
                                        )
                        )
                );

        duplicatePortsPartitionedByValidIps.forEach((port, hostPartition) -> {
            if (requireDistinctPorts && hasDuplicatePorts(hostPartition.withoutHost)) {
                throw new IllegalArgumentException("Each listener must have a different port, listeners: %s".formatted(listeners));
            }
            if (hostPartition.withHost.size() == 2) {
                String errorMessage = "If you have two listeners on the same port then one needs to be IPv4 and the other IPv6" +
                        "listeners: %s, port: %d".formatted(listeners, port);
                if (!validateOneIsIpv4AndOtherIpv6(hostPartition.withHost.get(0).host(), hostPartition.withHost.get(1).host())) {
                    throw new IllegalArgumentException(errorMessage);
                }
                // If we reach this point it means that even though duplicatesWithIpHosts in isolation can be valid, if
                // there happens to be ANOTHER listener on this port without an IP host (such as a null host) then it's
                // not valid.
                if (!hostPartition.withoutHost.isEmpty())
                    throw new IllegalArgumentException(errorMessage);
            }
            // Having more than 2 duplicate endpoints doesn't make sense since we only have 2 IP stacks (one is IPv4 and the other is IPv6)
            if (hostPartition.withHost.size() > 2 && requireDistinctPorts) {
                throw new IllegalArgumentException("Each listener must have a different port unless exactly one listener has " +
                        "an IPv4 address and the other IPv6 address, listeners: %s, port: %d".formatted(listeners, port));
            }
        });
    }

    private static void partitionByHostValidity(EndpointHostPartition endpointHostPartition, Endpoint endpoint) {
        InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();
        if (endpoint.host() != null && inetAddressValidator.isValid(endpoint.host())) {
            endpointHostPartition.withHost.add(endpoint);
        } else {
            endpointHostPartition.withoutHost.add(endpoint);
        }
    }

    private static void mergeResults(EndpointHostPartition endpointHostPartition1, EndpointHostPartition endpointHostPartition2) {
        endpointHostPartition1.withHost.addAll(endpointHostPartition2.withHost);
        endpointHostPartition1.withoutHost.addAll(endpointHostPartition2.withoutHost);
    }

    private static boolean validateOneIsIpv4AndOtherIpv6(String first, String second) {
        InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();
        return (inetAddressValidator.isValidInet4Address(first) && inetAddressValidator.isValidInet6Address(second)) ||
                (inetAddressValidator.isValidInet6Address(first) && inetAddressValidator.isValidInet4Address(second));
    }

    private static boolean hasDuplicatePorts(List<Endpoint> endpoints) {
        long distinctPortsCount = endpoints.stream().map(Endpoint::port).distinct().count();
        return distinctPortsCount != endpoints.size();
    }

    private static boolean hasDuplicateListeners(List<Endpoint> endpoints) {
        long distinctPortsCount = endpoints.stream().map(Endpoint::listener).distinct().count();
        return distinctPortsCount != endpoints.size();
    }

    private static class EndpointHostPartition {
        private final List<Endpoint> withHost = new ArrayList<>();
        private final List<Endpoint> withoutHost = new ArrayList<>();
    }
}
