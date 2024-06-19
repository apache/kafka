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

package org.apache.kafka.server.utils;


import org.apache.commons.validator.routines.InetAddressValidator;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.util.Csv;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EndpointUtils {
    private static InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();

    private static final Map<ListenerName, SecurityProtocol> DEFAULT_SECURITY_PROTOCOL_MAP = Arrays.stream(SecurityProtocol.values())
            .collect(Collectors.toMap(sp -> ListenerName.forSecurityProtocol(sp), sp -> sp));
    private static Pattern uriParseExp = Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");

    public static List<Endpoint> listenerListToEndpoints(String listeners, Map<ListenerName, SecurityProtocol> securityProtocolMap) {
        return listenerListToEndpoints(listeners, securityProtocolMap, true);
    }

    public static List<Endpoint> listenerListToEndpoints(String listeners, Map<ListenerName, SecurityProtocol> securityProtocolMap, boolean requireDistinctPorts) {
        List<Endpoint> endPoints;
        try {
            List<String> listenerList = Csv.parseCsvList(listeners);
            endPoints = listenerList.stream()
                    .map(listener -> createEndpoint(listener, Optional.of(securityProtocolMap)))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new IllegalArgumentException("Error creating broker listeners from '" + listeners + "': " + e.getMessage(), e);
        }

        validate(endPoints, listeners, requireDistinctPorts);
        return endPoints;
    }

    private static void validate(List<Endpoint> endPoints, String listeners, boolean requireDistinctPorts) {
        List<ListenerName> distinctListenerNames = endPoints.stream()
                .map(Endpoint::listenerName)
                .filter(Optional::isPresent)
                .map(name -> new ListenerName(name.get()))
                .distinct()
                .collect(Collectors.toList());

        if (distinctListenerNames.size() != endPoints.size()) {
            throw new IllegalArgumentException("Each listener must have a different name, listeners: " + listeners);
        }

        Map<Integer, List<Endpoint>> duplicatePorts = endPoints.stream()
                .filter(ep -> ep.port() != 0)
                .collect(Collectors.groupingBy(Endpoint::port));

        duplicatePorts.entrySet().removeIf(entry -> entry.getValue().size() <= 1);

        Map<Integer, Map<Boolean, List<Endpoint>>> duplicatesPartitionedByValidIps = duplicatePorts.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry ->
                        entry.getValue().stream()
                                .collect(Collectors.partitioningBy(ep ->
                                        ep.host() != null && inetAddressValidator.isValid(ep.host())
                                ))
                ));

        duplicatesPartitionedByValidIps.forEach((port, partitionedDuplicates) -> {
            List<Endpoint> duplicatesWithIpHosts = partitionedDuplicates.getOrDefault(true, Collections.emptyList());
            List<Endpoint> duplicatesWithoutIpHosts = partitionedDuplicates.getOrDefault(false, Collections.emptyList());

            if (requireDistinctPorts) {
                checkDuplicateListenerPorts(duplicatesWithoutIpHosts, listeners);
            }

            if (!duplicatesWithIpHosts.isEmpty()) {
                if (duplicatesWithIpHosts.size() == 2) {
                    if (requireDistinctPorts) {
                        String errorMessage = "If you have two listeners on " +
                                "the same port then one needs to be IPv4 and the other IPv6, listeners: " + listeners + ", port: " + port;
                        if (!validateOneIsIpv4AndOtherIpv6(duplicatesWithIpHosts.get(0).host(), duplicatesWithIpHosts.get(1).host())) {
                            throw new IllegalArgumentException(errorMessage);
                        }

                        if (!duplicatesWithoutIpHosts.isEmpty()) {
                            throw new IllegalArgumentException(errorMessage);
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Each listener must have a different port unless exactly one listener has " +
                            "an IPv4 address and the other IPv6 address, listeners: " + listeners + ", port: " + port);
                }
            }
        });
    }

    private static boolean validateOneIsIpv4AndOtherIpv6(String first, String second) {
        return (inetAddressValidator.isValidInet4Address(first) && inetAddressValidator.isValidInet6Address(second)) ||
                (inetAddressValidator.isValidInet6Address(first) && inetAddressValidator.isValidInet4Address(second));
    }

    private static void checkDuplicateListenerPorts(List<Endpoint> endpoints, String listeners) {
        Set<Integer> distinctPorts = endpoints.stream()
                .map(Endpoint::port)
                .collect(Collectors.toSet());

        if (distinctPorts.size() != endpoints.size()) {
            throw new IllegalArgumentException("Each listener must have a different port, listeners: " + listeners);
        }
    }

    private static Endpoint createEndpoint(String connectionString, Optional<Map<ListenerName, SecurityProtocol>> securityProtocolMap) {
        Map<ListenerName, SecurityProtocol> protocolMap = securityProtocolMap.orElse(DEFAULT_SECURITY_PROTOCOL_MAP);
        Matcher matcher = uriParseExp.matcher(connectionString);

        if (matcher.matches()) {
            String listenerNameString = matcher.group(1).toUpperCase();
            String host = matcher.group(2).equals("") ? null : matcher.group(2);
            String port = matcher.group(3);
            ListenerName listenerName = ListenerName.normalised(listenerNameString);
            return new Endpoint(listenerNameString, securityProtocol(listenerName, protocolMap), host, Integer.parseInt(port));
        } else {
            throw new KafkaException("Unable to parse " + connectionString + " to a broker endpoint");
        }
    }

    private static SecurityProtocol securityProtocol(ListenerName listenerName, Map<ListenerName, SecurityProtocol> protocolMap) {
        return Optional.ofNullable(protocolMap.get(listenerName))
                .orElseThrow(() -> new IllegalArgumentException("No security protocol defined for listener " + listenerName.value()));
    }

    public static String parseListenerName(String connectionString) {
        Matcher matcher = uriParseExp.matcher(connectionString);
        if (matcher.matches()) {
            String listenerNameString = matcher.group(1);
            return listenerNameString.toUpperCase(Locale.ROOT);
        } else {
            throw new KafkaException("Unable to parse a listener name from " + connectionString);
        }
    }
}
