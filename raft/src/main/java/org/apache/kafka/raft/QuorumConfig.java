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
package org.apache.kafka.raft;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * QuorumConfig encapsulates configuration specific to the cluster metadata KRaft replicas.
 *
 * The default raft timeouts are relatively low compared to some other timeouts such as
 * request.timeout.ms. This is part of a general design philosophy where we see changing
 * the leader of a Raft cluster as a relatively quick operation. For example, the KIP-631
 * controller should be able to transition from standby to active without reloading all of
 * the metadata. The standby is a "hot" standby, not a "cold" one.
 */
public class QuorumConfig {

    private static final String QUORUM_PREFIX = "controller.quorum.";

    // Non-routable address represents an endpoint that does not resolve to any particular node
    public static final String NON_ROUTABLE_HOST = "0.0.0.0";

    public static final String QUORUM_VOTERS_CONFIG = QUORUM_PREFIX + "voters";
    public static final String QUORUM_VOTERS_DOC = "Map of id/endpoint information for " +
        "the set of voters in a comma-separated list of <code>{id}@{host}:{port}</code> entries. " +
        "For example: <code>1@localhost:9092,2@localhost:9093,3@localhost:9094</code>";
    public static final List<String> DEFAULT_QUORUM_VOTERS = Collections.emptyList();

    public static final String QUORUM_ELECTION_TIMEOUT_MS_CONFIG = QUORUM_PREFIX + "election.timeout.ms";
    public static final String QUORUM_ELECTION_TIMEOUT_MS_DOC = "Maximum time in milliseconds to wait " +
        "without being able to fetch from the leader before triggering a new election";
    public static final int DEFAULT_QUORUM_ELECTION_TIMEOUT_MS = 1_000;

    public static final String QUORUM_FETCH_TIMEOUT_MS_CONFIG = QUORUM_PREFIX + "fetch.timeout.ms";
    public static final String QUORUM_FETCH_TIMEOUT_MS_DOC = "Maximum time without a successful fetch from " +
        "the current leader before becoming a candidate and triggering an election for voters; Maximum time " +
        "a leader can go without receiving valid fetch or fetchSnapshot request from a majority of the quorum before resigning.";
    public static final int DEFAULT_QUORUM_FETCH_TIMEOUT_MS = 2_000;

    public static final String QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG = QUORUM_PREFIX + "election.backoff.max.ms";
    public static final String QUORUM_ELECTION_BACKOFF_MAX_MS_DOC = "Maximum time in milliseconds before starting new elections. " +
        "This is used in the binary exponential backoff mechanism that helps prevent gridlocked elections";
    public static final int DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS = 1_000;

    public static final String QUORUM_LINGER_MS_CONFIG = QUORUM_PREFIX + "append.linger.ms";
    public static final String QUORUM_LINGER_MS_DOC = "The duration in milliseconds that the leader will " +
        "wait for writes to accumulate before flushing them to disk.";
    public static final int DEFAULT_QUORUM_LINGER_MS = 25;

    public static final String QUORUM_REQUEST_TIMEOUT_MS_CONFIG = QUORUM_PREFIX +
        CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    public static final String QUORUM_REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
    public static final int DEFAULT_QUORUM_REQUEST_TIMEOUT_MS = 2_000;

    public static final String QUORUM_RETRY_BACKOFF_MS_CONFIG = QUORUM_PREFIX +
        CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
    public static final String QUORUM_RETRY_BACKOFF_MS_DOC = CommonClientConfigs.RETRY_BACKOFF_MS_DOC;
    public static final int DEFAULT_QUORUM_RETRY_BACKOFF_MS = 20;

    private final int requestTimeoutMs;
    private final int retryBackoffMs;
    private final int electionTimeoutMs;
    private final int electionBackoffMaxMs;
    private final int fetchTimeoutMs;
    private final int appendLingerMs;

    public QuorumConfig(AbstractConfig abstractConfig) {
        this(
            abstractConfig.getInt(QUORUM_REQUEST_TIMEOUT_MS_CONFIG),
            abstractConfig.getInt(QUORUM_RETRY_BACKOFF_MS_CONFIG),
            abstractConfig.getInt(QUORUM_ELECTION_TIMEOUT_MS_CONFIG),
            abstractConfig.getInt(QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG),
            abstractConfig.getInt(QUORUM_FETCH_TIMEOUT_MS_CONFIG),
            abstractConfig.getInt(QUORUM_LINGER_MS_CONFIG)
        );
    }

    public QuorumConfig(
        int requestTimeoutMs,
        int retryBackoffMs,
        int electionTimeoutMs,
        int electionBackoffMaxMs,
        int fetchTimeoutMs,
        int appendLingerMs
    ) {
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionBackoffMaxMs = electionBackoffMaxMs;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.appendLingerMs = appendLingerMs;
    }

    public int requestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int retryBackoffMs() {
        return retryBackoffMs;
    }

    public int electionTimeoutMs() {
        return electionTimeoutMs;
    }

    public int electionBackoffMaxMs() {
        return electionBackoffMaxMs;
    }

    public int fetchTimeoutMs() {
        return fetchTimeoutMs;
    }

    public int appendLingerMs() {
        return appendLingerMs;
    }

    private static Integer parseVoterId(String idString) {
        try {
            return Integer.parseInt(idString);
        } catch (NumberFormatException e) {
            throw new ConfigException("Failed to parse voter ID as an integer from " + idString);
        }
    }

    public static Map<Integer, InetSocketAddress> parseVoterConnections(List<String> voterEntries) {
        return parseVoterConnections(voterEntries, true);
    }

    public static Set<Integer> parseVoterIds(List<String> voterEntries) {
        return parseVoterConnections(voterEntries, false).keySet();
    }

    private static Map<Integer, InetSocketAddress> parseVoterConnections(
        List<String> voterEntries,
        boolean requireRoutableAddresses
    ) {
        Map<Integer, InetSocketAddress> voterMap = new HashMap<>();
        for (String voterMapEntry : voterEntries) {
            String[] idAndAddress = voterMapEntry.split("@");
            if (idAndAddress.length != 2) {
                throw new ConfigException("Invalid configuration value for " + QUORUM_VOTERS_CONFIG
                    + ". Each entry should be in the form `{id}@{host}:{port}`.");
            }

            Integer voterId = parseVoterId(idAndAddress[0]);
            String host = Utils.getHost(idAndAddress[1]);
            if (host == null) {
                throw new ConfigException("Failed to parse host name from entry " + voterMapEntry
                    + " for the configuration " + QUORUM_VOTERS_CONFIG
                    + ". Each entry should be in the form `{id}@{host}:{port}`.");
            }

            Integer port = Utils.getPort(idAndAddress[1]);
            if (port == null) {
                throw new ConfigException("Failed to parse host port from entry " + voterMapEntry
                    + " for the configuration " + QUORUM_VOTERS_CONFIG
                    + ". Each entry should be in the form `{id}@{host}:{port}`.");
            }

            InetSocketAddress address = new InetSocketAddress(host, port);
            if (address.getHostString().equals(NON_ROUTABLE_HOST) && requireRoutableAddresses) {
                throw new ConfigException(
                    String.format("Host string (%s) is not routeable", address.getHostString())
                );
            } else {
                voterMap.put(voterId, address);
            }
        }

        return voterMap;
    }

    public static List<Node> quorumVoterStringsToNodes(List<String> voters) {
        return voterConnectionsToNodes(parseVoterConnections(voters));
    }

    public static List<Node> voterConnectionsToNodes(Map<Integer, InetSocketAddress> voterConnections) {
        return voterConnections
            .entrySet()
            .stream()
            .filter(Objects::nonNull)
            .map(entry -> new Node(entry.getKey(), entry.getValue().getHostString(), entry.getValue().getPort()))
            .collect(Collectors.toList());
    }

    public static class ControllerQuorumVotersValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, null);
            }

            @SuppressWarnings("unchecked")
            List<String> voterStrings = (List<String>) value;

            // Attempt to parse the connect strings
            parseVoterConnections(voterStrings, false);
        }

        @Override
        public String toString() {
            return "non-empty list";
        }
    }
}
