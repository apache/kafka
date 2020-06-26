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

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
import static org.apache.kafka.clients.CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class RaftConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    private static final String QUORUM_PREFIX = "quorum.";

    public static final String QUORUM_VOTERS_CONFIG = QUORUM_PREFIX + "bootstrap.voters";
    private static final String QUORUM_VOTERS_DOC = "List of voters. This is only used the " +
        "first time a cluster is initialized.";

    public static final String QUORUM_ELECTION_TIMEOUT_MS_CONFIG = QUORUM_PREFIX + "election.timeout.ms";
    private static final String QUORUM_ELECTION_TIMEOUT_MS_DOC = "Maximum time in milliseconds to wait " +
        "without being able to fetch from the leader before triggering a new election";

    public static final String QUORUM_FETCH_TIMEOUT_MS_CONFIG = QUORUM_PREFIX + "fetch.timeout.ms";
    private static final String QUORUM_FETCH_TIMEOUT_MS_DOC = "Maximum time without a successful fetch from " +
        "the current leader before becoming a candidate and triggering a election for voters; Maximum time without " +
        "receiving fetch from a majority of the quorum before asking around to see if there's a new epoch for leader";

    public static final String QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG = QUORUM_PREFIX + "election.backoff.max.ms";
    private static final String QUORUM_ELECTION_BACKOFF_MAX_MS_DOC = "Maximum time in milliseconds before starting new elections. " +
            "This is used in the binary exponential backoff mechanism that helps prevent gridlocked elections";

    static {
        CONFIG = new ConfigDef()
            .define(QUORUM_PREFIX + BOOTSTRAP_SERVERS_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                new ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH,
                CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
            .define(QUORUM_PREFIX + REQUEST_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                20000,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                REQUEST_TIMEOUT_MS_DOC)
            .define(QUORUM_PREFIX + RETRY_BACKOFF_MS_CONFIG,
                ConfigDef.Type.INT,
                100,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            .define(QUORUM_VOTERS_CONFIG,
                ConfigDef.Type.LIST,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(String name, Object value) {
                        if (value == null || ((List) value).isEmpty()) {
                            throw new ConfigException(name, value, "Empty list");
                        }
                    }

                    @Override
                    public String toString() {
                        return "non-empty list";
                    }
                },
                ConfigDef.Importance.HIGH,
                QUORUM_VOTERS_DOC)
            .define(QUORUM_ELECTION_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                5000,
                atLeast(0L),
                ConfigDef.Importance.HIGH,
                QUORUM_ELECTION_TIMEOUT_MS_DOC)
            .define(QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG,
                ConfigDef.Type.INT,
                5000,
                atLeast(0),
                ConfigDef.Importance.HIGH,
                QUORUM_ELECTION_BACKOFF_MAX_MS_DOC)
            .define(QUORUM_FETCH_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                15000,
                atLeast(0),
                ConfigDef.Importance.HIGH,
                QUORUM_FETCH_TIMEOUT_MS_DOC);
    }


    public RaftConfig(Properties props) {
        super(CONFIG, props);
    }

    public RaftConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    protected RaftConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml());
    }

    public int requestTimeoutMs() {
        return getInt(QUORUM_PREFIX + CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG);
    }

    public int retryBackoffMs() {
        return getInt(QUORUM_PREFIX + CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
    }

    public int electionTimeoutMs() {
        return getInt(QUORUM_ELECTION_TIMEOUT_MS_CONFIG);
    }

    public int electionBackoffMaxMs() {
        return getInt(QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG);
    }

    public int fetchTimeoutMs() {
        return getInt(QUORUM_FETCH_TIMEOUT_MS_CONFIG);
    }

    public Set<Integer> bootstrapVoters() {
        return getList(QUORUM_VOTERS_CONFIG).stream().map(Integer::valueOf).collect(Collectors.toSet());
    }

    public List<InetSocketAddress> bootstrapServers() {
        return ClientUtils.parseAndValidateAddresses(getList(QUORUM_PREFIX + BOOTSTRAP_SERVERS_CONFIG),
            ClientDnsLookup.USE_ALL_DNS_IPS);
    }
}
