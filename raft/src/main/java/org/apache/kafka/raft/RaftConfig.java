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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;
import static org.apache.kafka.clients.CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class RaftConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    public static final String QUORUM_VOTERS_CONFIG = "quorum.voters";
    private static final String QUORUM_VOTERS_DOC = "List of voters. This is only used the " +
        "first time a cluster is initialized.";

    public static final String QUORUM_ELECTION_TIMEOUT_MS_CONFIG = "quorum.election.timeout.ms";
    private static final String QUORUM_ELECTION_TIMEOUT_MS_DOC = "Maximum time in milliseconds to wait " +
        "without being able to fetch from the leader before triggering a new election";

    public static final String QUORUM_ELECTION_JITTER_MAX_MS_CONFIG = "quorum.election.jitter.max.ms";
    private static final String QUORUM_ELECTION_JITTER_MAX_MS_DOC = "Maximum jitter to delay new elections. " +
        "This helps prevent gridlocked elections";

    static {
        CONFIG = new ConfigDef()
            .define(BOOTSTRAP_SERVERS_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                new ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH,
                CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
            .define(REQUEST_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                20000,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                REQUEST_TIMEOUT_MS_DOC)
            .define(RETRY_BACKOFF_MS_CONFIG,
                ConfigDef.Type.LONG,
                100L,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            .define(QUORUM_VOTERS_CONFIG,
                ConfigDef.Type.LIST,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                QUORUM_VOTERS_DOC)
            .define(QUORUM_ELECTION_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                30000,
                atLeast(0L),
                ConfigDef.Importance.HIGH,
                QUORUM_ELECTION_TIMEOUT_MS_DOC)
            .define(QUORUM_ELECTION_JITTER_MAX_MS_CONFIG,
                ConfigDef.Type.INT,
                100,
                atLeast(0L),
                ConfigDef.Importance.HIGH,
                QUORUM_ELECTION_JITTER_MAX_MS_DOC);
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

}
