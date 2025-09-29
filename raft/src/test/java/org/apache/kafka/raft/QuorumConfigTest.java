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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class QuorumConfigTest {
    @Test
    public void testIllegalConfig() {
        assertInvalidConfig(Map.of(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, "-1"));
        assertInvalidConfig(Map.of(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, "-1"));
        assertInvalidConfig(Map.of(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, "-1"));
        assertInvalidConfig(Map.of(QuorumConfig.QUORUM_LINGER_MS_CONFIG, "-1"));
        assertInvalidConfig(Map.of(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, "-1"));
        assertInvalidConfig(Map.of(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, "-1"));
        assertInvalidConfig(Map.of(QuorumConfig.QUORUM_AUTO_JOIN_ENABLE_CONFIG, "-1"));
    }

    private void assertInvalidConfig(Map<String, Object> overrideConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9092");
        props.put(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, "10");
        props.put(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, "10");
        props.put(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, "10");
        props.put(QuorumConfig.QUORUM_LINGER_MS_CONFIG, "10");
        props.put(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, "10");
        props.put(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, "10");
        props.put(QuorumConfig.QUORUM_AUTO_JOIN_ENABLE_CONFIG, true);

        props.putAll(overrideConfig);

        assertThrows(ConfigException.class, () -> QuorumConfig.CONFIG_DEF.parse(props));
    }

}
