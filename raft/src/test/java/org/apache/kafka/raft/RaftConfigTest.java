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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RaftConfigTest {

    @Test
    public void testSingleQuorumVoterConnections() {
        RaftConfig config = raftDefaultConfigWithVoters("1@127.0.0.1:9092");
        assertEquals(Collections.singletonMap(1, new InetSocketAddress("127.0.0.1", 9092)),
            config.quorumVoterConnections());
    }

    @Test
    public void testMultiQuorumVoterConnections() {
        RaftConfig config = raftDefaultConfigWithVoters("1@kafka1:9092,2@kafka2:9092,3@kafka3:9092");

        HashMap<Integer, InetSocketAddress> expected = new HashMap<>();
        expected.put(1, new InetSocketAddress("kafka1", 9092));
        expected.put(2, new InetSocketAddress("kafka2", 9092));
        expected.put(3, new InetSocketAddress("kafka3", 9092));

        assertEquals(expected, config.quorumVoterConnections());
    }

    @Test
    public void testInvalidQuorumVotersConfig() {
        assertInvalidQuorumVoters("");
        assertInvalidQuorumVoters("1");
        assertInvalidQuorumVoters("1@");
        assertInvalidQuorumVoters("1:");
        assertInvalidQuorumVoters("blah@");
        assertInvalidQuorumVoters("1@kafka1");
        assertInvalidQuorumVoters("1@kafka1:9092,");
        assertInvalidQuorumVoters("1@kafka1:9092,");
        assertInvalidQuorumVoters("1@kafka1:9092,2");
        assertInvalidQuorumVoters("1@kafka1:9092,2@");
        assertInvalidQuorumVoters("1@kafka1:9092,2@blah");
        assertInvalidQuorumVoters("1@kafka1:9092,2@blah,");
    }

    private void assertInvalidQuorumVoters(String value) {
        assertThrows(ConfigException.class, () -> raftDefaultConfigWithVoters(value));
    }

    private RaftConfig raftDefaultConfigWithVoters(String voters) {
        return new RaftConfig(RaftConfig.DEFAULT_QUORUM_REQUEST_TIMEOUT_MS,
                RaftConfig.DEFAULT_QUORUM_RETRY_BACKOFF_MS,
                RaftConfig.DEFAULT_QUORUM_ELECTION_TIMEOUT_MS,
                RaftConfig.DEFAULT_QUORUM_ELECTION_BACKOFF_MAX_MS,
                RaftConfig.DEFAULT_QUORUM_FETCH_TIMEOUT_MS,
                RaftConfig.DEFAULT_QUORUM_LINGER_MS,
                voters);
    }
}
