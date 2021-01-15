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

import org.apache.kafka.common.Node;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RaftTestUtil {
    public static RaftConfig buildRaftConfig(
            int requestTimeoutMs,
            int retryBackoffMs,
            int electionTimeoutMs,
            int electionBackoffMs,
            int fetchTimeoutMs,
            int appendLingerMs,
            List<Node> voterNodes
    ) {
        Properties properties = new Properties();
        properties.put(RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        properties.put(RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        properties.put(RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, electionTimeoutMs);
        properties.put(RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, electionBackoffMs);
        properties.put(RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, fetchTimeoutMs);
        properties.put(RaftConfig.QUORUM_LINGER_MS_CONFIG, appendLingerMs);

        StringBuilder votersString = new StringBuilder();
        String prefix = "";
        for (Node voter : voterNodes) {
            votersString.append(prefix);
            votersString.append(voter.id()).append('@').append(voter.host()).append(':').append(voter.port());
            prefix = ",";
        }
        properties.put(RaftConfig.QUORUM_VOTERS_CONFIG, votersString.toString());

        return new RaftConfig(properties);
    }

    public static List<Node> voterNodesFromIds(Set<Integer> voterIds,
                                               Function<Integer, InetSocketAddress> voterAddressGenerator) {
        return voterIds.stream().map(voterId -> {
            InetSocketAddress voterAddress = voterAddressGenerator.apply(voterId);
            return new Node(voterId, voterAddress.getHostName(), voterAddress.getPort());
        }).collect(Collectors.toList());
    }
}
