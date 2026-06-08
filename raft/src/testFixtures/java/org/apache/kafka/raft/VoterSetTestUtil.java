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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.server.common.Feature;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class VoterSetTestUtil {
    public static final ListenerName DEFAULT_LISTENER_NAME = ListenerName.normalised("LISTENER");

    public static Map<Integer, VoterSet.VoterNode> voterMap(
        IntStream replicas,
        boolean withDirectoryId
    ) {
        return replicas
            .boxed()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    id -> voterNode(id, withDirectoryId)
                )
            );
    }

    public static Map<Integer, VoterSet.VoterNode> voterMap(Stream<ReplicaKey> replicas) {
        return replicas
            .collect(Collectors.toMap(ReplicaKey::id, VoterSetTestUtil::voterNode));
    }

    public static VoterSet.VoterNode voterNode(int id, boolean withDirectoryId) {
        return voterNode(
            ReplicaKey.of(
                id,
                withDirectoryId ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID
            )
        );
    }

    public static VoterSet.VoterNode voterNode(ReplicaKey replicaKey) {
        return voterNode(
            replicaKey,
            Endpoints.fromInetSocketAddresses(
                Map.of(
                    DEFAULT_LISTENER_NAME,
                    InetSocketAddress.createUnresolved(
                        "localhost",
                        9990 + replicaKey.id()
                    )
                )
            )
        );
    }

    public static VoterSet.VoterNode voterNode(ReplicaKey replicaKey, Endpoints endpoints) {
        var supportedVersionRange = replicaKey.directoryId().isEmpty() ?
            new SupportedVersionRange((short) 0) :
            Feature.KRAFT_VERSION.supportedVersionRange();

        return new VoterSet.VoterNode(replicaKey, endpoints, supportedVersionRange);
    }

    public static VoterSet voterSet(Map<Integer, VoterSet.VoterNode> voters) {
        return VoterSet.fromMap(voters);
    }

    public static VoterSet voterSet(Stream<ReplicaKey> voterKeys) {
        return voterSet(voterMap(voterKeys));
    }

    private VoterSetTestUtil() {}
}
