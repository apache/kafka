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
package org.apache.kafka.common.replica;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ReplicaSelector extends Configurable, Closeable {

    /**
     * Select the preferred replica a client should use for fetching. If no replica is available, this will return an
     * empty optional.
     */
    Optional<ReplicaInfo> select(TopicPartition topicPartition,
                                 ClientMetadata clientMetadata,
                                 Set<ReplicaInfo> replicaInfos);

    @Override
    default void close() throws IOException {

    }

    @Override
    default void configure(Map<String, ?> configs) {

    }


    class ClientMetadata {

        public static final ClientMetadata NO_METADATA = new ClientMetadata("", "", null, null, null);

        public final String rackId;
        public final String clientId;
        public final InetAddress clientAddress;
        public final KafkaPrincipal principal;
        public final String listenerName;

        public ClientMetadata(String rackId, String clientId, InetAddress clientAddress, KafkaPrincipal principal, String listenerName) {
            this.rackId = rackId;
            this.clientId = clientId;
            this.clientAddress = clientAddress;
            this.principal = principal;
            this.listenerName = listenerName;
        }
    }

    interface ReplicaInfo {

        boolean isLeader();

        Node getEndpoint();

        long logOffset();

        long lastCaughtUpTimeMs();
    }
}


