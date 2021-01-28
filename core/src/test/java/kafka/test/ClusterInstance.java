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

package kafka.test;

import kafka.network.SocketServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

public interface ClusterInstance {

    enum ClusterType {
        Zk,
        // Raft
    }

    ClusterType clusterType();

    ClusterConfig config();

    ListenerName listener();

    String brokerList();

    Collection<SocketServer> brokers();

    Collection<SocketServer> controllers();

    Optional<SocketServer> anyBroker();

    Optional<SocketServer> anyController();

    Object getUnderlying();

    default <T> T getUnderlying(Class<T> asClass) {
        return asClass.cast(getUnderlying());
    }

    Admin createAdminClient(Properties configOverrides);

    default Admin createAdminClient() {
        return createAdminClient(new Properties());
    }

    void start();

    void stop();
}
