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
import kafka.test.annotation.ClusterTest;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

public interface ClusterInstance {

    enum ClusterType {
        ZK,
        RAFT
    }

    /**
     * Cluster type. For now, only ZK is supported.
     */
    ClusterType clusterType();

    default boolean isKRaftTest() {
        return clusterType() == ClusterType.RAFT;
    }

    /**
     * The cluster configuration used to create this cluster. Changing data in this instance through this accessor will
     * have no effect on the cluster since it is already provisioned.
     */
    ClusterConfig config();

    /**
     * The listener for this cluster as configured by {@link ClusterTest} or by {@link ClusterConfig}. If
     * unspecified by those sources, this will return the listener for the default security protocol PLAINTEXT
     */
    ListenerName clientListener();

    /**
     * The listener for the kraft cluster controller configured by controller.listener.names. In ZK-based clusters, return Optional.empty
     */
    default Optional<ListenerName> controllerListenerName() {
        return Optional.empty();
    }

    /**
     * The listener for the zk controller configured by control.plane.listener.name. In Raft-based clusters, return Optional.empty
     */
    default Optional<ListenerName> controlPlaneListenerName() {
        return Optional.empty();
    }

    /**
     * The broker connect string which can be used by clients for bootstrapping
     */
    String bootstrapServers();

    /**
     * A collection of all brokers in the cluster. In ZK-based clusters this will also include the broker which is
     * acting as the controller (since ZK controllers serve both broker and controller roles).
     */
    Collection<SocketServer> brokerSocketServers();

    /**
     * A collection of all controllers in the cluster. For ZK-based clusters, this will return the broker which is also
     * currently the active controller. For Raft-based clusters, this will return all controller servers.
     */
    Collection<SocketServer> controllerSocketServers();

    /**
     * Return any one of the broker servers. Throw an error if none are found
     */
    SocketServer anyBrokerSocketServer();

    /**
     * Return any one of the controller servers. Throw an error if none are found
     */
    SocketServer anyControllerSocketServer();

    /**
     * The underlying object which is responsible for setting up and tearing down the cluster.
     */
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

    void shutdownBroker(int brokerId);

    void startBroker(int brokerId);

    void rollingBrokerRestart();

    void waitForReadyBrokers() throws InterruptedException;
}
