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

package org.apache.kafka.common.test.api;

import kafka.network.SocketServer;
import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.server.authorizer.Authorizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.GroupProtocol.CLASSIC;
import static org.apache.kafka.clients.consumer.GroupProtocol.CONSUMER;

public interface ClusterInstance {

    Type type();

    default boolean isKRaftTest() {
        return type() == Type.KRAFT || type() == Type.CO_KRAFT;
    }

    Map<Integer, KafkaBroker> brokers();

    default Map<Integer, KafkaBroker> aliveBrokers() {
        return brokers().entrySet().stream().filter(entry -> !entry.getValue().isShutdown())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Map<Integer, ControllerServer> controllers();

    /**
     * The immutable cluster configuration used to create this cluster.
     */
    ClusterConfig config();

    /**
     * Return the set of all controller IDs configured for this test. For kraft, this
     * will return only the nodes which have the "controller" role enabled in `process.roles`.
     */
    Set<Integer> controllerIds();

    /**
     * Return the set of all broker IDs configured for this test.
     */
    default Set<Integer> brokerIds() {
        return brokers().keySet();
    }

    /**
     * The listener for this cluster as configured by {@link ClusterTest} or by {@link ClusterConfig}. If
     * unspecified by those sources, this will return the listener for the default security protocol PLAINTEXT
     */
    ListenerName clientListener();

    /**
     * The listener for the kraft cluster controller configured by controller.listener.names.
     */
    default Optional<ListenerName> controllerListenerName() {
        return Optional.empty();
    }

    /**
     * The broker connect string which can be used by clients for bootstrapping
     */
    String bootstrapServers();

    /**
     * The broker connect string which can be used by clients for bootstrapping to the controller quorum.
     */
    String bootstrapControllers();

    /**
     * A collection of all brokers in the cluster.
     */
    default Collection<SocketServer> brokerSocketServers() {
        return brokers().values().stream()
                .map(KafkaBroker::socketServer)
                .collect(Collectors.toList());
    }

    /**
     * A collection of all controllers in the cluster.
     */
    Collection<SocketServer> controllerSocketServers();

    /**
     * Return any one of the broker servers. Throw an error if none are found
     */
    default SocketServer anyBrokerSocketServer() {
        return brokerSocketServers().stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No broker SocketServers found"));
    }

    /**
     * Return any one of the controller servers. Throw an error if none are found
     */
    default SocketServer anyControllerSocketServer() {
        return controllerSocketServers().stream()
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No controller SocketServers found"));
    }

    String clusterId();

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

    default Set<GroupProtocol> supportedGroupProtocols() {
        if (isKRaftTest() && brokers().values().stream().allMatch(b -> b.dataPlaneRequestProcessor().isConsumerGroupProtocolEnabled())) {
            return Set.of(CLASSIC, CONSUMER);
        } else {
            return Collections.singleton(CLASSIC);
        }
    }

    //---------------------------[modify]---------------------------//

    void start();

    void stop();

    void shutdownBroker(int brokerId);

    void startBroker(int brokerId);

    //---------------------------[wait]---------------------------//

    default void waitTopicDeletion(String topic) throws InterruptedException {
        waitForTopic(topic, 0);
    }
    
    default void createTopic(String topicName, int partitions, short replicas) throws InterruptedException {
        try (Admin admin = createAdminClient()) {
            admin.createTopics(Collections.singletonList(new NewTopic(topicName, partitions, replicas)));
            waitForTopic(topicName, partitions);
        }
    }

    void waitForReadyBrokers() throws InterruptedException;

    default void waitForTopic(String topic, int partitions) throws InterruptedException {
        // wait for metadata
        TestUtils.waitForCondition(
            () -> aliveBrokers().values().stream().allMatch(broker -> partitions == 0 ?
                broker.metadataCache().numPartitions(topic).isEmpty() :
                broker.metadataCache().numPartitions(topic).contains(partitions)
        ), 60000L, topic + " metadata not propagated after 60000 ms");

        for (ControllerServer controller : controllers().values()) {
            long controllerOffset = controller.raftManager().replicatedLog().endOffset().offset() - 1;
            TestUtils.waitForCondition(
                () -> aliveBrokers().values().stream().allMatch(broker -> ((BrokerServer) broker).sharedServer().loader().lastAppliedOffset() >= controllerOffset),
                60000L, "Timeout waiting for controller metadata propagating to brokers");
        }
    }

    default List<Authorizer> authorizers() {
        List<Authorizer> authorizers = new ArrayList<>();
        authorizers.addAll(brokers().values().stream()
                .filter(server -> server.authorizer().isDefined())
                .map(server -> server.authorizer().get()).collect(Collectors.toList()));
        authorizers.addAll(controllers().values().stream()
                .filter(server -> server.authorizer().isDefined())
                .map(server -> server.authorizer().get()).collect(Collectors.toList()));
        return authorizers;
    }

    default void waitAcls(AclBindingFilter filter, Collection<AccessControlEntry> entries) throws InterruptedException {
        for (Authorizer authorizer : authorizers()) {
            AtomicReference<Set<AccessControlEntry>> actualEntries = new AtomicReference<>(new HashSet<>());
            TestUtils.waitForCondition(() -> {
                Set<AccessControlEntry> accessControlEntrySet = new HashSet<>();
                authorizer.acls(filter).forEach(aclBinding -> accessControlEntrySet.add(aclBinding.entry()));
                actualEntries.set(accessControlEntrySet);
                return accessControlEntrySet.containsAll(entries) && entries.containsAll(accessControlEntrySet);
            }, "expected acls: " + entries + ", actual acls: " + actualEntries.get());
        }
    }

}
