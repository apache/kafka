/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools.reassign;

import kafka.server.IsrChangePropagationConfig;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.server.QuorumTestHarness;
import kafka.server.ZkAlterPartitionManager;
import kafka.utils.TestInfoUtils;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Some$;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.server.common.MetadataVersion.IBP_2_7_IV1;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.BROKER_LEVEL_THROTTLES;

@Timeout(300)
public class ReassignPartitionsIntegrationTest extends QuorumTestHarness {
    ReassignPartitionsTestCluster cluster;

    @AfterEach
    @Override
    public void tearDown() {
        Utils.closeQuietly(cluster, "ReassignPartitionsTestCluster");
        super.tearDown();
    }

    private final Map<Integer, Map<String, Long>> unthrottledBrokerConfigs = new HashMap<>(); {
        IntStream.range(0, 4).forEach(brokerId ->
            unthrottledBrokerConfigs.put(brokerId, BROKER_LEVEL_THROTTLES.stream()
                .collect(Collectors.toMap(throttle -> throttle, throttle -> -1L))));
    }

    @ParameterizedTest(name = "{displayName}.quorum={0}")
    @ValueSource(strings = {"zk", "kraft"})
    public void testReassignment(String quorum) throws Exception {
        ReassignPartitionsTestCluster cluster = new ReassignPartitionsTestCluster(Collections.emptyMap(), Collections.emptyMap());
        cluster.setup();
        executeAndVerifyReassignment();
    }

    @ParameterizedTest(name = "{displayName}.quorum={0}")
    @ValueSource(strings = "zk") // Note: KRaft requires AlterPartition
    public void testReassignmentWithAlterPartitionDisabled(String quorum) throws Exception {
        // Test reassignment when the IBP is on an older version which does not use
        // the `AlterPartition` API. In this case, the controller will register individual
        // watches for each reassigning partition so that the reassignment can be
        // completed as soon as the ISR is expanded.
        Map<String, String> configOverrides = Collections.singletonMap(KafkaConfig.InterBrokerProtocolVersionProp(), IBP_2_7_IV1.version());
        cluster = new ReassignPartitionsTestCluster(configOverrides, Collections.emptyMap());
        cluster.setup();
        executeAndVerifyReassignment();
    }

    @ParameterizedTest(name = "{displayName}.quorum={0}")
    @ValueSource(strings = "zk") // Note: KRaft requires AlterPartition
    public void testReassignmentCompletionDuringPartialUpgrade(String quorum) throws Exception {
        // Test reassignment during a partial upgrade when some brokers are relying on
        // `AlterPartition` and some rely on the old notification logic through Zookeeper.
        // In this test case, broker 0 starts up first on the latest IBP and is typically
        // elected as controller. The three remaining brokers start up on the older IBP.
        // We want to ensure that reassignment can still complete through the ISR change
        // notification path even though the controller expects `AlterPartition`.

        // Override change notification settings so that test is not delayed by ISR
        // change notification delay
        ZkAlterPartitionManager.DefaultIsrPropagationConfig_$eq(new IsrChangePropagationConfig(500, 100, 500));

        Map<String, String> oldIbpConfig = Collections.singletonMap(KafkaConfig.InterBrokerProtocolVersionProp(), IBP_2_7_IV1.version());
        Map<Integer, Map<String, String>> brokerConfigOverrides = new HashMap<>();
        brokerConfigOverrides.put(1, oldIbpConfig);
        brokerConfigOverrides.put(2, oldIbpConfig);
        brokerConfigOverrides.put(3, oldIbpConfig);

        cluster = new ReassignPartitionsTestCluster(Collections.emptyMap(), brokerConfigOverrides);
        cluster.setup();

        executeAndVerifyReassignment();
    }

    private void executeAndVerifyReassignment() {
        // TODO: fixme.
    }

    class ReassignPartitionsTestCluster implements Closeable {
        private final Map<String, String> configOverrides;

        private final Map<Integer, Map<String, String>> brokerConfigOverrides;

        private final List<KafkaConfig> brokerConfigs = new ArrayList<>();

        private final Map<Integer, String> brokers = new HashMap<>(); {
            brokers.put(0, "rack0");
            brokers.put(1, "rack0");
            brokers.put(2, "rack1");
            brokers.put(3, "rack1");
            brokers.put(4, "rack1");
        }

        private final Map<String, List<List<Integer>>> topics = new HashMap<>(); {
            topics.put("foo", Arrays.asList(Arrays.asList(0, 1, 2), Arrays.asList(1, 2, 3)));
            topics.put("bar", Arrays.asList(Arrays.asList(3, 2, 1)));
            topics.put("baz", Arrays.asList(Arrays.asList(1, 0, 2), Arrays.asList(2, 0, 1), Arrays.asList(0, 2, 1)));
        }

        private List<KafkaBroker> servers = new ArrayList<>();

        private String brokerList;

        private Admin adminClient;

        public ReassignPartitionsTestCluster(Map<String, String> configOverrides, Map<Integer, Map<String, String>> brokerConfigOverrides) {
            this.configOverrides = configOverrides;
            this.brokerConfigOverrides = brokerConfigOverrides;

            brokers.forEach((brokerId, rack) -> {
                Properties config = TestUtils.createBrokerConfig(
                    brokerId,
                    zkConnectOrNull(),
                    false, // shorten test time
                    true,
                    TestUtils.RandomPort(),
                    scala.None$.empty(),
                    scala.None$.empty(),
                    scala.None$.empty(),
                    true,
                    false,
                    TestUtils.RandomPort(),
                    false,
                    TestUtils.RandomPort(),
                    false,
                    TestUtils.RandomPort(),
                    Some$.MODULE$.apply(rack),
                    3,
                    false,
                    1,
                    (short)1,
                    false);
                // shorter backoff to reduce test durations when no active partitions are eligible for fetching due to throttling
                config.setProperty(KafkaConfig.ReplicaFetchBackoffMsProp(), "100");
                // Don't move partition leaders automatically.
                config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp(), "false");
                config.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp(), "1000");
                configOverrides.forEach(config::setProperty);
                brokerConfigOverrides.getOrDefault(brokerId, Collections.emptyMap()).forEach(config::setProperty);

                brokerConfigs.add(new KafkaConfig(config));
            });
        }

        public void setup() throws ExecutionException, InterruptedException {
            createServers();
            createTopics();
        }

        public void createServers() {
            brokers.keySet().forEach(brokerId ->
                servers.add(createBroker(brokerConfigs.get(brokerId), Time.SYSTEM, true, scala.None$.empty()))
            );
        }

        public void createTopics() throws ExecutionException, InterruptedException {
            TestUtils.waitUntilBrokerMetadataIsPropagated(toSeq(servers), org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS);
            brokerList = TestUtils.plaintextBootstrapServers(toSeq(servers));

            adminClient = Admin.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList));

            adminClient.createTopics(topics.entrySet().stream().map(e -> {
                Map<Integer, List<Integer>> partMap = new HashMap<>();

                Iterator<List<Integer>> partsIter = e.getValue().iterator();
                int index = 0;
                while (partsIter.hasNext()) {
                    partMap.put(index, partsIter.next());
                    index++;
                }
                return new NewTopic(e.getKey(), partMap);
            }).collect(Collectors.toList())).all().get();
            topics.forEach((topicName, parts) -> {
                    TestUtils.waitForAllPartitionsMetadata(toSeq(servers), topicName, parts.size());
            });

            if (isKRaftTest()) {
                TestUtils.ensureConsistentKRaftMetadata(
                    toSeq(cluster.servers),
                    controllerServer(),
                    "Timeout waiting for controller metadata propagating to brokers"
                );
            }
        }

        public void produceMessages(String topic, int partition, int numMessages) {
            List<ProducerRecord<byte[], byte[]>> records = IntStream.range(0, numMessages).mapToObj(i ->
                new ProducerRecord<byte[], byte[]>(topic, partition,
                    null, new byte[10000])).collect(Collectors.toList());
            TestUtils.produceMessages(toSeq(servers), toSeq(records), -1);
        }

        @Override
        public void close() throws IOException {
            brokerList = null;
            Utils.closeQuietly(adminClient, "adminClient");
            adminClient = null;
            try {
                TestUtils.shutdownServers(toSeq(servers), true);
            } finally {
                servers.clear();
            }
        }
    }

    private static <T> Seq<T> toSeq(List<T> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }
}
