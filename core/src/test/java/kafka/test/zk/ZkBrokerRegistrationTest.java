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
package kafka.test.zk;

import io.netty.channel.group.ChannelGroup;
import io.netty.util.concurrent.EventExecutor;
import kafka.cluster.Broker;
import kafka.server.KafkaConfig;
import kafka.zk.BrokerIdsZNode;
import kafka.zk.BrokerInfo;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static org.apache.zookeeper.client.ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET;

/**
 * This test simulates the loss of a session ID with Zookeeper (for instance due to network partition)
 * while a broker is in the process of creating its ephemeral znode under /brokers/ids/. This can
 * result in broker registration failing with due to a NODEEXISTS error which is not handled by the
 * fix KAFKA-6584 because in this case, the ID of the session of the conflicting ephemeral znode is
 * not known by the broker. See KAFKA-14845 for an example of timeline of events requires to reproduce
 * the use case.
 */
@Tag("integration")
public class ZkBrokerRegistrationTest {
    private static final Logger log = LoggerFactory.getLogger(ZkBrokerRegistrationTest.class);

    private KafkaConfig kafkaConfig;
    private BrokerInfo brokerInfo;
    private int zkPort;

    @BeforeEach
    public void setup() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            zkPort = socket.getLocalPort();
        }

        Map<Object, Object> config = new HashMap<>();
        config.put("zookeeper.connect", "127.0.0.1:" + zkPort);
        kafkaConfig = new KafkaConfig(config);

        ListenerName listener = ListenerName.forSecurityProtocol(PLAINTEXT);
        Broker broker = new Broker(18, "localhost", 9092, listener, PLAINTEXT);
        brokerInfo = new BrokerInfo(broker, 0, 9999);
    }

    @Test
    public void simulateRegistrationFailure() throws Throwable {
        // The second session is created on the server, but the response not sent to the client.
        ReceiptEvent secondZookeeperSession = new ReceiptEvent(ZooDefs.OpCode.createSession, false);

        Iterator<ReceiptEvent> requestTimeline = asList(
            // (1) First sessions is successfully established and the response sent to the client. Then the path
            //     /brokers/ids is created.
            new ReceiptEvent(ZooDefs.OpCode.createSession, true),

            new ReceiptEvent(ZooDefs.OpCode.create, true), // Attempt to create /brokers/ids, failing.
            new ReceiptEvent(ZooDefs.OpCode.create, true), // Create /brokers.
            new ReceiptEvent(ZooDefs.OpCode.create, true), // Create /brokers/ids.

            // (2) First znode creation for broker registration, which is successful.
            new ReceiptEvent(ZooDefs.OpCode.multi, true),

            // (3) We let the ping timeout, and the reconnection timeout, so the client will have to create a
            // second session, for which we do not send the response.
            secondZookeeperSession,

            // (4) The znode for broker registration is received and processed (the znode created in (2) has been
            // deleted after the session created in (1) expired, so a new znode can be successfully created.).
            new ReceiptEvent(ZooDefs.OpCode.multi, false),

            // (5) After the connection timeout, the client re-attempts to create a new session, which is
            // successful this time.
            new ReceiptEvent(ZooDefs.OpCode.createSession, true),

            // (6) The multi request which failed in (4) is retried, and its response NODEEXISTS is returned
            // to the client.
            new ReceiptEvent(ZooDefs.OpCode.multi, true),

            // (7) The client perfoms a GetData in order to get the owner of the ephemeral znode.
            new ReceiptEvent(ZooDefs.OpCode.getData, true),

            // Provides further multi events for the retries performed by the KafkaZkClient.
            // We may not need all of them, but that is fine.
            new ReceiptEvent(ZooDefs.OpCode.multi, true),
            new ReceiptEvent(ZooDefs.OpCode.getData, true),

            new ReceiptEvent(ZooDefs.OpCode.multi, true),
            new ReceiptEvent(ZooDefs.OpCode.getData, true),

            new ReceiptEvent(ZooDefs.OpCode.multi, true)
        ).iterator();

        Iterator<ConnectionEvent> connectionTimeline = asList(
            new ConnectionEvent(0),
            // The connection timeout is 18 seconds. Make this connection fails so that the session expires.
            new ConnectionEvent(18500),
            new ConnectionEvent(0),
            new ConnectionEvent(0),
            new ConnectionEvent(0),
            new ConnectionEvent(0)
        ).iterator();

        Iterator<ExpirationEvent> sessionExpirationTimeline = asList(
            new ExpirationEvent(0),
            // This allows to slightly delay the processing of the expiration (not the expiration itself) of
            // the second session. This operation is asynchronous and can intrinsically happen before or after
            // the processing of the third multi request. Test run show it can randomly be before or after.
            // Introducing this artificial delay makes the test more deterministic (although it assumed the
            // third multi request won't be delayed by 3 seconds too!).
            new ExpirationEvent(3000),
            new ExpirationEvent(0)
        ).iterator();

        ZkTestContext testContext = new ZkTestContext(
            requestTimeline,
            connectionTimeline,
            sessionExpirationTimeline);

        ServerCnxnFactory cnxnFactory = null;
        KafkaZkClient client = null;

        try {
            // Instantiate and start standalone single-node Zookeeper server.
            cnxnFactory = startZookeeper(testContext);

            System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, "ClientCnxnSocketNetty");

            // Instantiates the Zookeeper client running in Kafka.
            ZooKeeperClient zookeeperClient = new ZooKeeperClient(
                kafkaConfig.zkConnect(),
                kafkaConfig.zkSessionTimeoutMs(),
                kafkaConfig.zkConnectionTimeoutMs(),
                kafkaConfig.zkMaxInFlightRequests(),
                Time.SYSTEM,
                "kafka.server",
                "SessionExpireListener",
                new ZKClientConfig(),
                "ZkClient");

            client = new KafkaZkClient(zookeeperClient, false, Time.SYSTEM);

            client.makeSurePersistentPathExists(BrokerIdsZNode.path());
            client.registerBroker(brokerInfo);

            // Send the multi(14) to create the broker znode only once the second session is created
            // on the server although not acknowledged by the client.
            secondZookeeperSession.awaitProcessed();
            client.registerBroker(brokerInfo);

            // The following errors are expected in the logs, but with the fix KAFKA-14845, retries
            // eventually succeed.
            //
            // ERROR Error while creating ephemeral at /brokers/ids/18, node already exists and
            //       owner '72071046321995776' does not match current session '72071046321995777'
            //       (kafka.zk.KafkaZkClient$CheckedEphemeral)
            //
            // Without the fix, the following exception would be thrown:
            //
            // org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists
            //     at org.apache.zookeeper.KeeperException.create(KeeperException.java:126)
            //     at kafka.zk.KafkaZkClient$CheckedEphemeral.getAfterNodeExists(KafkaZkClient.scala:2185)
            //     at kafka.zk.KafkaZkClient$CheckedEphemeral.create(KafkaZkClient.scala:2123)
            //     at kafka.zk.KafkaZkClient.checkedEphemeralCreate(KafkaZkClient.scala:2090)
            //     at kafka.zk.KafkaZkClient.registerBroker(KafkaZkClient.scala:102)
            //     at kafka.repro.BrokerRegistrationTest.main(BrokerRegistrationTest.java:137)
        } finally {
            testContext.terminate();
            Utils.closeQuietly(client, "KafkaAdminClient");
            stopZookeeper(cnxnFactory);
        }
    }

    private ServerCnxnFactory startZookeeper(ZkTestContext spec) throws Exception {
        Path dataDir = Files.createTempDirectory("zk");

        Properties zkProperties = new Properties();
        zkProperties.put("dataDir", dataDir.toFile().getPath());
        zkProperties.put("clientPort", String.valueOf(zkPort));
        zkProperties.put("serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");

        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(zkProperties);
        FileTxnSnapLog txnLog = new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());

        ZooKeeperServer zookeeper = new InstrumentedZooKeeperServer(
            null,
            txnLog,
            config.getTickTime(),
            config.getMinSessionTimeout(),
            config.getMaxSessionTimeout(),
            config.getClientPortListenBacklog(),
            null,
            config.getInitialConfig(),
            spec);

        ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
        cnxnFactory.configure(
            config.getClientPortAddress(),
            config.getMaxClientCnxns(),
            config.getClientPortListenBacklog(),
            false);

        cnxnFactory.startup(zookeeper);
        return cnxnFactory;
    }

    private void stopZookeeper(ServerCnxnFactory cnxnFactory) {
        log.info("Shutting down sandboxed Zookeeper.");

        try {
            // Zookeeper is not shutting down the Netty event executor which spins a non-daemon thread
            // so we have to manually shut it down.
            if (cnxnFactory != null) {
                Field channelGroupField = cnxnFactory.getClass().getDeclaredField("allChannels");
                channelGroupField.setAccessible(true);
                ChannelGroup channelGroup = (ChannelGroup) channelGroupField.get(cnxnFactory);

                Field executorField = channelGroup.getClass().getDeclaredField("executor");
                executorField.setAccessible(true);
                EventExecutor executor = (EventExecutor) executorField.get(channelGroup);

                executor.shutdownGracefully();
            }
            // This shuts down the Zookeeper server too.
            cnxnFactory.shutdown();

        } catch (Exception e) {
            log.error("Error while shutting down Zookeeper", e);
        }
    }
}
