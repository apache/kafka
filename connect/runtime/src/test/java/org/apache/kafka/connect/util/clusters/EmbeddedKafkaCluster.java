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
package org.apache.kafka.connect.util.clusters;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class EmbeddedKafkaCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);

    // Kafka Config
    private final KafkaServer[] brokers;
    private final Properties brokerConfig;
    private final Time time = new MockTime();

    private EmbeddedZookeeper zookeeper = null;
    private ListenerName listenerName = new ListenerName("PLAINTEXT");

    public EmbeddedKafkaCluster(final int numBrokers,
                                final Properties brokerConfig) {
        brokers = new KafkaServer[numBrokers];
        this.brokerConfig = brokerConfig;
    }

    @Override
    protected void before() throws IOException {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    Time time() {
        return time;
    }

    public void start() throws IOException {
        zookeeper = new EmbeddedZookeeper();

        brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zKConnectString());
        brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), 9999); // pick a random port

        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.HostNameProp(), "localhost");
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
        putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);

        Object listenerConfig = brokerConfig.get(KafkaConfig$.MODULE$.InterBrokerListenerNameProp());
        if (listenerConfig != null) {
            listenerName = new ListenerName(listenerConfig.toString());
        }

        for (int i = 0; i < brokers.length; i++) {
            brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), i);
            brokerConfig.put(KafkaConfig$.MODULE$.LogDirProp(), createLogDir());
            brokers[i] = TestUtils.createServer(new KafkaConfig(brokerConfig, true), time);
        }
    }

    private void stop() {

        for (KafkaServer broker : brokers) {
            try {
                broker.shutdown();
            } catch (Throwable t) {
                log.error("Could not shutdown broker at " + address(broker), t);
            }
        }

        for (KafkaServer broker : brokers) {
            try {
                log.info("Cleaning up kafka log dirs at {}", broker.config().logDirs());
                CoreUtils.delete(broker.config().logDirs());
            } catch (Throwable t) {
                log.error("Could not clean up log dirs for broker " + address(broker), t);
            }
        }

        try {
            zookeeper.shutdown();
        } catch (Throwable t) {
            log.error("Could not shutdown zookeeper at " + zKConnectString(), t);
        }
    }

    private void putIfAbsent(final Properties props, final String propertyKey, final Object propertyValue) {
        if (!props.containsKey(propertyKey)) {
            props.put(propertyKey, propertyValue);
        }
    }

    private String createLogDir() throws IOException {
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        return tmpFolder.newFolder().getAbsolutePath();
    }

    public String bootstrapServers() {
        return address(brokers[0]);
    }

    public String address(KafkaServer server) {
        return server.config().hostName() + ":" + server.boundPort(listenerName);
    }

    public String zKConnectString() {
        return "127.0.0.1:" + zookeeper.port();
    }

}
