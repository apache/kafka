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
package org.apache.kafka.server.config;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.RaftConfig;
import org.apache.kafka.server.ReconfigurableServer;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.ClientMetricsReceiverPlugin;
import org.apache.kafka.server.utils.TestUtils;
import org.apache.kafka.zk.KafkaZKClient;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.raft.RaftConfig.UNKNOWN_ADDRESS_SPEC_INSTANCE;
import static org.apache.kafka.server.common.MetadataVersion.IBP_0_8_2;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_0_IV1;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaConfigTest {
    @Test
    public void testLogRetentionTimeHoursProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LOG_RETENTION_TIME_HOURS_PROP, "1");

        KafkaConfig cfg = fromProps(props);
        assertEquals(60L * 60L * 1000L, cfg.logRetentionTimeMillis());
    }

    @Test
    public void testLogRetentionTimeMinutesProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LOG_RETENTION_TIME_MINUTES_PROP, "30");

        KafkaConfig cfg = fromProps(props);
        assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis());
    }

    @Test
    public void testLogRetentionTimeMsProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP, "1800000");

        KafkaConfig cfg = fromProps(props);
        assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis());
    }

    @Test
    public void testLogRetentionTimeNoConfigProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);

        KafkaConfig cfg = fromProps(props);
        assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRetentionTimeMillis());
    }

    @Test
    public void testLogRetentionTimeBothMinutesAndHoursProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LOG_RETENTION_TIME_MINUTES_PROP, "30");
        props.setProperty(KafkaConfig.LOG_RETENTION_TIME_HOURS_PROP, "1");

        KafkaConfig cfg = fromProps(props);
        assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis());
    }

    @Test
    public void testLogRetentionTimeBothMinutesAndMsProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP, "1800000");
        props.setProperty(KafkaConfig.LOG_RETENTION_TIME_MINUTES_PROP, "10");

        KafkaConfig cfg = fromProps(props);
        assertEquals(30 * 60L * 1000L, cfg.logRetentionTimeMillis());
    }

    @Test
    public void testLogRetentionUnlimited() {
        Properties props1 = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        Properties props2 = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        Properties props3 = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        Properties props4 = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        Properties props5 = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);

        props1.setProperty("log.retention.ms", "-1");
        props2.setProperty("log.retention.minutes", "-1");
        props3.setProperty("log.retention.hours", "-1");

        KafkaConfig cfg1 = fromProps(props1);
        KafkaConfig cfg2 = fromProps(props2);
        KafkaConfig cfg3 = fromProps(props3);
        assertEquals(-1, cfg1.logRetentionTimeMillis(), "Should be -1");
        assertEquals(-1, cfg2.logRetentionTimeMillis(), "Should be -1");
        assertEquals(-1, cfg3.logRetentionTimeMillis(), "Should be -1");

        props4.setProperty("log.retention.ms", "-1");
        props4.setProperty("log.retention.minutes", "30");

        KafkaConfig cfg4 = fromProps(props4);
        assertEquals(-1, cfg4.logRetentionTimeMillis(), "Should be -1");

        props5.setProperty("log.retention.ms", "0");

        assertThrows(IllegalArgumentException.class, () -> fromProps(props5));
    }

    @Test
    public void testLogRetentionValid() {
        Properties props1 = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        Properties props2 = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        Properties props3 = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);

        props1.setProperty("log.retention.ms", "0");
        props2.setProperty("log.retention.minutes", "0");
        props3.setProperty("log.retention.hours", "0");

        assertThrows(IllegalArgumentException.class, () -> fromProps(props1));
        assertThrows(IllegalArgumentException.class, () -> fromProps(props2));
        assertThrows(IllegalArgumentException.class, () -> fromProps(props3));

    }

    @Test
    public void testAdvertiseDefaults() {
        int port = 9999;
        String hostName = "fake-host";
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");
        props.setProperty(KafkaConfig.LISTENERS_PROP, String.format("PLAINTEXT://%s:%s", hostName, port));
        KafkaConfig serverConfig = fromProps(props);

        List<Endpoint> endpoints = serverConfig.effectiveAdvertisedListeners();
        assertEquals(1, endpoints.size());
        Endpoint endpoint = endpoints.stream()
                .filter(e -> e.securityProtocol() == SecurityProtocol.PLAINTEXT)
                .findFirst().get();
        assertEquals(endpoint.host(), hostName);
        assertEquals(endpoint.port(), port);
    }

    @Test
    public void testAdvertiseConfigured() {
        String advertisedHostName = "routable-host";
        int advertisedPort = 1234;

        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT);
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, String.format("PLAINTEXT://%s:%s", advertisedHostName, advertisedPort));

        KafkaConfig serverConfig = fromProps(props);
        List<Endpoint> endpoints = serverConfig.effectiveAdvertisedListeners();
        Endpoint endpoint = endpoints.stream().filter(e -> e.securityProtocol() == SecurityProtocol.PLAINTEXT).findFirst().get();

        assertEquals(endpoint.host(), advertisedHostName);
        assertEquals(endpoint.port(), advertisedPort);
    }

    @Test
    public void testDuplicateListeners() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");

        // listeners with duplicate port
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9091,SSL://localhost:9091");
        assertBadConfigContainingMessage(props, "Each listener must have a different port");

        // listeners with duplicate name
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9091,PLAINTEXT://localhost:9092");
        assertBadConfigContainingMessage(props, "Each listener must have a different name");

        // advertised listeners can have duplicate ports
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "HOST:SASL_SSL,LB:SASL_SSL");
        props.setProperty(KafkaConfig.INTER_BROKER_LISTENER_NAME_PROP, "HOST");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "HOST://localhost:9091,LB://localhost:9092");
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, "HOST://localhost:9091,LB://localhost:9091");
        fromProps(props);

        // but not duplicate names
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, "HOST://localhost:9091,HOST://localhost:9091");
        assertBadConfigContainingMessage(props, "Each listener must have a different name");
    }

    @Test
    public void testIPv4AndIPv6SamePortListeners() {
        Properties props = new Properties();
        props.put(KafkaConfig.BROKER_ID_PROP, "1");
        props.put(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");

        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://[::1]:9092,SSL://[::1]:9092");
        IllegalArgumentException caught = assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        assertTrue(caught.getMessage().contains("If you have two listeners on the same port then one needs to be IPv4 and the other IPv6"));

        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9092");
        caught = assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        assertTrue(caught.getMessage().contains("If you have two listeners on the same port then one needs to be IPv4 and the other IPv6"));

        props.put(KafkaConfig.LISTENERS_PROP, "SSL://[::1]:9096,PLAINTEXT://127.0.0.1:9096,SASL_SSL://:9096");
        caught = assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        assertTrue(caught.getMessage().contains("If you have two listeners on the same port then one needs to be IPv4 and the other IPv6"));

        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://127.0.0.1:9092,PLAINTEXT://127.0.0.1:9092");
        caught = assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        assertTrue(caught.getMessage().contains("Each listener must have a different name"));

        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9092,SASL_SSL://127.0.0.1:9092");
        caught = assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        assertTrue(caught.getMessage().contains("Each listener must have a different port"));

        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://apache.org:9092,SSL://[::1]:9092");
        caught = assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        assertTrue(caught.getMessage().contains("Each listener must have a different port"));

        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://apache.org:9092,SSL://127.0.0.1:9092");
        caught = assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        assertTrue(caught.getMessage().contains("Each listener must have a different port"));

        // Happy case
        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092");
        assertTrue(isValidKafkaConfig(props));
        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://[::1]:9093,SSL://127.0.0.1:9093");
        assertTrue(isValidKafkaConfig(props));
        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://127.0.0.1:9094,SSL://[::1]:9094,SASL_SSL://127.0.0.1:9095,SASL_PLAINTEXT://[::1]:9095");
        assertTrue(isValidKafkaConfig(props));
        props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://[::1]:9096,SSL://127.0.0.1:9096,SASL_SSL://[::1]:9097,SASL_PLAINTEXT://127.0.0.1:9097");
        assertTrue(isValidKafkaConfig(props));
    }

    @Test
    public void testControlPlaneListenerName() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT);
        props.setProperty("listeners", "PLAINTEXT://localhost:0,CONTROLLER://localhost:5000");
        props.setProperty("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL");
        props.setProperty("control.plane.listener.name", "CONTROLLER");
        fromProps(props);

        KafkaConfig serverConfig = fromProps(props);
        Endpoint controlEndpoint = serverConfig.controlPlaneListener().get();
        assertEquals("localhost", controlEndpoint.host());
        assertEquals(5000, controlEndpoint.port());
        assertEquals(SecurityProtocol.SSL, controlEndpoint.securityProtocol());

        //advertised listener should contain control-plane listener
        List<Endpoint> advertisedEndpoints = serverConfig.effectiveAdvertisedListeners();
        assertTrue(advertisedEndpoints.stream()
                .anyMatch(e -> e.securityProtocol() == controlEndpoint.securityProtocol() && e.listenerName().get().equals(controlEndpoint.listenerName().get()))
        );

        // interBrokerListener name should be different from control-plane listener name
        ListenerName interBrokerListenerName = serverConfig.interBrokerListenerName();
        assertFalse(interBrokerListenerName.value().equals(controlEndpoint.listenerName().get()));
    }

    @Test
    public void testControllerListenerNames() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker,controller");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:0,CONTROLLER://localhost:5000");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:5000");
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "PLAINTEXT:PLAINTEXT,CONTROLLER:SASL_SSL");

        KafkaConfig serverConfig = fromProps(props);
        List<Endpoint> controllerEndpoints = serverConfig.controllerListeners();
        assertEquals(1, controllerEndpoints.size());
        Endpoint controllerEndpoint = controllerEndpoints.iterator().next();
        assertEquals("localhost", controllerEndpoint.host());
        assertEquals(5000, controllerEndpoint.port());
        assertEquals(SecurityProtocol.SASL_SSL, controllerEndpoint.securityProtocol());
    }

    @Test
    public void testControlPlaneListenerNameNotAllowedWithKRaft() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker,controller");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092,SSL://localhost:9093");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        props.setProperty(KafkaConfig.CONTROL_PLANE_LISTENER_NAME_PROP, "SSL");

        assertFalse(isValidKafkaConfig(props));
        assertBadConfigContainingMessage(props, "control.plane.listener.name is not supported in KRaft mode.");

        props.remove(KafkaConfig.CONTROL_PLANE_LISTENER_NAME_PROP);
        fromProps(props);
    }

    @Test
    public void testControllerListenerDefinedForKRaftController() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "controller");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "SSL://localhost:9093");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");

        assertBadConfigContainingMessage(props, "The listeners config must only contain KRaft controller listeners from controller.listener.names when process.roles=controller");

        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        fromProps(props);

        // confirm that redirecting via listener.security.protocol.map is acceptable
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "CONTROLLER:SSL");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "CONTROLLER://localhost:9093");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        fromProps(props);
    }

    @Test
    public void testControllerListenerDefinedForKRaftBroker() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "1");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");

        assertFalse(isValidKafkaConfig(props));
        assertBadConfigContainingMessage(props, "controller.listener.names must contain at least one value when running KRaft with just the broker role");

        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        fromProps(props);

        // confirm that redirecting via listener.security.protocol.map is acceptable
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        fromProps(props);
    }

    @Test
    public void testPortInQuorumVotersNotRequiredToMatchFirstControllerListenerPortForThisKRaftController() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "controller,broker");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093,3@anotherhost:9094");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL,SASL_SSL");
        fromProps(props);

        // change each of the 4 ports to port 5555 -- should pass in all circumstances since we can't validate the
        // controller.quorum.voters ports (which are the ports that clients use and are semantically "advertised" ports
        // even though the controller configuration doesn't list them in advertised.listeners) against the
        // listener ports (which are semantically different then the ports that clients use).
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092,SSL://localhost:5555,SASL_SSL://localhost:9094");
        fromProps(props);
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:5555");
        fromProps(props);
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094");
        // reset to original value
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:5555,3@anotherhost:9094");
        fromProps(props);
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093,3@anotherhost:5555");
        fromProps(props);
    }

    @Test
    public void testSeparateControllerListenerDefinedForKRaftBrokerController() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker,controller");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "SSL://localhost:9093");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");

        assertFalse(isValidKafkaConfig(props));
        assertBadConfigContainingMessage(props, "There must be at least one advertised listener. Perhaps all listeners appear in controller.listener.names?");

        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092,SSL://localhost:9093");
        fromProps(props);

        // confirm that redirecting via listener.security.protocol.map is acceptable
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093");
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        fromProps(props);
    }

    @Test
    public void testControllerListenerNameMapsToPlaintextByDefaultForKRaft() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "1");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        ListenerName controllerListenerName = new ListenerName("CONTROLLER");
        assertEquals(SecurityProtocol.PLAINTEXT,
                fromProps(props).effectiveListenerSecurityProtocolMap().get(controllerListenerName));
        // ensure we don't map it to PLAINTEXT when there is a SSL or SASL controller listener
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER,SSL");
        String controllerNotFoundInMapMessage = "Controller listener with name CONTROLLER defined in controller.listener.names not found in listener.security.protocol.map";
        assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage);
        // ensure we don't map it to PLAINTEXT when there is a SSL or SASL listener
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "SSL://localhost:9092");
        assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage);
        props.remove(KafkaConfig.LISTENERS_PROP);
        // ensure we don't map it to PLAINTEXT when it is explicitly mapped otherwise
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "PLAINTEXT:PLAINTEXT,CONTROLLER:SSL");
        assertEquals(SecurityProtocol.SSL,
                fromProps(props).effectiveListenerSecurityProtocolMap().get(controllerListenerName));
        // ensure we don't map it to PLAINTEXT when anything is explicitly given
        // (i.e. it is only part of the default value, even with KRaft)
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "PLAINTEXT:PLAINTEXT");
        assertBadConfigContainingMessage(props, controllerNotFoundInMapMessage);
        // ensure we can map it to a non-PLAINTEXT security protocol by default (i.e. when nothing is given)
        props.remove(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP);
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        assertEquals(SecurityProtocol.SSL,
                fromProps(props).effectiveListenerSecurityProtocolMap().get(new ListenerName("SSL")));
    }

    @Test
    public void testMultipleControllerListenerNamesMapToPlaintextByDefaultForKRaft() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "controller");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "CONTROLLER1://localhost:9092,CONTROLLER2://localhost:9093");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER1,CONTROLLER2");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "1");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "1@localhost:9092");
        assertEquals(SecurityProtocol.PLAINTEXT,
                fromProps(props).effectiveListenerSecurityProtocolMap().get(new ListenerName("CONTROLLER1")));
        assertEquals(SecurityProtocol.PLAINTEXT,
                fromProps(props).effectiveListenerSecurityProtocolMap().get(new ListenerName("CONTROLLER2")));
    }

    @Test
    public void testControllerListenerNameDoesNotMapToPlaintextByDefaultForNonKRaft() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "CONTROLLER://localhost:9092");
        assertBadConfigContainingMessage(props,
                "Error creating broker listeners from 'CONTROLLER://localhost:9092': No security protocol defined for listener CONTROLLER");
        // Valid now
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092");
        assertNull(fromProps(props).effectiveListenerSecurityProtocolMap().get(new ListenerName("CONTROLLER")));
    }

    @Test
    public void testBadListenerProtocol() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "BAD://localhost:9091");

        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testListenerNamesWithAdvertisedListenerUnset() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");

        props.setProperty(KafkaConfig.LISTENERS_PROP, "CLIENT://localhost:9091,REPLICATION://localhost:9092,INTERNAL://localhost:9093");
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "CLIENT:SSL,REPLICATION:SSL,INTERNAL:PLAINTEXT");
        props.setProperty(KafkaConfig.INTER_BROKER_LISTENER_NAME_PROP, "REPLICATION");
        KafkaConfig config = fromProps(props);
        List<Endpoint> expectedListeners = Arrays.asList(
                new Endpoint("CLIENT", SecurityProtocol.SSL, "localhost", 9091),
                new Endpoint("REPLICATION", SecurityProtocol.SSL, "localhost", 9092),
                new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9093));
        assertEquals(expectedListeners, config.listeners());
        assertEquals(expectedListeners, config.effectiveAdvertisedListeners());
        Map<ListenerName, SecurityProtocol> expectedSecurityProtocolMap = Utils.mkMap(
                Utils.mkEntry(new ListenerName("CLIENT"), SecurityProtocol.SSL),
                Utils.mkEntry(new ListenerName("REPLICATION"), SecurityProtocol.SSL),
                Utils.mkEntry(new ListenerName("INTERNAL"), SecurityProtocol.PLAINTEXT));
        assertEquals(expectedSecurityProtocolMap, config.effectiveListenerSecurityProtocolMap());
    }

    @Test
    public void testListenerAndAdvertisedListenerNames() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");

        props.setProperty(KafkaConfig.LISTENERS_PROP, "EXTERNAL://localhost:9091,INTERNAL://localhost:9093");
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, "EXTERNAL://lb1.example.com:9000,INTERNAL://host1:9093");
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "EXTERNAL:SSL,INTERNAL:PLAINTEXT");
        props.setProperty(KafkaConfig.INTER_BROKER_LISTENER_NAME_PROP, "INTERNAL");
        KafkaConfig config = fromProps(props);

        List<Endpoint> expectedListeners = Arrays.asList(
                new Endpoint("EXTERNAL", SecurityProtocol.SSL, "localhost", 9091),
                new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "localhost", 9093));
        assertEquals(expectedListeners, config.listeners());

        List<Endpoint>  expectedAdvertisedListeners = Arrays.asList(
                new Endpoint("EXTERNAL", SecurityProtocol.SSL, "lb1.example.com", 9000),
                new Endpoint("INTERNAL", SecurityProtocol.PLAINTEXT, "host1", 9093));
        assertEquals(expectedAdvertisedListeners, config.effectiveAdvertisedListeners());

        Map<ListenerName, SecurityProtocol> expectedSecurityProtocolMap = Utils.mkMap(
                Utils.mkEntry(new ListenerName("EXTERNAL"), SecurityProtocol.SSL),
                Utils.mkEntry(new ListenerName("INTERNAL"), SecurityProtocol.PLAINTEXT));
        assertEquals(expectedSecurityProtocolMap, config.effectiveListenerSecurityProtocolMap());
    }

    @Test
    public void testListenerNameMissingFromListenerSecurityProtocolMap() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");

        props.setProperty(KafkaConfig.LISTENERS_PROP, "SSL://localhost:9091,REPLICATION://localhost:9092");
        props.setProperty(KafkaConfig.INTER_BROKER_LISTENER_NAME_PROP, "SSL");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testInterBrokerListenerNameMissingFromListenerSecurityProtocolMap() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");

        props.setProperty(KafkaConfig.LISTENERS_PROP, "SSL://localhost:9091");
        props.setProperty(KafkaConfig.INTER_BROKER_LISTENER_NAME_PROP, "REPLICATION");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testInterBrokerListenerNameAndSecurityProtocolSet() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");

        props.setProperty(KafkaConfig.LISTENERS_PROP, "SSL://localhost:9091");
        props.setProperty(KafkaConfig.INTER_BROKER_LISTENER_NAME_PROP, "SSL");
        props.setProperty(KafkaConfig.INTER_BROKER_SECURITY_PROTOCOL_PROP, "SSL");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testCaseInsensitiveListenerProtocol() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "plaintext://localhost:9091,SsL://localhost:9092");
        KafkaConfig config = fromProps(props);
        Map<String, String> listenerNameToConnectionString = config.listeners().stream()
                .collect(Collectors.toMap(e -> e.listenerName().get().toUpperCase(Locale.ROOT), e -> e.connectionString()));

        assertEquals("SSL://localhost:9092", listenerNameToConnectionString.get("SSL"));
        assertEquals("PLAINTEXT://localhost:9091", listenerNameToConnectionString.get("PLAINTEXT"));
    }

    @Test
    public void testListenerDefaults() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");

        // configuration with no listeners
        KafkaConfig conf = fromProps(props);
        assertEquals(listenerListToEndpoints("PLAINTEXT://:9092"), conf.listeners());
        assertNull(conf.listeners().stream().filter(e -> e.securityProtocol() == SecurityProtocol.PLAINTEXT).findFirst().get().host());
        assertEquals(conf.effectiveAdvertisedListeners(), listenerListToEndpoints("PLAINTEXT://:9092"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testVersionConfiguration() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");
        KafkaConfig conf = fromProps(props);
        assertEquals(MetadataVersion.latest(), conf.interBrokerProtocolVersion());

        props.setProperty(KafkaConfig.INTER_BROKER_PROTOCOL_VERSION_PROP, "0.8.2.0");
        // We need to set the message format version to make the configuration valid.
        props.setProperty(KafkaConfig.LOG_MESSAGE_FORMAT_VERSION_PROP, "0.8.2.0");
        KafkaConfig conf2 = fromProps(props);
        assertEquals(IBP_0_8_2, conf2.interBrokerProtocolVersion());

        // check that 0.8.2.0 is the same as 0.8.2.1
        props.setProperty(KafkaConfig.INTER_BROKER_PROTOCOL_VERSION_PROP, "0.8.2.1");
        // We need to set the message format version to make the configuration valid
        props.setProperty(KafkaConfig.LOG_MESSAGE_FORMAT_VERSION_PROP, "0.8.2.1");
        KafkaConfig conf3 = fromProps(props);
        assertEquals(IBP_0_8_2, conf3.interBrokerProtocolVersion());

        //check that latest is newer than 0.8.2
        assertTrue(MetadataVersion.latest().isAtLeast(conf3.interBrokerProtocolVersion()));
    }

    @Test
    public void testUncleanLeaderElectionDefault() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        KafkaConfig serverConfig = fromProps(props);

        assertEquals(serverConfig.uncleanLeaderElectionEnable(), false);
    }

    @Test
    public void testUncleanElectionDisabled() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.UNCLEAN_LEADER_ELECTION_ENABLE_PROP, String.valueOf(false));
        KafkaConfig serverConfig = fromProps(props);

        assertEquals(serverConfig.uncleanLeaderElectionEnable(), false);
    }

    @Test
    public void testUncleanElectionEnabled() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.UNCLEAN_LEADER_ELECTION_ENABLE_PROP, String.valueOf(true));
        KafkaConfig serverConfig = fromProps(props);

        assertEquals(serverConfig.uncleanLeaderElectionEnable(), true);
    }

    @Test
    public void testUncleanElectionInvalid() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.UNCLEAN_LEADER_ELECTION_ENABLE_PROP, "invalid");

        assertThrows(ConfigException.class, () -> fromProps(props));
    }

    @Test
    public void testLogRollTimeMsProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP, "1800000");

        KafkaConfig cfg = fromProps(props);
        assertEquals(30 * 60L * 1000L, cfg.logRollTimeMillis());
    }

    @Test
    public void testLogRollTimeBothMsAndHoursProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP, "1800000");
        props.setProperty(KafkaConfig.LOG_ROLL_TIME_HOURS_PROP, "1");

        KafkaConfig cfg = fromProps(props);
        assertEquals(30 * 60L * 1000L, cfg.logRollTimeMillis());
    }

    @Test
    public void testLogRollTimeNoConfigProvided() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);

        KafkaConfig cfg = fromProps(props);
        assertEquals(24 * 7 * 60L * 60L * 1000L, cfg.logRollTimeMillis());
    }

    @Test
    public void testDefaultCompressionType() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        KafkaConfig serverConfig = fromProps(props);

        assertEquals(serverConfig.compressionType(), "producer");
    }

    @Test
    public void testValidCompressionType() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty("compression.type", "gzip");
        KafkaConfig serverConfig = fromProps(props);

        assertEquals(serverConfig.compressionType(), "gzip");
    }

    @Test
    public void testInvalidCompressionType() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.COMPRESSION_TYPE_PROP, "abc");
        assertThrows(ConfigException.class, () -> fromProps(props));
    }

    @Test
    public void testInvalidInterBrokerSecurityProtocol() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LISTENERS_PROP, "SSL://localhost:0");
        props.setProperty(KafkaConfig.INTER_BROKER_SECURITY_PROTOCOL_PROP, SecurityProtocol.PLAINTEXT.toString());
        assertThrows(IllegalArgumentException.class, () -> fromProps(props));
    }

    @Test
    public void testEqualAdvertisedListenersProtocol() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092,SSL://localhost:9093");
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, "PLAINTEXT://localhost:9092,SSL://localhost:9093");
        fromProps(props);
    }

    @Test
    public void testInvalidAdvertisedListenersProtocol() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.LISTENERS_PROP, "TRACE://localhost:9091,SSL://localhost:9093");
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, "PLAINTEXT://localhost:9092");
        assertBadConfigContainingMessage(props, "No security protocol defined for listener TRACE");

        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, "PLAINTEXT:PLAINTEXT,TRACE:PLAINTEXT,SSL:SSL");
        assertBadConfigContainingMessage(props, "advertised.listeners listener names must be equal to or a subset of the ones defined in listeners");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testInterBrokerVersionMessageFormatCompatibility() {
        Stream.of(MetadataVersion.VERSIONS).forEach(interBrokerVersion -> Stream.of(MetadataVersion.VERSIONS).forEach(messageFormatVersion -> {
            if (interBrokerVersion.highestSupportedRecordVersion().value >= messageFormatVersion.highestSupportedRecordVersion().value) {
                KafkaConfig config = buildConfig(interBrokerVersion, messageFormatVersion);
                assertEquals(interBrokerVersion, config.interBrokerProtocolVersion());
                if (interBrokerVersion.isAtLeast(IBP_3_0_IV1)) {
                    assertEquals(IBP_3_0_IV1, config.logMessageFormatVersion());
                } else {
                    assertEquals(messageFormatVersion, config.logMessageFormatVersion());
                }
            } else {
                assertThrows(IllegalArgumentException.class, () -> buildConfig(interBrokerVersion, messageFormatVersion));
            }
        }));
    }

    @SuppressWarnings("deprecation")
    KafkaConfig buildConfig(MetadataVersion interBrokerProtocol, MetadataVersion messageFormat) {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.INTER_BROKER_PROTOCOL_VERSION_PROP, interBrokerProtocol.version());
        props.setProperty(KafkaConfig.LOG_MESSAGE_FORMAT_VERSION_PROP, messageFormat.version());
        return fromProps(props);
    }

    private List<String> configNamesWithoutIgnoredConfigs() {
        List<String> configNames = new ArrayList<>(KafkaConfig.configNames());
        List<String> ignoredConfigs = Arrays.asList(
                KafkaConfig.ZK_CONNECT_PROP,
                KafkaConfig.ZK_CLIENT_CNXN_SOCKET_PROP,
                KafkaConfig.ZK_SSL_KEY_STORE_LOCATION_PROP,
                KafkaConfig.ZK_SSL_KEY_STORE_PASSWORD_PROP,
                KafkaConfig.ZK_SSL_KEY_STORE_TYPE_PROP,
                KafkaConfig.ZK_SSL_TRUST_STORE_LOCATION_PROP,
                KafkaConfig.ZK_SSL_TRUST_STORE_PASSWORD_PROP,
                KafkaConfig.ZK_SSL_TRUST_STORE_TYPE_PROP,
                KafkaConfig.ZK_SSL_PROTOCOL_PROP,
                KafkaConfig.ZK_SSL_ENABLED_PROTOCOLS_PROP,
                KafkaConfig.ZK_SSL_CIPHER_SUITES_PROP,
                KafkaConfig.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP,
                KafkaConfig.PROCESS_ROLES_PROP,
                KafkaConfig.METADATA_LOG_DIR_PROP,
                KafkaConfig.AUTHORIZER_CLASS_NAME_PROP,
                KafkaConfig.CREATE_TOPIC_POLICY_CLASS_NAME_PROP,
                KafkaConfig.PASSWORD_ENCODER_SECRET_PROP,
                KafkaConfig.PASSWORD_ENCODER_OLD_SECRET_PROP,
                KafkaConfig.PASSWORD_ENCODER_KEY_FACTORY_ALGORITHM_PROP,
                KafkaConfig.PASSWORD_ENCODER_CIPHER_ALGORITHM_PROP,
                KafkaConfig.METRIC_REPORTER_CLASSES_PROP,
                KafkaConfig.METRIC_RECORDING_LEVEL_PROP,
                KafkaConfig.REPLICA_SELECTOR_CLASS_PROP,
                KafkaConfig.LOG_DIRS_PROP,
                KafkaConfig.LOG_DIR_PROP,
                KafkaConfig.RACK_PROP,
                KafkaConfig.PRINCIPAL_BUILDER_CLASS_PROP,
                KafkaConfig.CONNECTIONS_MAX_REAUTH_MS_PROP,
                KafkaConfig.SSL_PROTOCOL_PROP,
                KafkaConfig.SSL_PROVIDER_PROP,
                KafkaConfig.SSL_ENABLED_PROTOCOLS_PROP,
                KafkaConfig.SSL_KEYSTORE_TYPE_PROP,
                KafkaConfig.SSL_KEYSTORE_LOCATION_PROP,
                KafkaConfig.SSL_KEYSTORE_PASSWORD_PROP,
                KafkaConfig.SSL_KEY_PASSWORD_PROP,
                KafkaConfig.SSL_KEYSTORE_CERTIFICATE_CHAIN_PROP,
                KafkaConfig.SSL_KEYSTORE_KEY_PROP,
                KafkaConfig.SSL_TRUSTSTORE_TYPE_PROP,
                KafkaConfig.SSL_TRUSTSTORE_PASSWORD_PROP,
                KafkaConfig.SSL_TRUSTSTORE_LOCATION_PROP,
                KafkaConfig.SSL_TRUSTSTORE_CERTIFICATES_PROP,
                KafkaConfig.SSL_KEY_MANAGER_ALGORITHM_PROP,
                KafkaConfig.SSL_TRUST_MANAGER_ALGORITHM_PROP,
                KafkaConfig.SSL_CLIENT_AUTH_PROP,
                KafkaConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_PROP,
                KafkaConfig.SSL_SECURE_RANDOM_IMPLEMENTATION_PROP,
                KafkaConfig.SSL_CIPHER_SUITES_PROP,
                KafkaConfig.SSL_PRINCIPAL_MAPPING_RULES_PROP,
                KafkaConfig.SASL_MECHANISM_CONTROLLER_PROTOCOL_PROP,
                KafkaConfig.SASL_MECHANISM_INTER_BROKER_PROTOCOL_PROP,
                KafkaConfig.SASL_ENABLED_MECHANISMS_PROP,
                KafkaConfig.SASL_CLIENT_CALLBACK_HANDLER_CLASS_PROP,
                KafkaConfig.SASL_SERVER_CALLBACK_HANDLER_CLASS_PROP,
                KafkaConfig.SASL_LOGIN_CLASS_PROP,
                KafkaConfig.SASL_LOGIN_CALLBACK_HANDLER_CLASS_PROP,
                KafkaConfig.SASL_KERBEROS_SERVICE_NAME_PROP,
                KafkaConfig.SASL_KERBEROS_KINIT_CMD_PROP,
                KafkaConfig.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_PROP,
                KafkaConfig.SASL_KERBEROS_TICKET_RENEW_JITTER_PROP,
                KafkaConfig.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_PROP,
                KafkaConfig.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_PROP,
                KafkaConfig.SASL_JAAS_CONFIG_PROP,
                KafkaConfig.SASL_LOGIN_REFRESH_WINDOW_FACTOR_PROP,
                KafkaConfig.SASL_LOGIN_REFRESH_WINDOW_JITTER_PROP,
                KafkaConfig.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_PROP,
                KafkaConfig.SASL_LOGIN_REFRESH_BUFFER_SECONDS_PROP,
                KafkaConfig.SASL_LOGIN_CONNECT_TIMEOUT_MS_PROP,
                KafkaConfig.SASL_LOGIN_READ_TIMEOUT_MS_PROP,
                KafkaConfig.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_PROP,
                KafkaConfig.SASL_LOGIN_RETRY_BACKOFF_MS_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_SCOPE_CLAIM_NAME_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_SUB_CLAIM_NAME_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_TOKEN_ENDPOINT_URL_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_JWKS_ENDPOINT_URL_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_JWKS_ENDPOINT_REFRESH_MS_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_CLOCK_SKEW_SECONDS_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_EXPECTED_AUDIENCE_PROP,
                KafkaConfig.SASL_O_AUTH_BEARER_EXPECTED_ISSUER_PROP,
                KafkaConfig.SECURITY_PROVIDER_CLASS_PROP,
                KafkaConfig.DELEGATION_TOKEN_SECRET_KEY_ALIAS_PROP,
                KafkaConfig.DELEGATION_TOKEN_SECRET_KEY_PROP,
                RaftConfig.QUORUM_VOTERS_CONFIG,
                RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
                RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_PATH_PROP,
                RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_PATH_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP,
                RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
                KafkaConfig.KAFKA_METRICS_REPORTER_CLASSES_PROP,
                KafkaConfig.KAFKA_METRICS_POLLING_INTERVAL_SECONDS_PROP,
                KafkaConfig.NEW_GROUP_COORDINATOR_ENABLE_PROP,
                KafkaConfig.CONSUMER_GROUP_ASSIGNORS_PROP,
                KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP
                );
        configNames.removeAll(ignoredConfigs);
        return configNames;
    }

    @SuppressWarnings("deprecation")
    private boolean validateLogConfig(String name) {
        if (Arrays.asList(
                KafkaConfig.LOG_RETENTION_BYTES_PROP,
                KafkaConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP,
                KafkaConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP,
                KafkaConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP,
                KafkaConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP,
                KafkaConfig.LOG_INDEX_SIZE_MAX_BYTES_PROP,
                KafkaConfig.LOG_FLUSH_INTERVAL_MESSAGES_PROP,
                KafkaConfig.LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP,
                KafkaConfig.LOG_FLUSH_INTERVAL_MS_PROP,
                KafkaConfig.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_PROP,
                KafkaConfig.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_PROP,
                KafkaConfig.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_PROP,
                KafkaConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP,
                KafkaConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP,
                KafkaConfig.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number");
            return true;
        }
        if (Arrays.asList(
                KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP,
                KafkaConfig.LOG_ROLL_TIME_MILLIS_PROP,
                KafkaConfig.MIN_IN_SYNC_REPLICAS_PROP,
                KafkaConfig.LOG_ROLL_TIME_HOURS_PROP,
                KafkaConfig.LOG_RETENTION_TIME_MINUTES_PROP,
                KafkaConfig.LOG_RETENTION_TIME_HOURS_PROP,
                KafkaConfig.LOG_CLEANUP_INTERVAL_MS_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "0");
            return true;
        }

        if (KafkaConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "1024");
            return true;
        }
        if (KafkaConfig.LOG_CLEANER_ENABLE_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_boolean");
            return true;
        }
        if (KafkaConfig.LOG_SEGMENT_BYTES_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", Records.LOG_OVERHEAD - 1);
            return true;
        }

        if (KafkaConfig.LOG_CLEANUP_POLICY_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "unknown_policy", "0");
            return true;
        }
        return false;
    }

    private boolean validateControlledShutdownConfig(String name) {
        if (KafkaConfig.CONTROLLED_SHUTDOWN_ENABLE_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_boolean", "0");
            return true;
        }
        if (Arrays.asList(KafkaConfig.CONTROLLER_SOCKET_TIMEOUT_MS_PROP, KafkaConfig.CONTROLLED_SHUTDOWN_MAX_RETRIES_PROP,
                KafkaConfig.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number");
            return true;
        }
        return false;
    }
    private boolean validateBasicConfig(String name) {
        if (Arrays.asList(
                KafkaConfig.ZK_SESSION_TIMEOUT_MS_PROP, KafkaConfig.ZK_CONNECTION_TIMEOUT_MS_PROP,
                KafkaConfig.BROKER_ID_PROP, KafkaConfig.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_PROP,
                KafkaConfig.QUEUED_MAX_BYTES_PROP, KafkaConfig.REQUEST_TIMEOUT_MS_PROP, KafkaConfig.CONNECTION_SETUP_TIMEOUT_MS_PROP,
                KafkaConfig.CONNECTION_SETUP_TIMEOUT_MAX_MS_PROP, KafkaConfig.SOCKET_SEND_BUFFER_BYTES_PROP,
                KafkaConfig.SOCKET_RECEIVE_BUFFER_BYTES_PROP, KafkaConfig.SOCKET_LISTEN_BACKLOG_SIZE_PROP,
                KafkaConfig.CONNECTIONS_MAX_IDLE_MS_PROP, KafkaConfig.SASL_SERVER_MAX_RECEIVE_SIZE_PROP,
                KafkaConfig.FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP, KafkaConfig.PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP,
                KafkaConfig.DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_PROP,
                KafkaConfig.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_PROP,
                KafkaConfig.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number");
            return true;
        }
        if (Arrays.asList(KafkaConfig.NUM_PARTITIONS_PROP, KafkaConfig.ZK_MAX_IN_FLIGHT_REQUESTS_PROP,
                KafkaConfig.NUM_NETWORK_THREADS_PROP, KafkaConfig.NUM_IO_THREADS_PROP, KafkaConfig.BACKGROUND_THREADS_PROP,
                KafkaConfig.QUEUED_MAX_REQUESTS_PROP, KafkaConfig.NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP,
                KafkaConfig.NUM_QUOTA_SAMPLES_PROP, KafkaConfig.QUOTA_WINDOW_SIZE_SECONDS_PROP,
                KafkaConfig.DELEGATION_TOKEN_MAX_LIFE_TIME_PROP, KafkaConfig.DELEGATION_TOKEN_EXPIRY_TIME_MS_PROP,
                KafkaConfig.DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "0");
            return true;
        }

        if (Arrays.asList(KafkaConfig.METRIC_NUM_SAMPLES_PROP, KafkaConfig.METRIC_SAMPLE_WINDOW_MS_PROP,
                KafkaConfig.PASSWORD_ENCODER_KEY_LENGTH_PROP, KafkaConfig.PASSWORD_ENCODER_ITERATIONS_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "-1", "0");
            return true;
        }

        if (Arrays.asList(KafkaConfig.UNCLEAN_LEADER_ELECTION_ENABLE_PROP, KafkaConfig.AUTO_LEADER_REBALANCE_ENABLE_PROP,
                KafkaConfig.AUTO_CREATE_TOPICS_ENABLE_PROP, KafkaConfig.DELETE_TOPIC_ENABLE_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_boolean", "0");
            return true;
        }

        if (KafkaConfig.MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "127.0.0.1:not_a_number");
            return true;
        }
        if (KafkaConfig.FAILED_AUTHENTICATION_DELAY_MS_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "-1");
            return true;
        }
        return false;
    }

    private boolean validateKraftConfig(String name) {
        if (Arrays.asList(KafkaConfig.ZK_ENABLE_SECURE_ACLS_PROP, KafkaConfig.ZK_SSL_CLIENT_ENABLE_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_boolean");
            return true;
        }
        if (Arrays.asList(
                KafkaConfig.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_PROP,
                KafkaConfig.BROKER_HEARTBEAT_INTERVAL_MS_PROP,
                KafkaConfig.BROKER_SESSION_TIMEOUT_MS_PROP,
                KafkaConfig.NODE_ID_PROP,
                KafkaConfig.METADATA_LOG_SEGMENT_BYTES_PROP,
                KafkaConfig.METADATA_LOG_SEGMENT_MILLIS_PROP,
                KafkaConfig.METADATA_MAX_RETENTION_BYTES_PROP,
                KafkaConfig.METADATA_MAX_RETENTION_MILLIS_PROP,
                KafkaConfig.METADATA_MAX_IDLE_INTERVAL_MS_PROP,
                // Raft Quorum Configs
                RaftConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG,
                RaftConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG,
                RaftConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG,
                RaftConfig.QUORUM_LINGER_MS_CONFIG,
                RaftConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG,
                RaftConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number");
            return true;
        }
        return false;
    }

    private boolean validateConsumerGroupAndMetadataConfig(String name) {
        if (Arrays.asList(KafkaConfig.GROUP_MIN_SESSION_TIMEOUT_MS_PROP, KafkaConfig.GROUP_MAX_SESSION_TIMEOUT_MS_PROP,
                KafkaConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_PROP, KafkaConfig.OFFSET_METADATA_MAX_SIZE_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number");
            return true;
        }
        if (KafkaConfig.GROUP_MAX_SIZE_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "0", "-1");
            return true;
        }
        if (KafkaConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "-1");
            return true;
        }
        if (Arrays.asList(KafkaConfig.OFFSETS_LOAD_BUFFER_SIZE_PROP, KafkaConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_PROP,
                KafkaConfig.OFFSETS_TOPIC_PARTITIONS_PROP, KafkaConfig.OFFSETS_TOPIC_SEGMENT_BYTES_PROP,
                KafkaConfig.OFFSETS_RETENTION_MINUTES_PROP, KafkaConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_PROP,
                KafkaConfig.OFFSET_COMMIT_TIMEOUT_MS_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "0");
            return true;
        }

        if (KafkaConfig.OFFSET_COMMIT_REQUIRED_ACKS_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "-2");
            return true;
        }
        /** New group coordinator configs */
        if (Arrays.asList(KafkaConfig.GROUP_COORDINATOR_NUM_THREADS_PROP, KafkaConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP,
                KafkaConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP, KafkaConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP,
                KafkaConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP, KafkaConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP,
                KafkaConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP, KafkaConfig.CONSUMER_GROUP_MAX_SIZE_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", 0, -1);
            return true;
        }
        return false;
    }

    private boolean validateTransactionalConfig(String name) {
        switch (name) {
            case KafkaConfig.TRANSACTIONAL_ID_EXPIRATION_MS_PROP:
            case KafkaConfig.TRANSACTIONS_MAX_TIMEOUT_MS_PROP:
            case KafkaConfig.TRANSACTIONS_TOPIC_MIN_ISR_PROP:
            case KafkaConfig.TRANSACTIONS_LOAD_BUFFER_SIZE_PROP:
            case KafkaConfig.TRANSACTIONS_TOPIC_PARTITIONS_PROP:
            case KafkaConfig.TRANSACTIONS_TOPIC_SEGMENT_BYTES_PROP:
            case KafkaConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_PROP:
                assertPropertyInvalid(baseProperties(), name, "not_a_number", "0", "-2");
                return true;
            default:
                return false;
        }
    }

    private boolean validateLogRemoteManagerConfig(String name) {
        switch (name) {
            case RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP:
                assertPropertyInvalid(baseProperties(), name, "not_a_boolean");
                return true;
            case RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP:
            case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP:
            case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP:
            case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MS_PROP:
            case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_BACK_OFF_MAX_MS_PROP:
            case RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP:
            case RemoteLogManagerConfig.REMOTE_LOG_READER_MAX_PENDING_TASKS_PROP:
                assertPropertyInvalid(baseProperties(), name, "not_a_number", 0, -1);
                return true;
            case RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_RETRY_JITTER_PROP:
                assertPropertyInvalid(baseProperties(), name, "not_a_number", -1, 0.51);
                return true;
            case RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP:
            case RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP:
                assertPropertyInvalid(baseProperties(), name, "not_a_number", -3);
                return true;
            default:
                return false;
        }
    }

    @Test
    @SuppressWarnings("deprecation") // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
    public void testFromPropsInvalid() {
        // to ensure a basis is valid - bootstraps all needed validation
        fromProps(baseProperties());

        configNamesWithoutIgnoredConfigs().forEach(name -> {
            if (Arrays.asList(validateBasicConfig(name), validateLogConfig(name), validateKraftConfig(name),
                    validateConsumerGroupAndMetadataConfig(name), validateTransactionalConfig(name), validateLogRemoteManagerConfig(name),
                    validateReplicaConfig(name), validateControlledShutdownConfig(name)).stream().allMatch(v -> !v)) {
                assertPropertyInvalid(baseProperties(), name, "not_a_number", "-1");
            }
        });
    }

    private boolean validateReplicaConfig(String name) {
        if (Arrays.asList(KafkaConfig.DEFAULT_REPLICATION_FACTOR_PROP, KafkaConfig.REPLICA_LAG_TIME_MAX_MS_PROP,
                KafkaConfig.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_PROP, KafkaConfig.REPLICA_FETCH_MAX_BYTES_PROP,
                KafkaConfig.REPLICA_FETCH_WAIT_MAX_MS_PROP, KafkaConfig.REPLICA_FETCH_MIN_BYTES_PROP,
                KafkaConfig.REPLICA_FETCH_RESPONSE_MAX_BYTES_PROP, KafkaConfig.NUM_REPLICA_FETCHERS_PROP,
                KafkaConfig.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_PROP).contains(name)) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number");
            return true;
        }
        if (KafkaConfig.REPLICA_SOCKET_TIMEOUT_MS_PROP == name) {
            assertPropertyInvalid(baseProperties(), name, "not_a_number", "-2");
            return true;
        }
        return false;
    }

    public <T> void assertDynamic(Properties props, KafkaConfig config, String property, Object value, Supplier<T> accessor) {
        Object initial = accessor.get();
        props.setProperty(property, value.toString());
        config.updateCurrentConfig(fromProps(props));
        assertNotEquals(initial, accessor.get());
    }
    @SuppressWarnings("deprecation")
    @Test
    public void testDynamicLogConfigs() {
        Properties props = baseProperties();
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        KafkaConfig config = fromProps(props);

        // Test dynamic log config values can be correctly passed through via KafkaConfig to LogConfig
        // Every log config prop must be explicitly accounted for here.
        // A value other than the default value for this config should be set to ensure that we can check whether
        // the value is dynamically updatable.
        testLogConfig(TopicConfig.CLEANUP_POLICY_CONFIG, props, config, TopicConfig.CLEANUP_POLICY_COMPACT, config::logCleanupPolicy);
        testLogConfig(TopicConfig.COMPRESSION_TYPE_CONFIG, props, config, "lz4", config::compressionType);
        testLogConfig(TopicConfig.SEGMENT_BYTES_CONFIG, props, config, 10000, config::logSegmentBytes);
        testLogConfig(TopicConfig.SEGMENT_MS_CONFIG, props, config, 10001L, config::logRollTimeMillis);
        testLogConfig(TopicConfig.DELETE_RETENTION_MS_CONFIG, props, config, 10002L, config::logCleanerDeleteRetentionMs);
        testLogConfig(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, props, config, 10003L, config::logDeleteDelayMs);
        testLogConfig(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, props, config, 10004L, config::logFlushIntervalMessages);
        testLogConfig(TopicConfig.FLUSH_MS_CONFIG, props, config, 10005L, config::logFlushIntervalMs);
        testLogConfig(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, props, config, 10006L, config::logCleanerMaxCompactionLagMs);
        testLogConfig(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, props, config, 10007, config::logIndexIntervalBytes);
        testLogConfig(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, props, config, 10008, config::messageMaxBytes);
        testLogConfig(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, props, config, false, config::logMessageDownConversionEnable);
        testLogConfig(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, props, config, 10009, config::logMessageTimestampDifferenceMaxMs);
        testLogConfig(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, props, config, 10015L, config::logMessageTimestampBeforeMaxMs);
        testLogConfig(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, props, config, 10016L, config::logMessageTimestampAfterMaxMs);
        testLogConfig(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, props, config, "LogAppendTime", () -> config.logMessageTimestampType().name);
        testLogConfig(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, props, config, 0.01, config::logCleanerMinCleanRatio);
        testLogConfig(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, props, config, 10010L, config::logCleanerMinCompactionLagMs);
        testLogConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, props, config, 4, config::minInSyncReplicas);
        testLogConfig(TopicConfig.RETENTION_BYTES_CONFIG, props, config, 10011L, config::logRetentionBytes);
        testLogConfig(TopicConfig.RETENTION_MS_CONFIG, props, config, 10012L, config::logRetentionTimeMillis);
        testLogConfig(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, props, config, 10013, config::logIndexSizeMaxBytes);
        testLogConfig(TopicConfig.SEGMENT_JITTER_MS_CONFIG, props, config, 10014L, config::logRollTimeJitterMillis);
        testLogConfig(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, props, config, true, config::uncleanLeaderElectionEnable);
        testLogConfig(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, props, config, 10015L, config::logLocalRetentionMs);
        testLogConfig(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, props, config, 10016L, config::logLocalRetentionBytes);
    }

    private <T> void testLogConfig(String logConfig, Properties props, KafkaConfig config, T expectedValue, Supplier<T> accessor) {
        ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.forEach((topicConfig, kafkaConfigProp) -> {
            if (logConfig.equals(topicConfig)) {
                assertDynamic(props, config, kafkaConfigProp, expectedValue, accessor);
            }
        });
    }

    @Test
    public void testSpecificProperties() {
        Properties defaults = new Properties();
        defaults.setProperty(KafkaConfig.ZK_CONNECT_PROP, "127.0.0.1:2181");
        // For ZkConnectionTimeoutMs
        defaults.setProperty(KafkaConfig.ZK_SESSION_TIMEOUT_MS_PROP, "1234");
        defaults.setProperty(KafkaConfig.BROKER_ID_GENERATION_ENABLE_PROP, "false");
        defaults.setProperty(KafkaConfig.MAX_RESERVED_BROKER_ID_PROP, "1");
        defaults.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        defaults.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://127.0.0.1:1122");
        defaults.setProperty(KafkaConfig.MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP, "127.0.0.1:2, 127.0.0.2:3");
        defaults.setProperty(KafkaConfig.LOG_DIR_PROP, "/tmp1,/tmp2");
        defaults.setProperty(KafkaConfig.LOG_ROLL_TIME_HOURS_PROP, "12");
        defaults.setProperty(KafkaConfig.LOG_ROLL_TIME_JITTER_HOURS_PROP, "11");
        defaults.setProperty(KafkaConfig.LOG_RETENTION_TIME_HOURS_PROP, "10");
        //For LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP
        defaults.setProperty(KafkaConfig.LOG_FLUSH_SCHEDULER_INTERVAL_MS_PROP, "123");
        defaults.setProperty(KafkaConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_PROP, Byte.toString(CompressionType.SNAPPY.id));
        // For METRIC_RECORDING_LEVEL_PROP
        defaults.setProperty(KafkaConfig.METRIC_RECORDING_LEVEL_PROP, Sensor.RecordingLevel.DEBUG.toString());

        KafkaConfig config = fromProps(defaults);
        assertEquals("127.0.0.1:2181", config.zkConnect());
        assertEquals(1234, config.zkConnectionTimeoutMs());
        assertEquals(false, config.brokerIdGenerationEnable());
        assertEquals(1, config.maxReservedBrokerId());
        assertEquals(1, config.brokerId());
        assertEquals(Arrays.asList("PLAINTEXT://127.0.0.1:1122"), config.effectiveAdvertisedListeners().stream().map(e -> e.connectionString()).collect(Collectors.toList()));
        assertEquals(Utils.mkMap(Utils.mkEntry("127.0.0.1", 2), Utils.mkEntry("127.0.0.2", 3)), config.maxConnectionsPerIpOverrides());
        assertEquals(Arrays.asList("/tmp1", "/tmp2"), config.logDirs());
        assertEquals(12 * 60L * 1000L * 60, config.logRollTimeMillis());
        assertEquals(11 * 60L * 1000L * 60, config.logRollTimeJitterMillis());
        assertEquals(10 * 60L * 1000L * 60, config.logRetentionTimeMillis());
        assertEquals(123L, config.logFlushIntervalMs());
        assertEquals(CompressionType.SNAPPY, config.offsetsTopicCompressionType());
        assertEquals(Sensor.RecordingLevel.DEBUG.toString(), config.metricRecordingLevel());
        assertEquals(false, config.tokenAuthEnabled());
        assertEquals(7 * 24 * 60L * 60L * 1000L, config.delegationTokenMaxLifeMs());
        assertEquals(24 * 60L * 60L * 1000L, config.delegationTokenExpiryTimeMs());
        assertEquals(1 * 60L * 1000L * 60, config.delegationTokenExpiryCheckIntervalMs());

        defaults.setProperty(KafkaConfig.DELEGATION_TOKEN_SECRET_KEY_PROP, "1234567890");
        KafkaConfig config1 = fromProps(defaults);
        assertEquals(true, config1.tokenAuthEnabled());
    }

    @Test
    public void testNonroutableAdvertisedListeners() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "127.0.0.1:2181");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://0.0.0.0:9092");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testMaxConnectionsPerIpProp() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP, "0");
        assertFalse(isValidKafkaConfig(props));
        props.setProperty(KafkaConfig.MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP, "127.0.0.1:100");
        fromProps(props);
        props.setProperty(KafkaConfig.MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP, "127.0.0.0#:100");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testDistinctControllerAndAdvertisedListenersAllowedForKRaftBroker() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094");
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, "PLAINTEXT://A:9092,SSL://B:9093"); // explicitly setting it in KRaft
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SASL_SSL");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "3@localhost:9094");

        // invalid due to extra listener also appearing in controller listeners
        assertBadConfigContainingMessage(props,
                "controller.listener.names must not contain a value appearing in the 'listeners' configuration when running KRaft with just the broker role");

        // Valid now
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://A:9092,SSL://B:9093");
        fromProps(props);

        // Also valid if we let advertised listeners be derived from listeners/controller.listener.names
        // since listeners and advertised.listeners are explicitly identical at this point
        props.remove(KafkaConfig.ADVERTISED_LISTENERS_PROP);
        fromProps(props);
    }

    @Test
    public void testControllerListenersCannotBeAdvertisedForKRaftBroker() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker,controller");
        String listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094";
        props.setProperty(KafkaConfig.LISTENERS_PROP, listeners);
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, listeners); // explicitly setting it in KRaft
        props.setProperty(KafkaConfig.INTER_BROKER_LISTENER_NAME_PROP, "SASL_SSL");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "PLAINTEXT,SSL");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9092");
        assertBadConfigContainingMessage(props,
                "The advertised.listeners config must not contain KRaft controller listeners from controller.listener.names when process.roles contains the broker role");

        // Valid now
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, "SASL_SSL://C:9094");
        fromProps(props);

        // Also valid if we allow advertised listeners to derive from listeners/controller.listener.names
        props.remove(KafkaConfig.ADVERTISED_LISTENERS_PROP);
        fromProps(props);
    }

    @Test
    public void testAdvertisedListenersDisallowedForKRaftControllerOnlyRole() {
        // Test that advertised listeners cannot be set when KRaft and server is controller only.
        // Test that listeners must enumerate every controller listener
        // Test that controller listener must enumerate every listener
        String correctListeners = "PLAINTEXT://A:9092,SSL://B:9093";
        String incorrectListeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094";

        String correctControllerListenerNames = "PLAINTEXT,SSL";

        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "controller");
        props.setProperty(KafkaConfig.LISTENERS_PROP, correctListeners);
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, incorrectListeners);
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, correctControllerListenerNames);
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9092");
        String expectedExceptionContainsText = "The advertised.listeners config must be empty when process.roles=controller";
        assertBadConfigContainingMessage(props, expectedExceptionContainsText);

        // Invalid if advertised listeners is explicitly to the set
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, correctListeners);
        assertBadConfigContainingMessage(props, expectedExceptionContainsText);

        // Invalid if listeners contains names not in controller.listener.names
        props.remove(KafkaConfig.ADVERTISED_LISTENERS_PROP);
        props.setProperty(KafkaConfig.LISTENERS_PROP, incorrectListeners);
        expectedExceptionContainsText = "The listeners config must only contain KRaft controller listeners from controller.listener.names when process.roles=controller";
        assertBadConfigContainingMessage(props, expectedExceptionContainsText);

        // Invalid if listeners doesn't contain every name in controller.listener.names
        props.setProperty(KafkaConfig.LISTENERS_PROP, correctListeners);
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, correctControllerListenerNames + ",SASL_SSL");
        expectedExceptionContainsText = "controller.listener.names must only contain values appearing in the 'listeners' configuration when running the KRaft controller role";
        assertBadConfigContainingMessage(props, expectedExceptionContainsText);

        // Valid now
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, correctControllerListenerNames);
        fromProps(props);
    }

    @Test
    public void testControllerQuorumVoterStringsToNodes() {
        assertThrows(ConfigException.class, () -> RaftConfig.quorumVoterStringsToNodes(Collections.singletonList("")));
        assertEquals(Arrays.asList(new Node(3000, "example.com", 9093)),
                RaftConfig.quorumVoterStringsToNodes(Arrays.asList("3000@example.com:9093")));
        assertEquals(Arrays.asList(new Node(3000, "example.com", 9093), new Node(3001, "example.com", 9094)),
                RaftConfig.quorumVoterStringsToNodes(Arrays.asList("3000@example.com:9093", "3001@example.com:9094")));
    }

    @Test
    public void testInvalidQuorumVoterConfig() {
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
        assertInvalidQuorumVoters("1@kafka1:9092:1@kafka2:9092");
    }

    @Test
    public void testValidQuorumVotersConfig() {
        Map<Integer, RaftConfig.AddressSpec> expected = new HashMap<>();
        assertValidQuorumVoters("", expected);

        expected.put(1, new RaftConfig.InetAddressSpec(new InetSocketAddress("127.0.0.1", 9092)));
        assertValidQuorumVoters("1@127.0.0.1:9092", expected);

        expected.clear();
        expected.put(1, UNKNOWN_ADDRESS_SPEC_INSTANCE);
        assertValidQuorumVoters("1@0.0.0.0:0", expected);

        expected.clear();
        expected.put(1, new RaftConfig.InetAddressSpec(new InetSocketAddress("kafka1", 9092)));
        expected.put(2, new RaftConfig.InetAddressSpec(new InetSocketAddress("kafka2", 9092)));
        expected.put(3, new RaftConfig.InetAddressSpec(new InetSocketAddress("kafka3", 9092)));
        assertValidQuorumVoters("1@kafka1:9092,2@kafka2:9092,3@kafka3:9092", expected);
    }

    private void assertValidQuorumVoters(String value, Map<Integer, RaftConfig.AddressSpec> expectedVoters) {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT);
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, value);
        RaftConfig raftConfig = new RaftConfig(fromProps(props));
        assertEquals(expectedVoters, raftConfig.quorumVoterConnections());
    }

    @Test
    public void testAcceptsLargeNodeIdForRaftBasedCase() {
        // Generation of Broker IDs is not supported when using Raft-based controller quorums,
        // so pick a broker ID greater than reserved.broker.max.id, which defaults to 1000,
        // and make sure it is allowed despite broker.id.generation.enable=true (true is the default)
        int largeBrokerId = 2000;
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://localhost:9092");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        props.setProperty(KafkaConfig.NODE_ID_PROP, Integer.toString(largeBrokerId));
        fromProps(props);
    }

    @Test
    public void testRejectsNegativeNodeIdForRaftBasedBrokerCaseWithAutoGenEnabled() {
        // -1 is the default for both node.id and broker.id
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testRejectsNegativeNodeIdForRaftBasedControllerCaseWithAutoGenEnabled() {
        // -1 is the default for both node.id and broker.id
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "controller");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testRejectsLargeNodeIdForZkBasedCaseWithAutoGenEnabled() {
        // Generation of Broker IDs is supported when using ZooKeeper-based controllers,
        // so pick a broker ID greater than reserved.broker.max.id, which defaults to 1000,
        // and make sure it is not allowed with broker.id.generation.enable=true (true is the default)
        int largeBrokerId = 2000;
        Properties props = TestUtils.createBrokerConfig(largeBrokerId, TestUtils.MOCK_ZK_CONNECT, TestUtils.MOCK_ZK_PORT);
        String listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094";
        props.setProperty(KafkaConfig.LISTENERS_PROP, listeners);
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, listeners);
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testAcceptsNegativeOneNodeIdForZkBasedCaseWithAutoGenEnabled() {
        // -1 is the default for both node.id and broker.id; it implies "auto-generate" and should succeed
        Properties props = TestUtils.createBrokerConfig(-1, TestUtils.MOCK_ZK_CONNECT, TestUtils.MOCK_ZK_PORT);
        String listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094";
        props.setProperty(KafkaConfig.LISTENERS_PROP, listeners);
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, listeners);
        fromProps(props);
    }

    @Test
    public void testRejectsNegativeTwoNodeIdForZkBasedCaseWithAutoGenEnabled() {
        // -1 implies "auto-generate" and should succeed, but -2 does not and should fail
        int negativeTwoNodeId = -2;
        Properties props = TestUtils.createBrokerConfig(negativeTwoNodeId, TestUtils.MOCK_ZK_CONNECT, TestUtils.MOCK_ZK_PORT);
        String listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094";
        props.setProperty(KafkaConfig.LISTENERS_PROP, listeners);
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, listeners);
        props.setProperty(KafkaConfig.NODE_ID_PROP, Integer.toString(negativeTwoNodeId));
        props.setProperty(KafkaConfig.BROKER_ID_PROP, Integer.toString(negativeTwoNodeId));
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testAcceptsLargeNodeIdForZkBasedCaseWithAutoGenDisabled() {
        // Ensure a broker ID greater than reserved.broker.max.id, which defaults to 1000,
        // is allowed with broker.id.generation.enable=false
        int largeBrokerId = 2000;
        Properties props = TestUtils.createBrokerConfig(largeBrokerId, TestUtils.MOCK_ZK_CONNECT, TestUtils.MOCK_ZK_PORT);
        String listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094";
        props.setProperty(KafkaConfig.LISTENERS_PROP, listeners);
        props.setProperty(KafkaConfig.ADVERTISED_LISTENERS_PROP, listeners);
        props.setProperty(KafkaConfig.BROKER_ID_GENERATION_ENABLE_PROP, "false");
        fromProps(props);
    }

    @Test
    public void testRejectsNegativeNodeIdForZkBasedCaseWithAutoGenDisabled() {
        // -1 is the default for both node.id and broker.id
        Properties props = TestUtils.createBrokerConfig(-1, TestUtils.MOCK_ZK_CONNECT, TestUtils.MOCK_ZK_PORT);
        String listeners = "PLAINTEXT://A:9092,SSL://B:9093,SASL_SSL://C:9094";
        props.setProperty(KafkaConfig.LISTENERS_PROP, listeners);
        props.setProperty(KafkaConfig.BROKER_ID_GENERATION_ENABLE_PROP, "false");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testZookeeperConnectRequiredIfEmptyProcessRoles() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://127.0.0.1:9092");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testZookeeperConnectNotRequiredIfNonEmptyProcessRoles() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://127.0.0.1:9092");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "1");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        fromProps(props);
    }

    @Test
    public void testCustomMetadataLogDir() {
        String metadataDir = "/path/to/metadata/dir";
        String dataDir = "/path/to/data/dir";

        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        props.setProperty(KafkaConfig.METADATA_LOG_DIR_PROP, metadataDir);
        props.setProperty(KafkaConfig.LOG_DIR_PROP, dataDir);
        props.setProperty(KafkaConfig.NODE_ID_PROP, "1");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        fromProps(props);

        KafkaConfig config = fromProps(props);
        assertEquals(metadataDir, config.metadataLogDir());
        assertEquals(Collections.singletonList(dataDir), config.logDirs());
    }

    @Test
    public void testDefaultMetadataLogDir() {
        String dataDir1 = "/path/to/data/dir/1";
        String dataDir2 = "/path/to/data/dir/2";

        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        props.setProperty(KafkaConfig.LOG_DIR_PROP, dataDir1 + "," + dataDir2);
        props.setProperty(KafkaConfig.NODE_ID_PROP, "1");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        fromProps(props);

        KafkaConfig config = fromProps(props);
        assertEquals(dataDir1, config.metadataLogDir());
        assertEquals(Arrays.asList(dataDir1, dataDir2), config.logDirs());
    }

    @Test
    public void testPopulateSynonymsOnEmptyMap() {
        assertEquals(Collections.emptyMap(), KafkaConfig.populateSynonyms(Collections.emptyMap()));
    }

    @Test
    public void testPopulateSynonymsOnMapWithoutNodeId() {
        Map<String, String> input = new HashMap<>();
        input.put(KafkaConfig.BROKER_ID_PROP, "4");
        Map<String, String> expectedOutput = new HashMap<>();
        expectedOutput.put(KafkaConfig.BROKER_ID_PROP, "4");
        expectedOutput.put(KafkaConfig.NODE_ID_PROP, "4");
        assertEquals(expectedOutput, KafkaConfig.populateSynonyms(input));
    }

    @Test
    public void testPopulateSynonymsOnMapWithoutBrokerId() {
        Map<String, String> input = new HashMap<>();
        input.put(KafkaConfig.NODE_ID_PROP, "4");
        Map<String, String>  expectedOutput = new HashMap<>();
        expectedOutput.put(KafkaConfig.BROKER_ID_PROP, "4");
        expectedOutput.put(KafkaConfig.NODE_ID_PROP, "4");
        assertEquals(expectedOutput, KafkaConfig.populateSynonyms(input));
    }

    @Test
    public void testNodeIdMustNotBeDifferentThanBrokerId() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "1");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "2");
        assertEquals("You must set `node.id` to the same value as `broker.id`.",
                assertThrows(ConfigException.class, () -> fromProps(props)).getMessage());
    }

    @Test
    public void testNodeIdOrBrokerIdMustBeSetWithKraft() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        assertEquals("Missing configuration `node.id` which is required when `process.roles` " +
                        "is defined (i.e. when running in KRaft mode).",
                assertThrows(ConfigException.class, () -> fromProps(props)).getMessage());
    }

    @Test
    public void testNodeIdIsInferredByBrokerIdWithKraft() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "SSL");
        props.setProperty(KafkaConfig.BROKER_ID_PROP, "3");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        KafkaConfig config = fromProps(props);
        assertEquals(3, config.brokerId());
        assertEquals(3, config.nodeId());
        Map<String, Object>  originals = config.originals();
        assertEquals("3", originals.get(KafkaConfig.BROKER_ID_PROP));
        assertEquals("3", originals.get(KafkaConfig.NODE_ID_PROP));
    }

    @Test
    public void testRejectsNegativeNodeIdForRaftBasedCaseWithAutoGenDisabled() {
        // -1 is the default for both node.id and broker.id
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.BROKER_ID_GENERATION_ENABLE_PROP, "false");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "2@localhost:9093");
        assertFalse(isValidKafkaConfig(props));
    }

    @Test
    public void testBrokerIdIsInferredByNodeIdWithKraft() {
        Properties props = new Properties();
        props.putAll(kraftProps());
        KafkaConfig config = fromProps(props);
        assertEquals(3, config.brokerId());
        assertEquals(3, config.nodeId());
        Map<String, Object> originals = config.originals();
        assertEquals("3", originals.get(KafkaConfig.BROKER_ID_PROP));
        assertEquals("3", originals.get(KafkaConfig.NODE_ID_PROP));
    }

    @Test
    public void testSaslJwksEndpointRetryDefaults() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");
        KafkaConfig config = fromProps(props);
        assertNotNull(config.getLong(KafkaConfig.SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_PROP));
        assertNotNull(config.getLong(KafkaConfig.SASL_O_AUTH_BEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_PROP));
    }

    @Test
    public void testInvalidAuthorizerClassName() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        Map<Object, Object> configs = new HashMap<>(props);

        configs.put(KafkaConfig.AUTHORIZER_CLASS_NAME_PROP, null);
        ConfigException ce = assertThrows(ConfigException.class, () -> fromProps(configs));
        assertTrue(ce.getMessage().contains(KafkaConfig.AUTHORIZER_CLASS_NAME_PROP));
    }

    @Test
    public void testInvalidSecurityInterBrokerProtocol() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.setProperty(KafkaConfig.INTER_BROKER_SECURITY_PROTOCOL_PROP, "abc");
        ConfigException ce = assertThrows(ConfigException.class, () -> fromProps(props));
        assertTrue(ce.getMessage().contains(KafkaConfig.INTER_BROKER_SECURITY_PROTOCOL_PROP));
    }

    @Test
    public void testEarlyStartListenersDefault() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "controller");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        props.setProperty(KafkaConfig.LISTENERS_PROP, "CONTROLLER://:8092");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "1");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "1@localhost:9093");
        KafkaConfig config = fromProps(props);
        assertEquals(Utils.mkSet("CONTROLLER"), config.earlyStartListeners.stream().map(ListenerName::value).collect(Collectors.toSet()));
    }

    @Test
    public void testEarlyStartListeners() {
        Properties props = new Properties();
        props.putAll(kraftProps());
        props.setProperty(KafkaConfig.EARLY_START_LISTENERS_PROP, "INTERNAL,INTERNAL2");
        props.setProperty(KafkaConfig.INTER_BROKER_LISTENER_NAME_PROP, "INTERNAL");
        props.setProperty(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP,
                "INTERNAL:PLAINTEXT,INTERNAL2:PLAINTEXT,CONTROLLER:PLAINTEXT");
        props.setProperty(KafkaConfig.LISTENERS_PROP,
                "INTERNAL://127.0.0.1:9092,INTERNAL2://127.0.0.1:9093");
        KafkaConfig config = fromProps(props);
        assertEquals(Utils.mkSet(new ListenerName("INTERNAL"), new ListenerName("INTERNAL2")),
                config.earlyStartListeners);
    }

    @Test
    public void testEarlyStartListenersMustBeListeners() {
        Properties props = new Properties();
        props.putAll(kraftProps());
        props.setProperty(KafkaConfig.EARLY_START_LISTENERS_PROP, "INTERNAL");
        assertEquals("early.start.listeners contains listener INTERNAL, but this is not " +
                        "contained in listeners or controller.listener.names",
                assertThrows(ConfigException.class, () -> fromProps(props)).getMessage());
    }

    @Test
    public void testIgnoreUserInterBrokerProtocolVersionKRaft() {
        Arrays.asList("3.0", "3.1", "3.2").forEach(ibp -> {
            Properties props = new Properties();
            props.putAll(kraftProps());
            props.setProperty(KafkaConfig.INTER_BROKER_PROTOCOL_VERSION_PROP, ibp);
            KafkaConfig config = fromProps(props);
            assertEquals(config.interBrokerProtocolVersion(), MetadataVersion.MINIMUM_KRAFT_VERSION);
        });
    }

    @Test
    public void testInvalidInterBrokerProtocolVersionKRaft() {
        Properties props = new Properties();
        props.putAll(kraftProps());
        props.setProperty(KafkaConfig.INTER_BROKER_PROTOCOL_VERSION_PROP, "2.8");
        assertEquals("A non-KRaft version 2.8 given for inter.broker.protocol.version. The minimum version is 3.0-IV1",
                assertThrows(ConfigException.class, () -> fromProps(props)).getMessage());
    }

    @Test
    public void testDefaultInterBrokerProtocolVersionKRaft() {
        Properties props = new Properties();
        props.putAll(kraftProps());
        KafkaConfig config = fromProps(props);
        assertEquals(config.interBrokerProtocolVersion(), MetadataVersion.MINIMUM_KRAFT_VERSION);
    }

    @Test
    public void testMetadataMaxSnapshotInterval() {
        int validValue = 100;
        Properties props = new Properties();
        props.putAll(kraftProps());
        props.setProperty(KafkaConfig.METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP, Integer.toString(validValue));

        KafkaConfig config = fromProps(props);
        assertEquals(validValue, config.metadataSnapshotMaxIntervalMs());

        props.setProperty(KafkaConfig.METADATA_SNAPSHOT_MAX_INTERVAL_MS_PROP, "-1");
        String errorMessage = assertThrows(ConfigException.class, () -> fromProps(props)).getMessage();

        assertEquals(
                "Invalid value -1 for configuration metadata.log.max.snapshot.interval.ms: Value must be at least 0",
                errorMessage
        );
    }

    @Test
    public void testMigrationEnabledZkMode() {
        Properties props = TestUtils.createBrokerConfig(1, TestUtils.MOCK_ZK_CONNECT, TestUtils.MOCK_ZK_PORT);
        props.setProperty(KafkaConfig.MIGRATION_ENABLED_PROP, "true");
        assertEquals(
                "If using zookeeper.metadata.migration.enable, controller.quorum.voters must contain a parseable set of voters.",
                assertThrows(ConfigException.class, () -> fromProps(props)).getMessage());

        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "3000@localhost:9093");
        assertEquals(
                "requirement failed: controller.listener.names must not be empty when running in ZooKeeper migration mode: []",
                assertThrows(IllegalArgumentException.class, () -> fromProps(props)).getMessage());

        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");

        // All needed configs are now set
        fromProps(props);

        // Check that we allow authorizer to be set
        props.setProperty(KafkaConfig.AUTHORIZER_CLASS_NAME_PROP, "kafka.security.authorizer.AclAuthorizer");
        fromProps(props);

        // Don't allow migration startup with an older IBP
        props.setProperty(KafkaConfig.INTER_BROKER_PROTOCOL_VERSION_PROP, MetadataVersion.IBP_3_3_IV0.version());
        assertEquals(
                "requirement failed: Cannot enable ZooKeeper migration without setting 'inter.broker.protocol.version' to 3.4 or higher",
                assertThrows(IllegalArgumentException.class, () -> fromProps(props)).getMessage());

        props.remove(KafkaConfig.MIGRATION_ENABLED_PROP);
        assertEquals(
                "requirement failed: controller.listener.names must be empty when not running in KRaft mode: [CONTROLLER]",
                assertThrows(IllegalArgumentException.class, () -> fromProps(props)).getMessage());

        props.remove(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP);
        fromProps(props);
    }

    @Test
    public void testMigrationCannotBeEnabledWithJBOD() {
        Properties props = TestUtils.createBrokerConfig(
                new TestUtils.BrokerConfigBuilder.Builder(1, TestUtils.MOCK_ZK_CONNECT).logDirCount(2).port(TestUtils.MOCK_ZK_PORT).build()
        );
        props.setProperty(KafkaConfig.MIGRATION_ENABLED_PROP, "true");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "3000@localhost:9093");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        props.setProperty(KafkaConfig.INTER_BROKER_PROTOCOL_VERSION_PROP, MetadataVersion.IBP_3_7_IV1.version());

        assertEquals(
                "requirement failed: Cannot enable ZooKeeper migration with multiple log directories " +
                        "(aka JBOD) without setting 'inter.broker.protocol.version' to 3.7-IV2 or higher",
                assertThrows(IllegalArgumentException.class, () -> fromProps(props)).getMessage());
    }

    @Test
    public void testMigrationEnabledKRaftMode() {
        Properties props = new Properties();
        props.putAll(kraftProps());
        props.setProperty(KafkaConfig.MIGRATION_ENABLED_PROP, "true");

        assertEquals(
                "If using `zookeeper.metadata.migration.enable` in KRaft mode, `zookeeper.connect` must also be set.",
                assertThrows(ConfigException.class, () -> fromProps(props)).getMessage());

        props.setProperty(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181");
        fromProps(props);
    }

    @Test
    public void testConsumerGroupSessionTimeoutValidation() {
        Properties props = new Properties();
        props.putAll(kraftProps());

        // Max should be greater than or equals to min.
        props.put(KafkaConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP, "20");
        props.put(KafkaConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP, "10");
        assertThrows(IllegalArgumentException.class, () -> fromProps(props));

        // The timeout should be within the min-max range.
        props.put(KafkaConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_PROP, "10");
        props.put(KafkaConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_PROP, "20");
        props.put(KafkaConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP, "5");
        assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        props.put(KafkaConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_PROP, "25");
        assertThrows(IllegalArgumentException.class, () -> fromProps(props));
    }

    @Test
    public void testConsumerGroupHeartbeatIntervalValidation() {
        Properties props = new Properties();
        props.putAll(kraftProps());

        // Max should be greater than or equals to min.
        props.put(KafkaConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP, "20");
        props.put(KafkaConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP, "10");
        assertThrows(IllegalArgumentException.class, () -> fromProps(props));

        // The timeout should be within the min-max range.
        props.put(KafkaConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_PROP, "10");
        props.put(KafkaConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_PROP, "20");
        props.put(KafkaConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP, "5");
        assertThrows(IllegalArgumentException.class, () -> fromProps(props));
        props.put(KafkaConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_PROP, "25");
        assertThrows(IllegalArgumentException.class, () -> fromProps(props));
    }

    @Test
    public void testMultipleLogDirectoriesNotSupportedWithRemoteLogStorage() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, String.valueOf(true));
        props.put(KafkaConfig.LOG_DIRS_PROP, "/tmp/a,/tmp/b");

        ConfigException caught = assertThrows(ConfigException.class, () -> fromProps(props));
        assertTrue(caught.getMessage().contains("Multiple log directories `/tmp/a,/tmp/b` are not supported when remote log storage is enabled"));
    }

    @Test
    public void testSingleLogDirectoryWithRemoteLogStorage() {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT, 8181);
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, String.valueOf(true));
        props.put(KafkaConfig.LOG_DIRS_PROP, "/tmp/a");
        assertDoesNotThrow(() -> fromProps(props));
    }

    Properties kraftProps() {
        Properties props = new Properties();
        props.setProperty(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.setProperty(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        props.setProperty(KafkaConfig.NODE_ID_PROP, "3");
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, "1@localhost:9093");
        return props;
    }
    private void assertInvalidQuorumVoters(String value) {
        Properties props = TestUtils.createBrokerConfig(0, TestUtils.MOCK_ZK_CONNECT);
        props.setProperty(KafkaConfig.QUORUM_VOTERS_PROP, value);
        assertThrows(ConfigException.class, () -> fromProps(props));
    }

    private void assertPropertyInvalid(Properties validRequiredProps, String name, Object... values) {
        for (Object value : values) {
            Properties props = new Properties();
            props.putAll(validRequiredProps);
            props.setProperty(name, value.toString());
            assertThrows(Exception.class, () -> fromProps(props),
                    "Expected exception for property `" + name + "` with invalid value `" + value + "` was not thrown");
        }
    }
    private Properties baseProperties() {
        Properties validRequiredProperties = new Properties();
        validRequiredProperties.setProperty(KafkaConfig.ZK_CONNECT_PROP, "127.0.0.1:2181");
        return validRequiredProperties;
    }
    private List<Endpoint> listenerListToEndpoints(String listenerList) {
        return listenerListToEndpoints(listenerList, Arrays.stream(SecurityProtocol.values()).collect(Collectors.toMap(sp -> ListenerName.forSecurityProtocol(sp), sp -> sp)));
    }
    private List<Endpoint> listenerListToEndpoints(String listenerList,
                                                   Map<ListenerName, SecurityProtocol> securityProtocolMap) {
        return org.apache.kafka.utils.CoreUtils.listenerListToEndpoints(listenerList, securityProtocolMap);
    }

    public static void assertBadConfigContainingMessage(Properties props, String expectedExceptionContainsText) {
        try {
            fromProps(props);
            fail("Expected illegal configuration but instead it was legal");
        } catch (ConfigException | IllegalArgumentException caught) {
            assertTrue(
                    caught.getMessage().contains(expectedExceptionContainsText),
                    "\"" + caught.getMessage() + "\" doesn't contain \"" + expectedExceptionContainsText + "\""
            );
        }
    }

    private static boolean isValidKafkaConfig(Properties props) {
        try {
            fromProps(props);
            return true;
        } catch (IllegalArgumentException | ConfigException ignored) {
            return false;
        }
    }

    private static KafkaConfig fromProps(Properties properties) {
        return fromProps(Utils.propsToMap(properties));
    }

    private static KafkaConfig fromProps(Map<?, ?> properties) {
        KafkaConfig kafkaConfig = new KafkaConfig(true, KafkaConfig.populateSynonyms(properties), new DynamicBrokerConfigProviderMock());
        kafkaConfig.validateValues();
        return kafkaConfig;
    }

    private static class DynamicBrokerConfigProviderMock implements KafkaConfig.DynamicConfigProvider {

        @Override
        public DynamicBrokerConfigBaseManager init(KafkaConfig config) {
            return new DynamicBrokerConfigBaseManager() {

                @Override
                public Map<String, String> staticBrokerConfigs() {
                    return null;
                }

                @Override
                public Map<String, String> staticDefaultConfigs() {
                    return null;
                }

                @Override
                public void addReconfigurable(Reconfigurable reconfigurable) {

                }

                @Override
                public void addBrokerReconfigurable(BrokerReconfigurable reconfigurable) {

                }

                @Override
                public void addReconfigurables(ReconfigurableServer server) {

                }

                @Override
                public void removeReconfigurable(Reconfigurable reconfigurable) {

                }

                @Override
                public void updateDefaultConfig(Properties persistentProps, Boolean doLog) {

                }

                @Override
                public void updateBrokerConfig(int brokerId, Properties persistentProps, Boolean doLog) {

                }

                @Override
                public Map<String, String> currentDynamicDefaultConfigs() {
                    return null;
                }

                @Override
                public Map<String, String> currentDynamicBrokerConfigs() {
                    return null;
                }

                @Override
                public KafkaConfig currentKafkaConfig() {
                    return null;
                }

                @Override
                public void clear() {

                }

                @Override
                public Properties toPersistentProps(Properties configProps, Boolean perBrokerConfig) {
                    return null;
                }

                @Override
                public void reloadUpdatedFilesWithoutConfigChange(Properties newProps) {

                }

                @Override
                public void maybeReconfigure(Reconfigurable reconfigurable, KafkaConfig oldConfig, Map<String, ?> newConfig) {

                }

                @Override
                public void initialize(Optional<KafkaZKClient> zkClientOpt, Optional<ClientMetricsReceiverPlugin> clientMetricsReceiverPluginOpt) {

                }

                @Override
                public Properties fromPersistentProps(Properties persistentProps, Boolean perBrokerConfig) {
                    return null;
                }

                @Override
                public void validate(Properties props, Boolean perBrokerConfig) {

                }

                @Override
                public Optional<ClientMetricsReceiverPlugin> clientMetricsReceiverPlugin() {
                    return Optional.empty();
                }
            };
        }
    }
}
