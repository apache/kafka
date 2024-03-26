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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.common.ProcessRole;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RaftConfigTest {
    @Test
    void shouldParseCorrectConfigurationWithControllerRole() {
        Properties properties = new Properties();
        properties.setProperty(RaftConfig.NODE_ID_CONFIG, "2");
        properties.setProperty(RaftConfig.PROCESS_ROLES_CONFIG, "controller");
        properties.setProperty(RaftConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092,3@localhost:9093,4@localhost:9094");
        RaftConfig config = new RaftConfig(properties);
        assertEquals(2, config.nodeId());
        assertEquals(new HashSet<>(Arrays.asList(ProcessRole.ControllerRole)), config.processRoles());
        assertEquals(new HashSet<>(Arrays.asList(2, 3, 4)), config.quorumVoterIds());
    }

    @Test
    void shouldParseCorrectConfigurationWithControllerAndBrokerRoles() {
        Properties properties = new Properties();
        properties.setProperty(RaftConfig.NODE_ID_CONFIG, "2");
        properties.setProperty(RaftConfig.PROCESS_ROLES_CONFIG, "controller,broker");
        properties.setProperty(RaftConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092,3@localhost:9093,4@localhost:9094");
        RaftConfig config = new RaftConfig(properties);
        assertEquals(2, config.nodeId());
        assertEquals(new HashSet<>(Arrays.asList(ProcessRole.BrokerRole, ProcessRole.ControllerRole)), config.processRoles());
        assertEquals(new HashSet<>(Arrays.asList(2, 3, 4)), config.quorumVoterIds());
    }

    @Test
    void shouldParseCorrectConfigurationWithBrokerRole() {
        Properties properties = new Properties();
        properties.setProperty(RaftConfig.NODE_ID_CONFIG, "5");
        properties.setProperty(RaftConfig.PROCESS_ROLES_CONFIG, "broker");
        properties.setProperty(RaftConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092,3@localhost:9093,4@localhost:9094");
        RaftConfig config = new RaftConfig(properties);
        assertEquals(5, config.nodeId());
        assertEquals(new HashSet<>(Arrays.asList(ProcessRole.BrokerRole)), config.processRoles());
        assertEquals(new HashSet<>(Arrays.asList(2, 3, 4)), config.quorumVoterIds());
    }

    @Test
    void shouldFailWithUnknownProcessRole() {
        Properties properties = new Properties();
        properties.setProperty(RaftConfig.NODE_ID_CONFIG, "2");
        properties.setProperty(RaftConfig.PROCESS_ROLES_CONFIG, "foo");
        properties.setProperty(RaftConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092");
        assertThrows(ConfigException.class, () -> new RaftConfig(properties));
    }

    @Test
    void shouldFailWithDuplicateRoleName() {
        Properties properties = new Properties();
        properties.setProperty(RaftConfig.NODE_ID_CONFIG, "2");
        properties.setProperty(RaftConfig.PROCESS_ROLES_CONFIG, "broker,controller,broker");
        properties.setProperty(RaftConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092");
        assertThrows(ConfigException.class, () -> new RaftConfig(properties));
    }
}
