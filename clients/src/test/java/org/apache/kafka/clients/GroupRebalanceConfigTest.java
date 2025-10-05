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
package org.apache.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupRebalanceConfigTest {

    @ParameterizedTest
    @EnumSource(value = GroupRebalanceConfig.ProtocolType.class, names = {"CONSUMER", "SHARE"})
    void testRackIdIsEmptyIfNoDefined(GroupRebalanceConfig.ProtocolType protocolType) {
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            new ConsumerConfig(Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
            )),
            protocolType
        );
        assertTrue(groupRebalanceConfig.rackId.isEmpty());
    }

    @ParameterizedTest
    @EnumSource(value = GroupRebalanceConfig.ProtocolType.class, names = {"CONSUMER", "SHARE"})
    void testRackIdIsEmptyIfValueIsEmptyString(GroupRebalanceConfig.ProtocolType protocolType) {
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            new ConsumerConfig(Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.CLIENT_RACK_CONFIG, "",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
            )),
            protocolType
        );
        assertTrue(groupRebalanceConfig.rackId.isEmpty());
    }

    @ParameterizedTest
    @EnumSource(value = GroupRebalanceConfig.ProtocolType.class, names = {"CONSUMER", "SHARE"})
    void testRackIdIsNotEmptyIfDefined(GroupRebalanceConfig.ProtocolType protocolType) {
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            new ConsumerConfig(Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.CLIENT_RACK_CONFIG, "rack1",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
            )),
            protocolType
        );
        assertTrue(groupRebalanceConfig.rackId.isPresent());
        assertEquals("rack1", groupRebalanceConfig.rackId.get());
    }
}
