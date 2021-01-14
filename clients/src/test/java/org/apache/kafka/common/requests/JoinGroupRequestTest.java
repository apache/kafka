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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class JoinGroupRequestTest {

    @Test
    public void shouldAcceptValidGroupInstanceIds() {
        String maxLengthString = TestUtils.randomString(249);
        String[] validGroupInstanceIds = {"valid", "INSTANCE", "gRoUp", "ar6", "VaL1d", "_0-9_.", "...", maxLengthString};

        for (String instanceId : validGroupInstanceIds) {
            JoinGroupRequest.validateGroupInstanceId(instanceId);
        }
    }

    @Test
    public void shouldThrowOnInvalidGroupInstanceIds() {
        char[] longString = new char[250];
        Arrays.fill(longString, 'a');
        String[] invalidGroupInstanceIds = {"", "foo bar", "..", "foo:bar", "foo=bar", ".", new String(longString)};

        for (String instanceId : invalidGroupInstanceIds) {
            try {
                JoinGroupRequest.validateGroupInstanceId(instanceId);
                fail("No exception was thrown for invalid instance id: " + instanceId);
            } catch (InvalidConfigurationException e) {
                // Good
            }
        }
    }

    @Test
    public void shouldRecognizeInvalidCharactersInGroupInstanceIds() {
        char[] invalidChars = {'/', '\\', ',', '\u0000', ':', '"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '='};

        for (char c : invalidChars) {
            String instanceId = "Is " + c + "illegal";
            assertFalse(JoinGroupRequest.containsValidPattern(instanceId));
        }
    }

    @Test
    public void testRequestVersionCompatibilityFailBuild() {
        assertThrows(UnsupportedVersionException.class, () -> new JoinGroupRequest.Builder(
            new JoinGroupRequestData()
                .setGroupId("groupId")
                .setMemberId("consumerId")
                .setGroupInstanceId("groupInstanceId")
                .setProtocolType("consumer")
        ).build((short) 4));
    }

    @Test
    public void testRebalanceTimeoutDefaultsToSessionTimeoutV0() {
        int sessionTimeoutMs = 30000;

        Struct struct = new JoinGroupRequestData()
                .setGroupId("groupId")
                .setMemberId("consumerId")
                .setProtocolType("consumer")
                .setSessionTimeoutMs(sessionTimeoutMs)
                .toStruct((short) 0);

        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();

        JoinGroupRequest request = JoinGroupRequest.parse(buffer, (short) 0);
        assertEquals(sessionTimeoutMs, request.data().sessionTimeoutMs());
        assertEquals(sessionTimeoutMs, request.data().rebalanceTimeoutMs());
    }
}
