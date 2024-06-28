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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumConfigTest {

    @Test
    public void testValidControllerQuorumVoters() {
        assertTrue(QuorumConfig.validateControllerQuorumVoters("1@hostname:12345"));
        assertTrue(QuorumConfig.validateControllerQuorumVoters("1-2@hostname:12345")); // with replica directory id
        assertTrue(QuorumConfig.validateControllerQuorumVoters("123@domain.com:54321"));
        assertTrue(QuorumConfig.validateControllerQuorumVoters("10-20@sub.domain.com:8080")); // with replica directory id
    }

    @Test
    public void testInvalidControllerQuorumVoters() {
        assertFalse(QuorumConfig.validateControllerQuorumVoters("1@hostname:123456")); // Port number too long
        assertFalse(QuorumConfig.validateControllerQuorumVoters("1@hostname:")); // Missing port number
        assertFalse(QuorumConfig.validateControllerQuorumVoters("1@hostname")); // Missing port number
        assertFalse(QuorumConfig.validateControllerQuorumVoters("hostname:12345")); // Missing replica id, @
        assertFalse(QuorumConfig.validateControllerQuorumVoters("1-2-3@hostname:12345")); // Invalid replica id range
        assertFalse(QuorumConfig.validateControllerQuorumVoters("1@host name:12345")); // Invalid hostname
        assertFalse(QuorumConfig.validateControllerQuorumVoters("@hostname:12345")); // Missing replica id
    }
}
