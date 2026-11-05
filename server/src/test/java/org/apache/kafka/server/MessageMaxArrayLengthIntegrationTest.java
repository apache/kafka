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
package org.apache.kafka.server;

import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.api.AfterEach;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageMaxArrayLengthIntegrationTest {

    @AfterEach
    public void tearDown() {
        MessageUtil.setMaxArrayLength(MessageUtil.MAX_ARRAY_LENGTH);
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
    public void testDefaultMaxArrayLength() {
        assertEquals(MessageUtil.MAX_ARRAY_LENGTH, MessageUtil.maxArrayLength());
    }

    @ClusterTest(
        types = {Type.KRAFT, Type.CO_KRAFT},
        serverProperties = @ClusterConfigProperty(key = ServerConfigs.MESSAGE_MAX_ARRAY_LENGTH_CONFIG, value = "2000000")
    )
    public void testMaxArrayLengthFromServerProperties() {
        assertEquals(2000000, MessageUtil.maxArrayLength());
    }
}
