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

import org.apache.kafka.common.message.InitProducerIdRequestData;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InitProducerIdRequestTest {

    private InitProducerIdRequestData baseData() {
        return new InitProducerIdRequestData()
            .setTransactionalId("txn-2pc")
            .setTransactionTimeoutMs(10);
    }

    @Test
    public void testBuilderAllowsV6WhenEnable2PcSet() {
        // enable2Pc is a v6-only field (KIP-939); the builder must allow v6 so
        // NetworkClient can negotiate it against a 2PC-capable broker.
        InitProducerIdRequest.Builder builder =
            new InitProducerIdRequest.Builder(baseData().setEnable2Pc(true));
        assertEquals((short) 6, builder.latestAllowedVersion());
    }

    @Test
    public void testBuilderAllowsV6WhenKeepPreparedTxnSet() {
        InitProducerIdRequest.Builder builder =
            new InitProducerIdRequest.Builder(baseData().setKeepPreparedTxn(true));
        assertEquals((short) 6, builder.latestAllowedVersion());
    }

    @Test
    public void testBuilderCapsAtStableVersionWithout2Pc() {
        // Control: a plain producer keeps the stable cap, unaffected by the fix.
        InitProducerIdRequest.Builder builder = new InitProducerIdRequest.Builder(baseData());
        assertEquals((short) 5, builder.latestAllowedVersion());
    }
}
