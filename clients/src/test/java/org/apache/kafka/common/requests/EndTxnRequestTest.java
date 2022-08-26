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

import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EndTxnRequestTest {

    @Test
    public void testConstructor() {
        short producerEpoch = 0;
        int producerId = 1;
        String transactionId = "txn_id";
        int throttleTimeMs = 10;

        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(
            new EndTxnRequestData()
                .setCommitted(true)
                .setProducerEpoch(producerEpoch)
                .setProducerId(producerId)
                .setTransactionalId(transactionId));

        for (short version : ApiKeys.END_TXN.allVersions()) {
            EndTxnRequest request = builder.build(version);

            EndTxnResponse response = request.getErrorResponse(throttleTimeMs, Errors.NOT_COORDINATOR.exception());

            assertEquals(Collections.singletonMap(Errors.NOT_COORDINATOR, 1), response.errorCounts());

            assertEquals(TransactionResult.COMMIT, request.result());

            assertEquals(throttleTimeMs, response.throttleTimeMs());
        }
    }
}
