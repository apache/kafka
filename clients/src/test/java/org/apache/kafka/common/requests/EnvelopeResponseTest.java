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

import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnvelopeResponseTest {

    @Test
    public void testToSend() {
        for (short version = ApiKeys.ENVELOPE.oldestVersion(); version <= ApiKeys.ENVELOPE.latestVersion(); version++) {
            ByteBuffer responseData = ByteBuffer.wrap("foobar".getBytes());
            EnvelopeResponse response = new EnvelopeResponse(responseData, Errors.NONE);
            short headerVersion = ApiKeys.ENVELOPE.responseHeaderVersion(version);
            ResponseHeader header = new ResponseHeader(15, headerVersion);

            Send send = response.toSend(header, version);
            ByteBuffer buffer = TestUtils.toBuffer(send);
            assertEquals(send.size() - 4, buffer.getInt());
            assertEquals(header, ResponseHeader.parse(buffer, headerVersion));

            EnvelopeResponseData parsedResponseData = new EnvelopeResponseData();
            parsedResponseData.read(new ByteBufferAccessor(buffer), version);
            assertEquals(response.data(), parsedResponseData);
        }
    }

}
