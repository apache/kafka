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

import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EnvelopeRequestTest {

    @Test
    public void testGetPrincipal() {
        KafkaPrincipal kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "principal", true);
        DefaultKafkaPrincipalBuilder kafkaPrincipalBuilder = new DefaultKafkaPrincipalBuilder(null, null);

        EnvelopeRequest.Builder requestBuilder = new EnvelopeRequest.Builder(ByteBuffer.allocate(0),
            kafkaPrincipalBuilder.serialize(kafkaPrincipal), "client-address".getBytes());
        EnvelopeRequest request = requestBuilder.build(EnvelopeRequestData.HIGHEST_SUPPORTED_VERSION);
        assertEquals(kafkaPrincipal, kafkaPrincipalBuilder.deserialize(request.requestPrincipal()));
    }

    @Test
    public void testToSend() throws IOException {
        for (short version = ApiKeys.ENVELOPE.oldestVersion(); version <= ApiKeys.ENVELOPE.latestVersion(); version++) {
            ByteBuffer requestData = ByteBuffer.wrap("foobar".getBytes());
            RequestHeader header = new RequestHeader(ApiKeys.ENVELOPE, version, "clientId", 15);
            EnvelopeRequest request = new EnvelopeRequest.Builder(
                requestData,
                "principal".getBytes(),
                InetAddress.getLocalHost().getAddress()
            ).build(version);

            Send send = request.toSend(header);
            ByteBuffer buffer = TestUtils.toBuffer(send);
            assertEquals(send.size() - 4, buffer.getInt());
            assertEquals(header, RequestHeader.parse(buffer));

            EnvelopeRequestData parsedRequestData = new EnvelopeRequestData();
            parsedRequestData.read(new ByteBufferAccessor(buffer), version);
            assertEquals(request.data(), parsedRequestData);
        }
    }

}
