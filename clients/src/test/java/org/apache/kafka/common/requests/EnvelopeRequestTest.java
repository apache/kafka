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
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class EnvelopeRequestTest {

    @Test
    public void testGetPrincipal() {
        KafkaPrincipal kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "principal", true);
        DefaultKafkaPrincipalBuilder kafkaPrincipalBuilder = new DefaultKafkaPrincipalBuilder(null, null);

        EnvelopeRequest.Builder requestBuilder = new EnvelopeRequest.Builder(ByteBuffer.allocate(0),
            kafkaPrincipalBuilder.serialize(kafkaPrincipal), "client-address".getBytes());
        EnvelopeRequest request = requestBuilder.build(EnvelopeRequestData.HIGHEST_SUPPORTED_VERSION);
        assertEquals(kafkaPrincipal, kafkaPrincipalBuilder.deserialize(request.principalData()));
    }
}
