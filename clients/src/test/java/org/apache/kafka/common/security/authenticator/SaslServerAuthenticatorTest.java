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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.network.InvalidReceiveException;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.security.scram.ScramMechanism.SCRAM_SHA_256;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SaslServerAuthenticatorTest {

    @Test(expected = InvalidReceiveException.class)
    public void testOversizeRequest() throws IOException {
        SaslServerAuthenticator authenticator = setupAuthenticator();
        TransportLayer transportLayer = EasyMock.mock(TransportLayer.class);
        PrincipalBuilder principalBuilder = null; // SASL authenticator does not currently use principal builder
        Map<String, ?> configs = Collections.singletonMap(SaslConfigs.SASL_ENABLED_MECHANISMS,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));

        final Capture<ByteBuffer> size = EasyMock.newCapture();
        EasyMock.expect(transportLayer.read(EasyMock.capture(size))).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                size.getValue().putInt(SaslServerAuthenticator.MAX_RECEIVE_SIZE + 1);
                return 4;
            }
        });

        EasyMock.replay(transportLayer);

        authenticator.configure(transportLayer, principalBuilder, configs);
        authenticator.authenticate();
    }

    @Test
    public void testUnexpectedRequestType() throws IOException {
        SaslServerAuthenticator authenticator = setupAuthenticator();
        TransportLayer transportLayer = EasyMock.mock(TransportLayer.class);
        PrincipalBuilder principalBuilder = null; // SASL authenticator does not currently use principal builder
        Map<String, ?> configs = Collections.singletonMap(SaslConfigs.SASL_ENABLED_MECHANISMS,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA.id, (short) 0, "clientId", 13243);
        final Struct headerStruct = header.toStruct();

        final Capture<ByteBuffer> size = EasyMock.newCapture();
        EasyMock.expect(transportLayer.read(EasyMock.capture(size))).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                size.getValue().putInt(headerStruct.sizeOf());
                return 4;
            }
        });

        final Capture<ByteBuffer> payload = EasyMock.newCapture();
        EasyMock.expect(transportLayer.read(EasyMock.capture(payload))).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                // serialize only the request header. the authenticator should not parse beyond this
                headerStruct.writeTo(payload.getValue());
                return headerStruct.sizeOf();
            }
        });

        EasyMock.replay(transportLayer);

        authenticator.configure(transportLayer, principalBuilder, configs);
        try {
            authenticator.authenticate();
            fail("Expected authenticate() to raise an exception");
        } catch (IOException e) {
            assertTrue(e.getCause() instanceof IllegalSaslStateException);
        }
    }

    private SaslServerAuthenticator setupAuthenticator() throws IOException {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), new HashMap<String, Object>());
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig);
        Subject subject = new Subject();
        return new SaslServerAuthenticator("node", jaasContext, subject, null, "localhost", new CredentialCache());
    }

}
