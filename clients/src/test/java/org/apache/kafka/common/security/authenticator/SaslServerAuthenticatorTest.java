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

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.network.InvalidReceiveException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.JaasContext;
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

import static org.apache.kafka.common.security.scram.internals.ScramMechanism.SCRAM_SHA_256;
import static org.junit.Assert.fail;

public class SaslServerAuthenticatorTest {

    @Test(expected = InvalidReceiveException.class)
    public void testOversizeRequest() throws IOException {
        TransportLayer transportLayer = EasyMock.mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer, SCRAM_SHA_256.mechanismName());

        final Capture<ByteBuffer> size = EasyMock.newCapture();
        EasyMock.expect(transportLayer.read(EasyMock.capture(size))).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                size.getValue().putInt(SaslServerAuthenticator.MAX_RECEIVE_SIZE + 1);
                return 4;
            }
        });

        EasyMock.replay(transportLayer);

        authenticator.authenticate();
    }

    @Test
    public void testUnexpectedRequestType() throws IOException {
        TransportLayer transportLayer = EasyMock.mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer, SCRAM_SHA_256.mechanismName());

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 0, "clientId", 13243);
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

        try {
            authenticator.authenticate();
            fail("Expected authenticate() to raise an exception");
        } catch (IllegalSaslStateException e) {
            // expected exception
        }
    }

    private SaslServerAuthenticator setupAuthenticator(Map<String, ?> configs, TransportLayer transportLayer, String mechanism) throws IOException {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), new HashMap<String, Object>());
        Map<String, JaasContext> jaasContexts = Collections.singletonMap(mechanism,
                new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null));
        Map<String, Subject> subjects = Collections.singletonMap(mechanism, new Subject());
        Map<String, AuthenticateCallbackHandler> callbackHandlers = Collections.<String, AuthenticateCallbackHandler>singletonMap(
                mechanism, new SaslServerCallbackHandler());
        return new SaslServerAuthenticator(configs, callbackHandlers, "node", subjects, null,
                new ListenerName("ssl"), SecurityProtocol.SASL_SSL, transportLayer);
    }

}
