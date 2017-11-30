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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramMechanism;
import org.apache.kafka.common.security.scram.ScramSaslServer;
import org.apache.kafka.common.security.scram.ScramServerCallbackHandler;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.security.scram.ScramMechanism.SCRAM_SHA_256;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SaslServerAuthenticatorTest {

    @Test(expected = InvalidReceiveException.class)
    public void testOversizeRequest() throws IOException {
        TransportLayer transportLayer = EasyMock.mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer);

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
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer);

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

    public static class TestScramSaslServer extends ScramSaslServer {

        public TestScramSaslServer(Map<String, ?> props, CallbackHandler cbh) throws NoSuchAlgorithmException {
            super(ScramMechanism.SCRAM_SHA_256, props, cbh);
            assertNotNull("JAAS context was null", ((ScramServerCallbackHandler) cbh).jaasContext());
        }
    };

    public static class TestScramSaslServerFactory implements SaslServerFactory {

        @Override
        public SaslServer createSaslServer(String mechanism, String protocol,
                                           String serverName, Map<String, ?> props, CallbackHandler cbh)
                throws SaslException {
            final JaasContext context = ((ScramServerCallbackHandler) cbh).jaasContext();
            assertNotNull("JAAS Context should be available", context);
            try {
                return new TestScramSaslServer(props, cbh);
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException("", e);
            }
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            return new String[]{"SCRAM-SHA-256"};
        }
    }

    @Test
    public void testScramSaslServerCanAccessJaasContext() throws Exception {
        class ScramSaslServerCanAccessJaasContextProvider extends Provider {

            protected ScramSaslServerCanAccessJaasContextProvider() {
                super("Test SCRAM SASL Server Provider", 1.0,
                        "This is for testing that a custom SCRAM SaslServer can access the JAAS context");
                put("SaslServerFactory.SCRAM-SHA-256", TestScramSaslServerFactory.class.getName());
            }
        }

        long start = System.currentTimeMillis();
        final ScramSaslServerCanAccessJaasContextProvider provider = new ScramSaslServerCanAccessJaasContextProvider();
        while (true) {
            synchronized (Security.class) {
                Security.insertProviderAt(provider, 0);
                try {
                    TransportLayer transportLayer = EasyMock.mock(TransportLayer.class);
                    Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                            Collections.singletonList(SCRAM_SHA_256.mechanismName()));

                    Socket socket = EasyMock.mock(Socket.class);
                    EasyMock.expect(socket.getLocalAddress()).andReturn(InetAddress.getByName("127.0.0.1"));

                    SocketChannel socketChannel = EasyMock.mock(SocketChannel.class);
                    EasyMock.expect(socketChannel.socket()).andReturn(socket);

                    EasyMock.expect(transportLayer.socketChannel()).andReturn(socketChannel);

                    EasyMock.replay(transportLayer, socketChannel, socket);

                    SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer);
                    final Method createSaslServerMethod = SaslServerAuthenticator.class.getDeclaredMethod("createSaslServer", String.class);
                    createSaslServerMethod.setAccessible(true);
                    createSaslServerMethod.invoke(authenticator, "SCRAM-SHA-256");
                    final Field saslServerField = SaslServerAuthenticator.class.getDeclaredField("saslServer");
                    saslServerField.setAccessible(true);

                    final Object fieldValue = saslServerField.get(authenticator);

                    if (fieldValue instanceof TestScramSaslServer) {
                        // It can happen that other tests can interfere with this one because of the
                        // global nature of the installed security providers,
                        // so we loop until this test passes
                        break;
                    }
                    if (System.currentTimeMillis()-start > TimeUnit.MINUTES.toMillis(2)) {
                        fail("Timeout");
                    }
                } finally {
                    Security.removeProvider(provider.getName());
                }
            }
        }
    }

    private SaslServerAuthenticator setupAuthenticator(Map<String, ?> configs, TransportLayer transportLayer) throws IOException {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), new HashMap<String, Object>());
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig);
        Subject subject = new Subject();
        return new SaslServerAuthenticator(configs, "node", jaasContext, subject, null, new CredentialCache(),
                new ListenerName("ssl"), SecurityProtocol.SASL_SSL, transportLayer);
    }

}
