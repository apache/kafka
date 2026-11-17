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
package org.apache.kafka.common.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.TestSecurityConfig;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.test.TestUtils;

import org.apache.logging.log4j.Level;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class SaslChannelBuilderTest {

    @AfterEach
    public void tearDown() {
        System.clearProperty(SaslChannelBuilder.GSS_NATIVE_PROP);
    }

    @Test
    public void testCloseBeforeConfigureIsIdempotent() {
        SaslChannelBuilder builder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "PLAIN");
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
    }

    @Test
    public void testCloseAfterConfigIsIdempotent() {
        SaslChannelBuilder builder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "PLAIN");
        builder.configure(new HashMap<>());
        assertNotNull(builder.loginManagers().get("PLAIN"));
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
    }

    @Test
    public void testLoginManagerReleasedIfConfigureThrowsException() {
        SaslChannelBuilder builder = createChannelBuilder(SecurityProtocol.SASL_SSL, "PLAIN");
        assertThrows(KafkaException.class, () -> builder.configure(Collections.singletonMap(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "1")));
        assertTrue(builder.loginManagers().isEmpty());
        builder.close();
        assertTrue(builder.loginManagers().isEmpty());
    }

    @Test
    public void testNativeGssapiCredentials() throws Exception {
        System.setProperty(SaslChannelBuilder.GSS_NATIVE_PROP, "true");

        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", TestGssapiLoginModule.class.getName(), new HashMap<>());
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        Map<String, JaasContext> jaasContexts = Collections.singletonMap("GSSAPI", jaasContext);
        GSSManager gssManager = Mockito.mock(GSSManager.class);
        GSSName gssName = Mockito.mock(GSSName.class);
        Mockito.when(gssManager.createName(Mockito.anyString(), Mockito.any()))
                .thenAnswer(unused -> gssName);
        Oid oid = new Oid("1.2.840.113554.1.2.2");
        Mockito.when(gssManager.createCredential(gssName, GSSContext.INDEFINITE_LIFETIME, oid, GSSCredential.ACCEPT_ONLY))
                .thenAnswer(unused -> Mockito.mock(GSSCredential.class));

        SaslChannelBuilder channelBuilder1 = createGssapiChannelBuilder(jaasContexts, gssManager);
        assertEquals(1, channelBuilder1.subject("GSSAPI").getPrincipals().size());
        assertEquals(1, channelBuilder1.subject("GSSAPI").getPrivateCredentials().size());

        SaslChannelBuilder channelBuilder2 = createGssapiChannelBuilder(jaasContexts, gssManager);
        assertEquals(1, channelBuilder2.subject("GSSAPI").getPrincipals().size());
        assertEquals(1, channelBuilder2.subject("GSSAPI").getPrivateCredentials().size());
        assertSame(channelBuilder1.subject("GSSAPI"), channelBuilder2.subject("GSSAPI"));

        Mockito.verify(gssManager, Mockito.times(1))
                .createCredential(gssName, GSSContext.INDEFINITE_LIFETIME, oid, GSSCredential.ACCEPT_ONLY);
    }

    /**
     * Verify that unparsed broker configs don't break clients. This is to ensure that clients
     * created by brokers are not broken if broker configs are passed to clients.
     */
    @Test
    public void testClientChannelBuilderWithBrokerConfigs() throws Exception {
        CertStores certStores = new CertStores(false, "client", "localhost");
        Map<String, Object> configs = new HashMap<>(certStores.getTrustingConfig(certStores));
        configs.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
        configs.putAll(new ConfigDef().withClientSaslSupport().parse(configs));
        for (Field field : BrokerSecurityConfigs.class.getFields()) {
            if (field.getName().endsWith("_CONFIG"))
                configs.put(field.get(BrokerSecurityConfigs.class).toString(), "somevalue");
        }

        SaslChannelBuilder plainBuilder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "PLAIN");
        plainBuilder.configure(configs);

        SaslChannelBuilder gssapiBuilder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "GSSAPI");
        gssapiBuilder.configure(configs);

        SaslChannelBuilder oauthBearerBuilder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "OAUTHBEARER");
        oauthBearerBuilder.configure(configs);

        SaslChannelBuilder scramBuilder = createChannelBuilder(SecurityProtocol.SASL_PLAINTEXT, "SCRAM-SHA-256");
        scramBuilder.configure(configs);

        SaslChannelBuilder saslSslBuilder = createChannelBuilder(SecurityProtocol.SASL_SSL, "PLAIN");
        saslSslBuilder.configure(configs);
    }

    @Test
    public void testUnsecuredOAuthBearerValidatorLogsWarning() {
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(SaslChannelBuilder.class)) {
            appender.setClassLogger(SaslChannelBuilder.class, Level.WARN);
            createOAuthBearerServerChannelBuilder(new HashMap<>()).close();
            assertEquals(List.of("Using the unsecured OAUTHBEARER validator, which accepts unsigned tokens and must not be used in production, " +
                    "because listener.name.sasl_plaintext.oauthbearer.sasl.server.callback.handler.class is not set. " +
                    "Set it to a secure validator such as org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler."),
                appender.getMessages(Level.WARN));
        }
    }

    @Test
    public void testConfiguredOAuthBearerValidatorDoesNotLogWarning() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("oauthbearer." + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG,
            OAuthBearerUnsecuredValidatorCallbackHandler.class);
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(SaslChannelBuilder.class)) {
            appender.setClassLogger(SaslChannelBuilder.class, Level.WARN);
            createOAuthBearerServerChannelBuilder(configs).close();
            assertEquals(List.of(), appender.getMessages(Level.WARN));
        }
    }

    private SaslChannelBuilder createOAuthBearerServerChannelBuilder(Map<String, Object> configs) {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", OAuthBearerLoginModule.class.getName(),
            Collections.singletonMap("unsecuredLoginStringClaim_sub", "admin"));
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        SaslChannelBuilder channelBuilder = new SaslChannelBuilder(ConnectionMode.SERVER,
            Collections.singletonMap(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaasContext), SecurityProtocol.SASL_PLAINTEXT,
            new ListenerName("SASL_PLAINTEXT"), false, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
            null, null, null, Time.SYSTEM, new LogContext(), defaultApiVersionsSupplier());
        Map<String, Object> allConfigs = new HashMap<>(new TestSecurityConfig(new HashMap<>()).values());
        allConfigs.putAll(configs);
        channelBuilder.configure(allConfigs);
        return channelBuilder;
    }

    private SaslChannelBuilder createGssapiChannelBuilder(Map<String, JaasContext> jaasContexts, GSSManager gssManager) {
        SaslChannelBuilder channelBuilder = new SaslChannelBuilder(ConnectionMode.SERVER, jaasContexts,
            SecurityProtocol.SASL_PLAINTEXT, new ListenerName("GSSAPI"), false, "GSSAPI",
            null, null, null, Time.SYSTEM, new LogContext(), defaultApiVersionsSupplier()) {

            @Override
            protected GSSManager gssManager() {
                return gssManager;
            }
        };
        Map<String, Object> props = Collections.singletonMap(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
        channelBuilder.configure(new TestSecurityConfig(props).values());
        return channelBuilder;
    }

    private Function<Short, ApiVersionsResponse> defaultApiVersionsSupplier() {
        return version -> TestUtils.defaultApiVersionsResponse(ApiMessageType.ListenerType.BROKER);
    }

    private SaslChannelBuilder createChannelBuilder(SecurityProtocol securityProtocol, String saslMechanism) {
        Class<?> loginModule;
        switch (saslMechanism) {
            case "PLAIN":
                loginModule = PlainLoginModule.class;
                break;
            case "SCRAM-SHA-256":
                loginModule = ScramLoginModule.class;
                break;
            case "OAUTHBEARER":
                loginModule = OAuthBearerLoginModule.class;
                break;
            case "GSSAPI":
                loginModule = TestGssapiLoginModule.class;
                break;
            default:
                throw new IllegalArgumentException("Unsupported SASL mechanism " + saslMechanism);
        }
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", loginModule.getName(), new HashMap<>());
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        Map<String, JaasContext> jaasContexts = Collections.singletonMap(saslMechanism, jaasContext);
        return new SaslChannelBuilder(ConnectionMode.CLIENT, jaasContexts, securityProtocol, new ListenerName(saslMechanism),
                false, saslMechanism, null,
                null, null, Time.SYSTEM, new LogContext(), defaultApiVersionsSupplier());
    }

    public static final class TestGssapiLoginModule implements LoginModule {
        private Subject subject;

        @Override
        public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
            this.subject = subject;
        }

        @Override
        public boolean login() throws LoginException {
            subject.getPrincipals().add(new KafkaPrincipal("User", "kafka@kafka1.example.com"));
            return true;
        }

        @Override
        public boolean commit() throws LoginException {
            return true;
        }

        @Override
        public boolean abort() throws LoginException {
            return true;
        }

        @Override
        public boolean logout() {
            return true;
        }
    }
}
