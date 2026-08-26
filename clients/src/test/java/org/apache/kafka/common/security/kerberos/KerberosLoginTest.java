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
package org.apache.kafka.common.security.kerberos;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KerberosLoginTest {

    private static final String CONTEXT_NAME = "KafkaClient";
    private static final String PASSWORD = "secret";

    private KerberosLogin kerberosLogin;

    @BeforeEach
    public void setUp() {
        PasswordPromptingLoginModule.reset();
    }

    @AfterEach
    public void tearDown() {
        if (kerberosLogin != null) {
            kerberosLogin.close();
        }
    }

    /**
     * sasl.login.callback.handler.class is used for the initial login. Re-login must pass the same
     * handler into LoginContext so password-based Kerberos authentication still works when the TGT
     * is refreshed.
     */
    @Test
    public void testReloginUsesLoginCallbackHandler() throws Exception {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.createOrUpdateEntry(CONTEXT_NAME, PasswordPromptingLoginModule.class.getName(),
                Map.of("principal", "client@EXAMPLE.COM"));

        RecordingLoginCallbackHandler callbackHandler = new RecordingLoginCallbackHandler(PASSWORD);
        kerberosLogin = new KerberosLogin();
        kerberosLogin.configure(kerberosConfigs(), CONTEXT_NAME, jaasConfig, callbackHandler);

        kerberosLogin.login();
        assertEquals(1, callbackHandler.handleCount());
        assertNotNull(kerberosLogin.subject());

        assertDoesNotThrow(() -> kerberosLogin.reLogin());
        assertEquals(2, callbackHandler.handleCount(),
                "LoginCallbackHandler must be used when Kerberos re-logins after ticket expiry");
        assertEquals(2, PasswordPromptingLoginModule.loginCount());
    }

    private static Map<String, Object> kerberosConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
        configs.put(SaslConfigs.SASL_KERBEROS_KINIT_CMD, SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD);
        configs.put(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
                SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR);
        configs.put(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, 0.0);
        configs.put(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, 0L);
        return configs;
    }

    public static final class RecordingLoginCallbackHandler implements AuthenticateCallbackHandler {
        private final String password;
        private final AtomicInteger handleCount = new AtomicInteger();

        RecordingLoginCallbackHandler(String password) {
            this.password = password;
        }

        int handleCount() {
            return handleCount.get();
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        }

        @Override
        public void handle(Callback[] callbacks) {
            handleCount.incrementAndGet();
            for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(password.toCharArray());
                }
            }
        }

        @Override
        public void close() {
        }
    }

    /**
     * Login module that requires a PasswordCallback, matching password-based Krb5LoginModule.
     * It plants a long-lived TGT so KerberosLogin treats the subject as a Kerberos login.
     */
    public static final class PasswordPromptingLoginModule implements LoginModule {
        private static final AtomicInteger LOGIN_COUNT = new AtomicInteger();

        private Subject subject;
        private CallbackHandler callbackHandler;
        private KerberosPrincipal client;
        private KerberosTicket ticket;

        static void reset() {
            LOGIN_COUNT.set(0);
        }

        static int loginCount() {
            return LOGIN_COUNT.get();
        }

        @Override
        public void initialize(Subject subject,
                               CallbackHandler callbackHandler,
                               Map<String, ?> sharedState,
                               Map<String, ?> options) {
            this.subject = subject;
            this.callbackHandler = callbackHandler;
        }

        @Override
        public boolean login() throws LoginException {
            if (callbackHandler == null) {
                throw new LoginException("Callback handler is required to obtain the password");
            }
            PasswordCallback passwordCallback = new PasswordCallback("password: ", false);
            try {
                callbackHandler.handle(new Callback[] {passwordCallback});
            } catch (IOException | UnsupportedCallbackException e) {
                throw new LoginException("Failed to obtain password: " + e.getMessage());
            }
            if (passwordCallback.getPassword() == null) {
                throw new LoginException("Password was not provided by the callback handler");
            }
            LOGIN_COUNT.incrementAndGet();
            return true;
        }

        @Override
        public boolean commit() {
            client = new KerberosPrincipal("client@EXAMPLE.COM");
            KerberosPrincipal server = new KerberosPrincipal("krbtgt/EXAMPLE.COM@EXAMPLE.COM");
            Date now = new Date();
            Date end = new Date(now.getTime() + TimeUnit.HOURS.toMillis(1));
            ticket = new KerberosTicket(new byte[] {1}, client, server, new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
                    1, new boolean[32], now, now, end, end, null);
            subject.getPrincipals().add(client);
            subject.getPrivateCredentials().add(ticket);
            return true;
        }

        @Override
        public boolean abort() {
            return true;
        }

        @Override
        public boolean logout() {
            if (subject != null) {
                if (client != null) {
                    subject.getPrincipals().remove(client);
                }
                if (ticket != null) {
                    subject.getPrivateCredentials().remove(ticket);
                }
            }
            return true;
        }
    }
}
