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

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.Mode;

/**
 * Callback handler for Sasl clients. The callbacks required for the SASL mechanism
 * configured for the client should be supported by this callback handler. See
 * <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html">Java SASL API</a>
 * for the list of SASL callback handlers required for each SASL mechanism.
 */
public class SaslClientCallbackHandler implements AuthCallbackHandler {

    private boolean isKerberos;
    private Subject subject;

    @Override
    public void configure(Map<String, ?> configs, Mode mode, Subject subject, String mechanism) {
        this.isKerberos = mechanism.equals(SaslConfigs.GSSAPI_MECHANISM);
        this.subject = subject;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nc = (NameCallback) callback;
                if (!isKerberos && subject != null && !subject.getPublicCredentials(String.class).isEmpty()) {
                    nc.setName(subject.getPublicCredentials(String.class).iterator().next());
                } else
                    nc.setName(nc.getDefaultName());
            } else if (callback instanceof PasswordCallback) {
                if (!isKerberos && subject != null && !subject.getPrivateCredentials(String.class).isEmpty()) {
                    char[] password = subject.getPrivateCredentials(String.class).iterator().next().toCharArray();
                    ((PasswordCallback) callback).setPassword(password);
                } else {
                    String errorMessage = "Could not login: the client is being asked for a password, but the Kafka" +
                             " client code does not currently support obtaining a password from the user.";
                    if (isKerberos) {
                        errorMessage += " Make sure -Djava.security.auth.login.config property passed to JVM and" +
                             " the client is configured to use a ticket cache (using" +
                             " the JAAS configuration setting 'useTicketCache=true)'. Make sure you are using" +
                             " FQDN of the Kafka broker you are trying to connect to.";
                    }
                    throw new UnsupportedCallbackException(callback, errorMessage);
                }
            } else if (callback instanceof RealmCallback) {
                RealmCallback rc = (RealmCallback) callback;
                rc.setText(rc.getDefaultText());
            } else if (callback instanceof AuthorizeCallback) {
                AuthorizeCallback ac = (AuthorizeCallback) callback;
                String authId = ac.getAuthenticationID();
                String authzId = ac.getAuthorizationID();
                ac.setAuthorized(authId.equals(authzId));
                if (ac.isAuthorized())
                    ac.setAuthorizedID(authzId);
            } else {
                throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
            }
        }
    }

    @Override
    public void close() {
    }
}
