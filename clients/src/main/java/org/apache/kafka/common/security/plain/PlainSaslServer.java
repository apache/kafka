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
package org.apache.kafka.common.security.plain;

import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.SaslServerCallbackHandler;

/**
 * Simple SaslServer implementation for SASL/PLAIN. In order to make this implementation
 * fully pluggable, authentication of username/password is fully contained within the
 * server implementation.
 * <p>
 * Valid users with passwords are specified in the Jaas configuration file. Each user
 * is specified with user_<username> as key and <password> as value. This is consistent
 * with Zookeeper Digest-MD5 implementation.
 * <p>
 * To avoid storing clear passwords on disk or to integrate with external authentication
 * servers in production systems, this module can be replaced with a different implementation.
 *
 */
public class PlainSaslServer implements SaslServer {

    public static final String PLAIN_MECHANISM = "PLAIN";
    private static final String JAAS_USER_PREFIX = "user_";
    private static final String JAAS_SOURCE = "source";
    private static final String SOURCE_LDAP = "ldap";
    private static final String JAAS_LDAP_URL = "ldap_url";
    private static final String JAAS_LDAP_USER_TEMPLATE = "ldap_user_template";

    private final JaasContext jaasContext;

    private boolean complete;
    private String authorizationId;
    private String ldapUrl;
    private String ldapUserTemplate;
    private String source;


    public PlainSaslServer(JaasContext jaasContext) {
        this.jaasContext = jaasContext;
        source = jaasContext.configEntryOption(JAAS_SOURCE, PlainLoginModule.class.getName());
        if (source == null) {
            source = "config";
        }
        if (source.equals(SOURCE_LDAP)) {
            ldapUrl = jaasContext.configEntryOption(JAAS_LDAP_URL, PlainLoginModule.class.getName());
            ldapUserTemplate = jaasContext.configEntryOption(JAAS_LDAP_USER_TEMPLATE, PlainLoginModule.class.getName());
        }

    }

    /**
     * @throws SaslAuthenticationException if username/password combination is invalid or if the requested
     *         authorization id is not the same as username.
     * <p>
     * <b>Note:</b> This method may throw {@link SaslAuthenticationException} to provide custom error messages
     * to clients. But care should be taken to avoid including any information in the exception message that
     * should not be leaked to unauthenticated clients. It may be safer to throw {@link SaslException} in
     * some cases so that a standard error message is returned to clients.
     * </p>
     */
    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException, SaslAuthenticationException {
        /*
         * Message format (from https://tools.ietf.org/html/rfc4616):
         *
         * message   = [authzid] UTF8NUL authcid UTF8NUL passwd
         * authcid   = 1*SAFE ; MUST accept up to 255 octets
         * authzid   = 1*SAFE ; MUST accept up to 255 octets
         * passwd    = 1*SAFE ; MUST accept up to 255 octets
         * UTF8NUL   = %x00 ; UTF-8 encoded NUL character
         *
         * SAFE      = UTF1 / UTF2 / UTF3 / UTF4
         *                ;; any UTF-8 encoded Unicode character except NUL
         */

        String[] tokens;
        try {
            tokens = new String(response, "UTF-8").split("\u0000");
        } catch (UnsupportedEncodingException e) {
            throw new SaslException("UTF-8 encoding not supported", e);
        }
        if (tokens.length != 3)
            throw new SaslException("Invalid SASL/PLAIN response: expected 3 tokens, got " + tokens.length);
        String authorizationIdFromClient = tokens[0];
        String username = tokens[1];
        String password = tokens[2];

        if (username.isEmpty()) {
            throw new SaslException("Authentication failed: username not specified");
        }
        if (password.isEmpty()) {
            throw new SaslException("Authentication failed: password not specified");
        }

        if (source.equals(SOURCE_LDAP)) {
            try {
                Properties props = new Properties();
                props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
                props.put(Context.PROVIDER_URL, ldapUrl);
                props.put(Context.SECURITY_PRINCIPAL, MessageFormat.format(ldapUserTemplate, username));
                props.put(Context.SECURITY_CREDENTIALS, password);
            
                InitialDirContext context = new InitialDirContext(props);
            } catch (Exception e) {
                throw new SaslException("Authentication failed: Invalid JAAS configuration", e);
            }

        } else {
            String expectedPassword = jaasContext.configEntryOption(JAAS_USER_PREFIX + username,
                PlainLoginModule.class.getName());
            if (!password.equals(expectedPassword)) {
                throw new SaslException("Authentication failed: Invalid username or password");
            }
        }

        if (!authorizationIdFromClient.isEmpty() && !authorizationIdFromClient.equals(username))
            throw new SaslAuthenticationException("Authentication failed: Client requested an authorization id that is different from username");

        this.authorizationId = username;

        complete = true;
        return new byte[0];
    }

    @Override
    public String getAuthorizationID() {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return authorizationId;
    }

    @Override
    public String getMechanismName() {
        return PLAIN_MECHANISM;
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return null;
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        if (!complete)
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public void dispose() throws SaslException {
    }

    public static class PlainSaslServerFactory implements SaslServerFactory {

        @Override
        public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh)
            throws SaslException {

            if (!PLAIN_MECHANISM.equals(mechanism))
                throw new SaslException(String.format("Mechanism \'%s\' is not supported. Only PLAIN is supported.", mechanism));

            if (!(cbh instanceof SaslServerCallbackHandler))
                throw new SaslException("CallbackHandler must be of type SaslServerCallbackHandler, but it is: " + cbh.getClass());

            return new PlainSaslServer(((SaslServerCallbackHandler) cbh).jaasContext());
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            if (props == null) return new String[]{PLAIN_MECHANISM};
            String noPlainText = (String) props.get(Sasl.POLICY_NOPLAINTEXT);
            if ("true".equals(noPlainText))
                return new String[]{};
            else
                return new String[]{PLAIN_MECHANISM};
        }
    }
}
