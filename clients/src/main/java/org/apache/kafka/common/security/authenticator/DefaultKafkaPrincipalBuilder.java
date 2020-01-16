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

import javax.security.auth.x500.X500Principal;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.sasl.SaslServer;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;

import java.io.Closeable;
import java.io.IOException;
import java.security.Principal;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of {@link KafkaPrincipalBuilder} which provides basic support for
 * SSL authentication and SASL authentication. In the latter case, when GSSAPI is used, this
 * class applies {@link org.apache.kafka.common.security.kerberos.KerberosShortNamer} to transform
 * the name.
 *
 * NOTE: This is an internal class and can change without notice. Unlike normal implementations
 * of {@link KafkaPrincipalBuilder}, there is no default no-arg constructor since this class
 * must adapt implementations of the older {@link org.apache.kafka.common.security.auth.PrincipalBuilder} interface.
 */
public class DefaultKafkaPrincipalBuilder implements KafkaPrincipalBuilder, Closeable {
    // Use FQN to avoid import deprecation warnings
    @SuppressWarnings("deprecation")
    private final org.apache.kafka.common.security.auth.PrincipalBuilder oldPrincipalBuilder;
    private final Authenticator authenticator;
    private final TransportLayer transportLayer;
    private final KerberosShortNamer kerberosShortNamer;
    private final SslPrincipalMapper sslPrincipalMapper;

    /**
     * Construct a new instance which wraps an instance of the older {@link org.apache.kafka.common.security.auth.PrincipalBuilder}.
     *
     * @param authenticator The authenticator in use
     * @param transportLayer The underlying transport layer
     * @param oldPrincipalBuilder Instance of {@link org.apache.kafka.common.security.auth.PrincipalBuilder}
     * @param kerberosShortNamer Kerberos name rewrite rules or null if none have been configured
     */
    @SuppressWarnings("deprecation")
    public static DefaultKafkaPrincipalBuilder fromOldPrincipalBuilder(Authenticator authenticator,
                                                                       TransportLayer transportLayer,
                                                                       org.apache.kafka.common.security.auth.PrincipalBuilder oldPrincipalBuilder,
                                                                       KerberosShortNamer kerberosShortNamer) {
        return new DefaultKafkaPrincipalBuilder(
                requireNonNull(authenticator),
                requireNonNull(transportLayer),
                requireNonNull(oldPrincipalBuilder),
                kerberosShortNamer,
                null);
    }

    @SuppressWarnings("deprecation")
    private DefaultKafkaPrincipalBuilder(Authenticator authenticator,
                                         TransportLayer transportLayer,
                                         org.apache.kafka.common.security.auth.PrincipalBuilder oldPrincipalBuilder,
                                         KerberosShortNamer kerberosShortNamer,
                                         SslPrincipalMapper sslPrincipalMapper) {
        this.authenticator = authenticator;
        this.transportLayer = transportLayer;
        this.oldPrincipalBuilder = oldPrincipalBuilder;
        this.kerberosShortNamer = kerberosShortNamer;
        this.sslPrincipalMapper =  sslPrincipalMapper;
    }

    /**
     * Construct a new instance.
     *
     * @param kerberosShortNamer Kerberos name rewrite rules or null if none have been configured
     * @param sslPrincipalMapper SSL Principal mapper or null if none have been configured
     */
    public DefaultKafkaPrincipalBuilder(KerberosShortNamer kerberosShortNamer, SslPrincipalMapper sslPrincipalMapper) {
        this(null, null, null, kerberosShortNamer, sslPrincipalMapper);
    }

    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        if (context instanceof PlaintextAuthenticationContext) {
            if (oldPrincipalBuilder != null)
                return convertToKafkaPrincipal(oldPrincipalBuilder.buildPrincipal(transportLayer, authenticator));

            return KafkaPrincipal.ANONYMOUS;
        } else if (context instanceof SslAuthenticationContext) {
            SSLSession sslSession = ((SslAuthenticationContext) context).session();

            if (oldPrincipalBuilder != null)
                return convertToKafkaPrincipal(oldPrincipalBuilder.buildPrincipal(transportLayer, authenticator));

            try {
                return applySslPrincipalMapper(sslSession.getPeerPrincipal());
            } catch (SSLPeerUnverifiedException se) {
                return KafkaPrincipal.ANONYMOUS;
            }
        } else if (context instanceof SaslAuthenticationContext) {
            SaslServer saslServer = ((SaslAuthenticationContext) context).server();
            if (SaslConfigs.GSSAPI_MECHANISM.equals(saslServer.getMechanismName()))
                return applyKerberosShortNamer(saslServer.getAuthorizationID());
            else
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID());
        } else {
            throw new IllegalArgumentException("Unhandled authentication context type: " + context.getClass().getName());
        }
    }

    private KafkaPrincipal applyKerberosShortNamer(String authorizationId) {
        KerberosName kerberosName = KerberosName.parse(authorizationId);
        try {
            String shortName = kerberosShortNamer.shortName(kerberosName);
            return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, shortName);
        } catch (IOException e) {
            throw new KafkaException("Failed to set name for '" + kerberosName +
                    "' based on Kerberos authentication rules.", e);
        }
    }

    private KafkaPrincipal applySslPrincipalMapper(Principal principal) {
        try {
            if (!(principal instanceof X500Principal) || principal == KafkaPrincipal.ANONYMOUS) {
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal.getName());
            } else {
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, sslPrincipalMapper.getName(principal.getName()));
            }
        } catch (IOException e) {
            throw new KafkaException("Failed to map name for '" + principal.getName() +
                    "' based on SSL principal mapping rules.", e);
        }
    }

    private KafkaPrincipal convertToKafkaPrincipal(Principal principal) {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal.getName());
    }

    @Override
    public void close() {
        if (oldPrincipalBuilder != null)
            oldPrincipalBuilder.close();
    }

}
