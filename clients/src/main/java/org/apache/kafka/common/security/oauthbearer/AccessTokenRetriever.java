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

package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.FileTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.HttpAccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;

import javax.net.ssl.SSLSocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;

/**
 * An <code>AccessTokenRetriever</code> is the internal API by which the login module will
 * retrieve an access token for use in authorization by the broker. The implementation may
 * involve authentication to a remote system, or it can be as simple as loading the contents
 * of a file or configuration setting.
 *
 * <i>Retrieval</i> is a separate concern from <i>validation</i>, so it isn't necessary for
 * the <code>AccessTokenRetriever</code> implementation to validate the integrity of the JWT
 * access token.
 *
 * @see HttpAccessTokenRetriever
 * @see FileTokenRetriever
 */

public interface AccessTokenRetriever extends Initable, Closeable {

    /**
     * Retrieves a JWT access token in its serialized three-part form. The implementation
     * is free to determine how it should be retrieved but should not perform validation
     * on the result.
     *
     * <b>Note</b>: This is a blocking function and callers should be aware that the
     * implementation may be communicating over a network, with the file system, coordinating
     * threads, etc. The facility in the {@link javax.security.auth.spi.LoginModule} from
     * which this is ultimately called does not provide an asynchronous approach.
     *
     * @return Non-<code>null</code> JWT access token string
     *
     * @throws IOException Thrown on errors related to IO during retrieval
     */

    String retrieve() throws IOException;

    /**
     * Lifecycle method to perform a clean shutdown of the retriever. This must
     * be performed by the caller to ensure the correct state, freeing up and releasing any
     * resources performed in {@link #init()}.
     *
     * @throws IOException Thrown on errors related to IO during closure
     */

    default void close() throws IOException {
        // This method left intentionally blank.
    }

    /**
     * Create an {@link AccessTokenRetriever} from the given SASL and JAAS configuration.
     *
     * <b>Note</b>: the returned <code>AccessTokenRetriever</code> is <em>not</em> initialized
     * here and must be done by the caller prior to use.
     *
     * @param configs    SASL configuration
     * @param jaasConfig JAAS configuration
     *
     * @return Non-<code>null</code> {@link AccessTokenRetriever}
     */

    static AccessTokenRetriever create(Map<String, ?> configs, Map<String, Object> jaasConfig) {
        return create(configs, null, jaasConfig);
    }

    static AccessTokenRetriever create(Map<String, ?> configs, String saslMechanism, Map<String, Object> jaasConfig) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL tokenEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (tokenEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            return new FileTokenRetriever(cu.validateFile(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL));
        } else {
            JaasOptionsUtils jou = new JaasOptionsUtils(jaasConfig);
            String clientId = jou.validateString(CLIENT_ID_CONFIG);
            String clientSecret = jou.validateString(CLIENT_SECRET_CONFIG);
            String scope = jou.validateString(SCOPE_CONFIG, false);

            SSLSocketFactory sslSocketFactory = null;

            if (jou.shouldCreateSSLSocketFactory(tokenEndpointUrl))
                sslSocketFactory = jou.createSSLSocketFactory();

            boolean urlencodeHeader = HttpAccessTokenRetriever.validateUrlencodeHeader(cu);

            return new HttpAccessTokenRetriever(clientId,
                clientSecret,
                scope,
                sslSocketFactory,
                tokenEndpointUrl.toString(),
                cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS),
                cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS),
                cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false),
                cu.validateInteger(SASL_LOGIN_READ_TIMEOUT_MS, false),
                urlencodeHeader);
        }
    }

}
