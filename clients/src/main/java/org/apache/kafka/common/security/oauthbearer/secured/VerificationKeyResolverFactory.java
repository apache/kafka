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

package org.apache.kafka.common.security.oauthbearer.secured;

import static org.apache.kafka.common.security.oauthbearer.secured.ValidatorCallbackHandlerConfiguration.JWKS_ENDPOINT_URI_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.ValidatorCallbackHandlerConfiguration.JWKS_FILE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.ValidatorCallbackHandlerConfiguration.PEM_DIRECTORY_CONFIG;

import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;
import javax.net.ssl.SSLSocketFactory;
import org.apache.kafka.common.config.ConfigException;
import org.jose4j.http.Get;

public class VerificationKeyResolverFactory {

    /**
     * Create an {@link AccessTokenRetriever} from the given
     * {@link LoginCallbackHandlerConfiguration}.
     *
     * <b>Note</b>: the returned <code>CloseableVerificationKeyResolver</code> is not
     * initialized here and must be done by the caller.
     *
     * Primarily exposed here for unit testing.
     *
     * @param conf Configuration for {@link javax.security.auth.callback.CallbackHandler}
     *
     * @return Non-<code>null</code> <code>AccessTokenRetriever</code>
     */

    public static CloseableVerificationKeyResolver create(ValidatorCallbackHandlerConfiguration conf) {
        String jwksFile = conf.getJwksFile();
        String jwksEndpointUri = conf.getJwksEndpointUri();
        String pemDirectory = conf.getPemDirectory();

        long count = Stream.of(jwksFile, jwksEndpointUri, pemDirectory)
            .filter(Objects::nonNull)
            .count();

        if (count != 1) {
            throw new ConfigException(String.format("The OAuth validator configuration must include only one of %s, %s, or %s options", JWKS_FILE_CONFIG, JWKS_ENDPOINT_URI_CONFIG, PEM_DIRECTORY_CONFIG));
        } else if (jwksFile != null) {
            jwksFile = ConfigurationUtils.validateString(JWKS_FILE_CONFIG, jwksFile);
            return new JwksFileVerificationKeyResolver(Paths.get(jwksFile));
        } else if (jwksEndpointUri != null) {
            jwksEndpointUri = ConfigurationUtils.validateString(JWKS_ENDPOINT_URI_CONFIG, jwksEndpointUri);
            long refreshIntervalMs = conf.getJwksEndpointRefreshIntervalMs();
            RefreshingHttpsJwks httpsJkws = new RefreshingHttpsJwks(jwksEndpointUri, refreshIntervalMs);
            SSLSocketFactory sslSocketFactory = ConfigurationUtils.createSSLSocketFactory(conf.originals(), JWKS_ENDPOINT_URI_CONFIG);

            if (sslSocketFactory != null) {
                Get get = new Get();
                get.setSslSocketFactory(sslSocketFactory);
                httpsJkws.setSimpleHttpGet(get);
            }

            return new RefreshingHttpsJwksVerificationKeyResolver(httpsJkws);
        } else {
            pemDirectory = ConfigurationUtils.validateString(PEM_DIRECTORY_CONFIG, pemDirectory);
            return new PemDirectoryVerificationKeyResolver(Paths.get(pemDirectory));
        }
    }

}