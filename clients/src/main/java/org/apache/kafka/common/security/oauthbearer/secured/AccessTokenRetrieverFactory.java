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

import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.ACCESS_TOKEN_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.ACCESS_TOKEN_FILE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.TOKEN_ENDPOINT_URI_CONFIG;

import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;
import javax.net.ssl.SSLSocketFactory;
import org.apache.kafka.common.config.ConfigException;

public class AccessTokenRetrieverFactory  {

    /**
     * Create an {@link AccessTokenRetriever} from the given
     * {@link LoginCallbackHandlerConfiguration}.
     *
     * <b>Note</b>: the returned <code>AccessTokenRetriever</code> is not initialized here and
     * must be done by the caller.
     *
     * Primarily exposed here for unit testing.
     *
     * @param conf Configuration for {@link javax.security.auth.callback.CallbackHandler}
     *
     * @return Non-<code>null</code> <code>AccessTokenRetriever</code>
     */

    public static AccessTokenRetriever create(LoginCallbackHandlerConfiguration conf) {
        String accessToken = conf.getAccessToken();
        String accessTokenFile = conf.getAccessTokenFile();
        String clientId = conf.getClientId();

        long count = Stream.of(accessToken, accessTokenFile, clientId)
            .filter(Objects::nonNull)
            .count();

        if (count != 1) {
            throw new ConfigException(String.format("The OAuth login configuration must include only one of %s, %s, or %s options", ACCESS_TOKEN_CONFIG, ACCESS_TOKEN_FILE_CONFIG, CLIENT_ID_CONFIG));
        } else if (accessToken != null) {
            accessToken = ConfigurationUtils.validateString(ACCESS_TOKEN_CONFIG, accessToken);
            return new StaticAccessTokenRetriever(accessToken);
        } else if (accessTokenFile != null) {
            accessTokenFile = ConfigurationUtils.validateString(ACCESS_TOKEN_FILE_CONFIG, accessTokenFile);
            return new FileTokenRetriever(Paths.get(accessTokenFile));
        } else {
            clientId = ConfigurationUtils.validateString(CLIENT_ID_CONFIG, clientId);
            String clientSecret = ConfigurationUtils.validateString(CLIENT_SECRET_CONFIG, conf.getClientSecret());
            String tokenEndpointUri = ConfigurationUtils.validateString(TOKEN_ENDPOINT_URI_CONFIG, conf.getTokenEndpointUri());

            SSLSocketFactory sslSocketFactory = ConfigurationUtils.createSSLSocketFactory(conf.originals(), TOKEN_ENDPOINT_URI_CONFIG);

            return new HttpAccessTokenRetriever(clientId,
                clientSecret,
                conf.getScope(),
                sslSocketFactory,
                tokenEndpointUri,
                conf.getLoginAttempts(),
                conf.getLoginRetryWaitMs(),
                conf.getLoginRetryMaxWaitMs(),
                conf.getLoginConnectTimeoutMs(),
                conf.getLoginReadTimeoutMs());
        }
    }

}