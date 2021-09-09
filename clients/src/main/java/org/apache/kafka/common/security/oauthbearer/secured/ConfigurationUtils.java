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

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>ConfigurationUtils</code> is a utility class to perform basic configuration-related
 * logic and is separated out here for easier, more direct testing.
 */

public class ConfigurationUtils {

    private static final Logger log = LoggerFactory.getLogger(ConfigurationUtils.class);

    public static Map<String, Object> getModuleOptions(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException(String.format("Unexpected SASL mechanism: %s", saslMechanism));

        if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null)
            throw new IllegalArgumentException(String.format("Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)", jaasConfigEntries.size()));

        return Collections.unmodifiableMap(jaasConfigEntries.get(0).getOptions());
    }

    public static Map<String, ?> getSslClientConfig(Map<String, ?> config, String uriConfigName) {
        String urlConfigValue = (String) config.get(uriConfigName);

        if (urlConfigValue == null || urlConfigValue.trim().isEmpty())
            throw new ConfigException(String.format("The OAuth configuration option %s is required", uriConfigName));

        URL url;

        try {
            url = new URL(urlConfigValue);
        } catch (IOException e) {
            throw new ConfigException(String.format("The OAuth configuration option %s was not a valid URL (%s)", uriConfigName, urlConfigValue));
        }

        if (!url.getProtocol().equalsIgnoreCase("https")) {
            log.warn("Not creating SSL socket factory as URL for {} ({}) is not SSL-/TLS-based", uriConfigName, url);
            return null;
        }

        ConfigDef sslConfigDef = new ConfigDef();
        sslConfigDef.withClientSslSupport();
        AbstractConfig sslClientConfig = new AbstractConfig(sslConfigDef, config);
        return sslClientConfig.values();
    }

    public static SSLSocketFactory createSSLSocketFactory(Map<String, ?> config, String uriConfigName) {
        Map<String, ?> sslClientConfig = getSslClientConfig(config, uriConfigName);

        if (sslClientConfig == null) {
            log.warn("Requesting SSL client socket factory but SSL configs were null");
            return null;
        }

        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(sslClientConfig);
        SSLSocketFactory socketFactory = ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext().getSocketFactory();
        log.warn("socketFactory: {}", socketFactory);
        return socketFactory;
    }

    public static String validateString(String name, String value) throws ValidateException {
        if (value == null)
            throw new ValidateException(String.format("The OAuth configuration option %s value must be non-null", name));

        if (value.isEmpty())
            throw new ValidateException(String.format("The OAuth configuration option%s value must be non-empty", name));

        value = value.trim();

        if (value.isEmpty())
            throw new ValidateException(String.format("The OAuth configuration option %s value must not contain only whitespace", name));

        return value;
    }

}
