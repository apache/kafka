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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import javax.net.ssl.SSLSocketFactory;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Mode;
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

    private final Map<String, ?> configs;

    private final String prefix;

    public ConfigurationUtils(Map<String, ?> configs) {
        this(configs, null);
    }

    public ConfigurationUtils(Map<String, ?> configs, String saslMechanism) {
        this.configs = configs;

        if (saslMechanism != null && !saslMechanism.trim().isEmpty())
            this.prefix = ListenerName.saslMechanismPrefix(saslMechanism.trim());
        else
            this.prefix = null;
    }

    public Map<String, ?> getSslClientConfig(String uriConfigName) {
        String urlConfigValue = get(uriConfigName);

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
        AbstractConfig sslClientConfig = new AbstractConfig(sslConfigDef, configs);
        return sslClientConfig.values();
    }

    public SSLSocketFactory createSSLSocketFactory(String uriConfigName) {
        Map<String, ?> sslClientConfig = getSslClientConfig(uriConfigName);

        if (sslClientConfig == null) {
            log.warn("Requesting SSL client socket factory but SSL configs were null");
            return null;
        }

        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(sslClientConfig);
        SSLSocketFactory socketFactory = ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext().getSocketFactory();
        log.debug("Created SSLSocketFactory: {}", sslClientConfig);
        return socketFactory;
    }

    /**
     * Validates that, if a value is supplied, is a file that:
     *
     * <li>
     *     <ul>exists</ul>
     *     <ul>has read permission</ul>
     *     <ul>points to a file</ul>
     * </li>
     *
     * If the value is null or an empty string, it is assumed to be an "empty" value and thus.
     * ignored. Any whitespace is trimmed off of the beginning and end.
     */

    public Path validateFile(String name) {
        URI uri = validateUri(name);
        File file = new File(uri.getRawPath()).getAbsoluteFile();

        if (!file.exists())
            throw new ConfigException(name, file, String.format("The OAuth configuration option %s contains a file (%s) that doesn't exist", name, file));

        if (!file.canRead())
            throw new ConfigException(name, file, String.format("The OAuth configuration option %s contains a file (%s) that doesn't have read permission", name, file));

        if (file.isDirectory())
            throw new ConfigException(name, file, String.format("The OAuth configuration option %s references a directory (%s), not a file", name, file));

        return file.toPath();
    }

    /**
     * Validates that, if a value is supplied, is a value that:
     *
     * <li>
     *     <ul>is an Integer</ul>
     *     <ul>has a value that is not less than the provided minimum value</ul>
     * </li>
     *
     * If the value is null or an empty string, it is assumed to be an "empty" value and thus
     * ignored. Any whitespace is trimmed off of the beginning and end.
     */

    public Integer validateInteger(String name) {
        return validateInteger(name, true);
    }

    public Integer validateInteger(String name, boolean isRequired) {
        return validateInteger(name, isRequired, null);
    }

    public Integer validateInteger(String name, boolean isRequired, Integer min) {
        Integer value = get(name);

        if (value == null) {
            if (isRequired)
                throw new ConfigException(name, null, String.format("The OAuth configuration option %s must be non-null", name));
            else
                return null;
        }

        if (min != null && value < min)
            throw new ConfigException(name, value, String.format("The OAuth configuration option %s value must be at least %s", name, min));

        return value;
    }

    /**
     * Validates that, if a value is supplied, is a value that:
     *
     * <li>
     *     <ul>is an Integer</ul>
     *     <ul>has a value that is not less than the provided minimum value</ul>
     * </li>
     *
     * If the value is null or an empty string, it is assumed to be an "empty" value and thus
     * ignored. Any whitespace is trimmed off of the beginning and end.
     */

    public Long validateLong(String name) {
        return validateLong(name, true);
    }

    public Long validateLong(String name, boolean isRequired) {
        return validateLong(name, isRequired, null);
    }

    public Long validateLong(String name, boolean isRequired, Long min) {
        Long value = get(name);

        if (value == null) {
            if (isRequired)
                throw new ConfigException(name, null, String.format("The OAuth configuration option %s must be non-null", name));
            else
                return null;
        }

        if (min != null && value < min)
            throw new ConfigException(name, value, String.format("The OAuth configuration option %s value must be at least %s", name, min));

        return value;
    }

    /**
     * Validates that the configured URI that:
     *
     * <li>
     *     <ul>is well-formed</ul>
     *     <ul>contains a scheme</ul>
     *     <ul>uses either HTTP, HTTPS, or file protocols</ul>
     * </li>
     *
     * No effort is made to contact the URL in the validation step.
     */

    public URI validateUri(String name) {
        String value = validateString(name);
        URI uri;

        try {
            uri = new URI(value.trim());
        } catch (URISyntaxException e) {
            throw new ConfigException(name, value, String.format("The OAuth configuration option %s contains a URI (%s) that is malformed: %s", name, value, e.getMessage()));
        }

        String scheme = uri.getScheme();

        if (scheme == null || scheme.trim().isEmpty())
            throw new ConfigException(name, value, String.format("The OAuth configuration option %s contains a URI (%s) that is missing the scheme", name, value));

        scheme = scheme.toLowerCase(Locale.ROOT);

        if (!(scheme.equals("http") || scheme.equals("https") || scheme.equals("file")))
            throw new ConfigException(name, value, String.format("The OAuth configuration option %s contains a URI (%s) that contains an invalid scheme (%s); only \"http\", \"https\", and \"file\" schemes are supported", name, value, scheme));

        return uri;
    }

    public String validateString(String name) throws ValidateException {
        return validateString(name, true);
    }

    public String validateString(String name, boolean isRequired) throws ValidateException {
        String value = get(name);

        if (value == null) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth configuration option %s value must be non-null", name));
            else
                return null;
        }

        value = value.trim();

        if (value.isEmpty()) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth configuration option %s value must not contain only whitespace", name));
            else
                return null;
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String name) {
        T value = (T) configs.get(prefix + name);

        if (value != null)
            return value;

        return (T) configs.get(name);
    }

}
