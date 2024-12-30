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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

/**
 * <code>ConfigurationUtils</code> is a utility class to perform basic configuration-related
 * logic and is separated out here for easier, more direct testing.
 */

public class ConfigurationUtils {

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
        URL url = validateUrl(name);
        File file;

        try {
            file = new File(url.toURI().getRawPath()).getAbsoluteFile();
        } catch (URISyntaxException e) {
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URL (%s) that is malformed: %s", name, url, e.getMessage()));
        }

        if (!file.exists())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't exist", name, file));

        if (!file.canRead())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't have read permission", name, file));

        if (file.isDirectory())
            throw new ConfigException(String.format("The OAuth configuration option %s references a directory (%s), not a file", name, file));

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

    public Integer validateInteger(String name, boolean isRequired) {
        Integer value = get(name);

        if (value == null) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth configuration option %s must be non-null", name));
            else
                return null;
        }

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
                throw new ConfigException(String.format("The OAuth configuration option %s must be non-null", name));
            else
                return null;
        }

        if (min != null && value < min)
            throw new ConfigException(String.format("The OAuth configuration option %s value must be at least %s", name, min));

        return value;
    }

    /**
     * Validates that the configured URL that:
     *
     * <li>
     *     <ul>is well-formed</ul>
     *     <ul>contains a scheme</ul>
     *     <ul>uses either HTTP, HTTPS, or file protocols</ul>
     * </li>
     *
     * No effort is made to connect to the URL in the validation step.
     */

    public URL validateUrl(String name) {
        String value = validateString(name);
        URL url;

        try {
            url = new URL(value);
        } catch (MalformedURLException e) {
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URL (%s) that is malformed: %s", name, value, e.getMessage()));
        }

        String protocol = url.getProtocol();

        if (protocol == null || protocol.trim().isEmpty())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URL (%s) that is missing the protocol", name, value));

        protocol = protocol.toLowerCase(Locale.ROOT);

        if (!(protocol.equals("http") || protocol.equals("https") || protocol.equals("file")))
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URL (%s) that contains an invalid protocol (%s); only \"http\", \"https\", and \"file\" protocol are supported", name, value, protocol));

        return url;
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

    public Boolean validateBoolean(String name, boolean isRequired) {
        Boolean value = get(name);

        if (value == null && isRequired)
            throw new ConfigException(String.format("The OAuth configuration option %s must be non-null", name));

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
