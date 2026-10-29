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
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_FILES_DEFAULT;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_DEFAULT;

/**
 * <code>ConfigurationUtils</code> is a utility class to perform basic configuration-related
 * logic and is separated out here for easier, more direct testing.
 */

public class ConfigurationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

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

    public boolean containsKey(String name) {
        return get(name) != null;
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

    public File validateFileUrl(String name) {
        URL url = validateUrl(name);
        File file;

        try {
            file = new File(url.toURI().getRawPath()).getAbsoluteFile();
        } catch (URISyntaxException e) {
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URL (%s) that is malformed: %s", name, url, e.getMessage()));
        }

        return validateFile(name, file);
    }

    /**
     * Validates that the file:
     *
     * <li>
     *     <ul>exists</ul>
     *     <ul>has read permission</ul>
     *     <ul>points to a file</ul>
     * </li>
     */
    public File validateFile(String name) {
        String s = validateString(name);
        File file = validateFile(name, new File(s).getAbsoluteFile());
        throwIfFileIsNotAllowed(name, file.getAbsolutePath());
        return file;
    }

    /**
     * Validates that the file:
     *
     * <li>
     *     <ul>exists</ul>
     *     <ul>has read permission</ul>
     *     <ul>points to a file</ul>
     * </li>
     */
    private File validateFile(String name, File file) {
        if (!file.exists())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't exist", name, file));

        if (!file.canRead())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't have read permission", name, file));

        if (file.isDirectory())
            throw new ConfigException(String.format("The OAuth configuration option %s references a directory (%s), not a file", name, file));

        return file;
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
                throw new ConfigException(String.format("The OAuth configuration option %s is required", name));
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
                throw new ConfigException(String.format("The OAuth configuration option %s is required", name));
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
     * <ul>
     *     <li>is well-formed</li>
     *     <li>contains a scheme</li>
     *     <li>uses either HTTP, HTTPS, or file protocols</li>
     *     <li>is in the allow-list</li>
     * </ul>
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

        throwIfURLIsNotAllowed(name, value);

        return url;
    }

    public String validatePassword(String name) {
        Password value = get(name);

        if (value == null || Utils.isBlank(value.value()))
            throw new ConfigException(String.format("The OAuth configuration option %s value is required", name));

        return value.value().trim();
    }

    public String validateString(String name) {
        return validateString(name, true);
    }

    public String validateString(String name, boolean isRequired) {
        String value = get(name);

        if (Utils.isBlank(value)) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth configuration option %s value is required", name));
            else
                return null;
        }

        return value.trim();
    }

    public Boolean validateBoolean(String name, boolean isRequired) {
        Boolean value = get(name);

        if (value == null && isRequired)
            throw new ConfigException(String.format("The OAuth configuration option %s is required", name));

        return value;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String name) {
        T value = (T) configs.get(prefix + name);

        if (value != null)
            return value;

        return (T) configs.get(name);
    }

    public static <T> T getConfiguredInstance(Map<String, ?> configs,
                                              String saslMechanism,
                                              List<AppConfigurationEntry> jaasConfigEntries,
                                              String configName,
                                              Class<T> expectedClass) {
        Object configValue = configs.get(configName);
        Object o;

        if (configValue instanceof String) {
            String implementationClassName = (String) configValue;

            try {
                o = Utils.newInstance(implementationClassName, expectedClass);
            } catch (Exception e) {
                throw new ConfigException(
                    String.format(
                        "The class %s defined in the %s configuration could not be instantiated: %s",
                        implementationClassName,
                        configName,
                        e.getMessage()
                    )
                );
            }
        } else if (configValue instanceof Class<?>) {
            Class<?> implementationClass = (Class<?>) configValue;

            try {
                o = Utils.newInstance(implementationClass);
            } catch (Exception e) {
                throw new ConfigException(
                    String.format(
                        "The class %s defined in the %s configuration could not be instantiated: %s",
                        implementationClass.getName(),
                        configName,
                        e.getMessage()
                    )
                );
            }
        } else if (configValue != null) {
            throw new ConfigException(
                String.format(
                    "The type for the %s configuration must be either %s or %s, but was %s",
                    configName,
                    String.class.getName(),
                    Class.class.getName(),
                    configValue.getClass().getName()
                )
            );
        } else {
            throw new ConfigException(String.format("The required configuration %s was null", configName));
        }

        if (!expectedClass.isInstance(o)) {
            throw new ConfigException(
                String.format(
                    "The configured class (%s) for the %s configuration is not an instance of %s, as is required",
                    o.getClass().getName(),
                    configName,
                    expectedClass.getName()
                )
            );
        }

        if (o instanceof OAuthBearerConfigurable) {
            try {
                ((OAuthBearerConfigurable) o).configure(configs, saslMechanism, jaasConfigEntries);
            } catch (Exception e) {
                Utils.maybeCloseQuietly(o, "Instance of class " + o.getClass().getName() + " failed call to configure()");
                LOG.warn(
                    "The class {} defined in the {} configuration encountered an error on configure(): {}",
                    o.getClass().getName(),
                    configName,
                    e.getMessage(),
                    e
                );
                throw new ConfigException(
                    String.format(
                        "The class %s defined in the %s configuration encountered an error on configure(): %s",
                        o.getClass().getName(),
                        configName,
                        e.getMessage()
                    )
                );
            }
        }

        return expectedClass.cast(o);
    }

    // visible for testing
    // make sure the url is in the "org.apache.kafka.sasl.oauthbearer.allowed.urls" system property
    void throwIfURLIsNotAllowed(String configName, String configValue) {
        throwIfResourceIsNotAllowed(
            "URL",
            configName,
            configValue,
            ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG,
            ALLOWED_SASL_OAUTHBEARER_URLS_DEFAULT
        );
    }

    // visible for testing
    // make sure the file is in the "org.apache.kafka.sasl.oauthbearer.allowed.files" system property
    void throwIfFileIsNotAllowed(String configName, String configValue) {
        throwIfResourceIsNotAllowed(
            "file",
            configName,
            configValue,
            ALLOWED_SASL_OAUTHBEARER_FILES_CONFIG,
            ALLOWED_SASL_OAUTHBEARER_FILES_DEFAULT
        );
    }

    private void throwIfResourceIsNotAllowed(String resourceType,
                                             String configName,
                                             String configValue,
                                             String propertyName,
                                             String propertyDefault) {
        String[] allowedArray = System.getProperty(propertyName, propertyDefault).split(",");
        Set<String> allowed = Arrays.stream(allowedArray)
            .map(String::trim)
            .collect(Collectors.toSet());

        if (!allowed.contains(configValue)) {
            String message = String.format(
                "The %s cannot be accessed due to restrictions. Update the system property '%s' to allow the %s to be accessed.",
                resourceType,
                propertyName,
                resourceType
            );
            throw new ConfigException(configName, configValue, message);
        }
    }
}
