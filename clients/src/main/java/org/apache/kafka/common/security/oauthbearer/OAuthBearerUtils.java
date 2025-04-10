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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SslResource;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_DEFAULT;

public class OAuthBearerUtils {

    public static boolean protocolMatches(URL url, String protocol) {
        return protocol.equalsIgnoreCase(url.getProtocol());
    }

    public static Optional<SslResource> maybeCreateSslResource(URL url, OAuthBearerJaasConfig jaasConfig) {
        if (protocolMatches(url, "https")) {
            Map<String, ?> sslClientConfig = getSslClientConfig(jaasConfig);
            return Optional.of(SslResource.create(sslClientConfig));
        } else {
            return Optional.empty();
        }
    }

    public static Map<String, ?> getSslClientConfig(OAuthBearerJaasConfig jaasConfig) {
        ConfigDef sslConfigDef = new ConfigDef();
        sslConfigDef.withClientSslSupport();
        AbstractConfig sslClientConfig = new AbstractConfig(sslConfigDef, jaasConfig.options());
        return sslClientConfig.values();
    }

    // visible for testing
    // make sure the url is in the "org.apache.kafka.sasl.oauthbearer.allowed.urls" system property
    public static void throwIfURLIsNotAllowed(String value) {
        Set<String> allowedUrls = Arrays.stream(
                System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, ALLOWED_SASL_OAUTHBEARER_URLS_DEFAULT).split(","))
            .map(String::trim)
            .collect(Collectors.toSet());
        if (!allowedUrls.contains(value)) {
            throw new ConfigException(value + " is not allowed. Update system property '"
                + ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG + "' to allow " + value);
        }
    }

    public static <T> T getConfiguredInstanceOrDefault(Map<String, ?> configs,
                                                       String saslMechanism,
                                                       List<AppConfigurationEntry> jaasConfigEntries,
                                                       String configName,
                                                       Class<T> clazz) {
        Object classOrClassName = configs.get(configName);
        Object o;

        if (classOrClassName instanceof String) {
            try {
                o = Utils.newInstance((String) classOrClassName, clazz);
            } catch (ClassNotFoundException e) {
                throw new KafkaException("Class " + classOrClassName + " cannot be found", e);
            }
        } else if (classOrClassName instanceof Class<?>) {
            o = Utils.newInstance((Class<?>) classOrClassName);
        } else {
            throw new KafkaException("Unexpected element of type " + classOrClassName.getClass().getName() + ", expected String or Class");
        }

        if (!clazz.isInstance(o))
            throw new KafkaException(classOrClassName + " is not an instance of " + clazz.getName());

        try {
            if (o instanceof OAuthBearerConfigurable)
                ((OAuthBearerConfigurable) o).configure(configs, saslMechanism, jaasConfigEntries);
        } catch (Exception e) {
            Utils.closeQuietly((AutoCloseable) o, "AutoCloseable object constructed and configured during failed call to configure()");
            throw e;
        }

        return clazz.cast(o);
    }

    /**
     * In some cases, the incoming {@link Map} doesn't contain a value for
     * {@link SaslConfigs#SASL_OAUTHBEARER_HEADER_URLENCODE}. Returning {@code null} from {@link Map#get(Object)}
     * will cause a {@link NullPointerException} when it is later unboxed.
     *
     * <p/>
     *
     * This utility method ensures that we have a non-{@code null} value to use in the
     * {@link ClientCredentialsJwtRetriever} constructor.
     */
    public static boolean urlencodeHeader(OAuthBearerConfig oauthConfig) {
        if (oauthConfig.containsKey(SASL_OAUTHBEARER_HEADER_URLENCODE))
            return oauthConfig.getBoolean(SASL_OAUTHBEARER_HEADER_URLENCODE);
        else
            return DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
    }

    /**
     * Validates that the scopes are valid, where <i>invalid</i> means <i>any</i> of
     * the following:
     *
     * <ul>
     *     <li>Collection is <code>null</code></li>
     *     <li>Collection has duplicates</li>
     *     <li>Any of the elements in the collection are <code>null</code></li>
     *     <li>Any of the elements in the collection are zero length</li>
     *     <li>Any of the elements in the collection are whitespace only</li>
     * </ul>
     *
     * @param scopeClaimName Name of the claim used for the scope values
     * @param scopes         Collection of String scopes
     *
     * @return Unmodifiable {@link Set} that includes the values of the original set, but with
     *         each value trimmed
     *
     * @throws JwtValidatorException Thrown if the value is <code>null</code>, contains duplicates, or
     *                           if any of the values in the set are <code>null</code>, empty,
     *                           or whitespace only
     */

    public static Set<String> validateClaimScopes(String scopeClaimName, Collection<String> scopes) throws JwtValidatorException {
        if (scopes == null)
            throw new JwtValidatorException(String.format("%s value must be non-null", scopeClaimName));

        Set<String> copy = new HashSet<>();

        for (String scope : scopes) {
            scope = validateClaimValue(scopeClaimName, scope);

            if (copy.contains(scope))
                throw new JwtValidatorException(String.format("%s value must not contain duplicates - %s already present", scopeClaimName, scope));

            copy.add(scope);
        }

        return Collections.unmodifiableSet(copy);
    }

    /**
     * Validates that the given lifetime is valid, where <i>invalid</i> means <i>any</i> of
     * the following:
     *
     * <ul>
     *     <li><code>null</code></li>
     *     <li>Negative</li>
     * </ul>
     *
     * @param claimName  Name of the claim
     * @param claimValue Expiration time (in milliseconds)
     *
     * @return Input parameter, as provided
     *
     * @throws JwtValidatorException Thrown if the value is <code>null</code> or negative
     */
    public static long validateClaimExpiration(String claimName, Long claimValue) throws JwtValidatorException {
        if (claimValue == null)
            throw new JwtValidatorException(String.format("%s value must be non-null", claimName));

        if (claimValue < 0)
            throw new JwtValidatorException(String.format("%s value must be non-negative; value given was \"%s\"", claimName, claimValue));

        return claimValue;
    }

    /**
     * Validates that the given claim value is valid, where <i>invalid</i> means <i>any</i> of
     * the following:
     *
     * <ul>
     *     <li><code>null</code></li>
     *     <li>Zero length</li>
     *     <li>Whitespace only</li>
     * </ul>
     *
     * @param claimName  Name of the claim
     * @param claimValue Name of the subject
     *
     * @return Trimmed version of the <code>claimValue</code> parameter
     *
     * @throws JwtValidatorException Thrown if the value is <code>null</code>, empty, or whitespace only
     */

    public static String validateClaimSubject(String claimName, String claimValue) throws JwtValidatorException {
        return validateClaimValue(claimName, claimValue);
    }

    /**
     * Validates that the given issued at claim name is valid, where <i>invalid</i> means <i>any</i> of
     * the following:
     *
     * <ul>
     *     <li>Negative</li>
     * </ul>
     *
     * @param claimName  Name of the claim
     * @param claimValue Start time (in milliseconds) or <code>null</code> if not used
     *
     * @return Input parameter, as provided
     *
     * @throws JwtValidatorException Thrown if the value is negative
     */

    public static Long validateClaimIssuedAt(String claimName, Long claimValue) throws JwtValidatorException {
        if (claimValue != null && claimValue < 0)
            throw new JwtValidatorException(String.format("%s value must be null or non-negative; value given was \"%s\"", claimName, claimValue));

        return claimValue;
    }

    /**
     * Validates that the given claim name override is valid, where <i>invalid</i> means
     * <i>any</i> of the following:
     *
     * <ul>
     *     <li><code>null</code></li>
     *     <li>Zero length</li>
     *     <li>Whitespace only</li>
     * </ul>
     *
     * @param name  "Standard" name of the claim, e.g. <code>sub</code>
     * @param value "Override" name of the claim, e.g. <code>email</code>
     *
     * @return Trimmed version of the <code>value</code> parameter
     *
     * @throws JwtValidatorException Thrown if the value is <code>null</code>, empty, or whitespace only
     */

    public static String validateClaimNameOverride(String name, String value) throws JwtValidatorException {
        return validateClaimValue(name, value);
    }

    public static String validateClaimValue(String name, String value) throws JwtValidatorException {
        if (Utils.isBlank(value))
            throw new JwtValidatorException(String.format("The value of the OAuth claim %s must be non-null, non-empty, and non-whitespace", name));

        return value.trim();
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
    public static File validateFile(OAuthBearerAbstractConfig config, String key) {
        String fileName = config.getString(key);
        File file = new File(fileName);
        return validateFile(key, file);
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
    public static File validateFileUrl(OAuthBearerAbstractConfig config, String key) {
        URL url = validateUrl(config, key);
        File file;

        try {
            file = new File(url.toURI().getRawPath()).getAbsoluteFile();
        } catch (URISyntaxException e) {
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URL (%s) that is malformed: %s", key, url, e.getMessage()));
        }

        return validateFile(key, file);
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
    public static File validateFile(String key, File file) {
        if (!file.exists())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't exist", key, file));

        if (!file.canRead())
            throw new ConfigException(String.format("The OAuth configuration option %s contains a file (%s) that doesn't have read permission", key, file));

        if (file.isDirectory())
            throw new ConfigException(String.format("The OAuth configuration option %s references a directory (%s), not a file", key, file));

        return file;
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
    public static URL validateUrl(OAuthBearerAbstractConfig config, String key) {
        String value = config.getString(key);
        URL url;

        try {
            url = new URL(value);
        } catch (MalformedURLException e) {
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URL (%s) that is malformed: %s", key, value, e.getMessage()));
        }

        String protocol = url.getProtocol();

        if (!protocolMatches(url, "https") && !protocolMatches(url, "http") && !protocolMatches(url, "file"))
            throw new ConfigException(String.format("The OAuth configuration option %s contains a URL (%s) that contains an invalid protocol (%s); only \"http\", \"https\", and \"file\" protocol are supported", key, value, protocol));

        throwIfURLIsNotAllowed(value);

        return url;
    }
}
