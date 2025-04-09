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
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_DEFAULT;

public class OAuthBearerUtils {

    public static AssertionJwtTemplate createAssertionTemplate(OAuthBearerConfig oauthConfig) {
//        Map<String, Object> payload = new HashMap<>();
//        Optional.ofNullable(oauthConfig.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD, false)).ifPresent(a -> payload.put("aud", a));
//        Optional.ofNullable(oauthConfig.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS, false)).ifPresent(i -> payload.put("iss", i));
//        Optional.ofNullable(oauthConfig.validateString(SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB, false)).ifPresent(s -> payload.put("sub", s));
//
//        if (!payload.isEmpty()) {
//            StaticAssertionJwtTemplate staticTemplate = new StaticAssertionJwtTemplate(payload);
//
//        }
//
        File assertionTemplateFile = oauthConfig.validateFile(SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE);
        return new FileAssertionJwtTemplate(assertionTemplateFile);
    }

    public static boolean schemeMatches(URI uri, String scheme) {
        return scheme.equalsIgnoreCase(uri.getScheme());
    }

    public static boolean protocolMatches(URL url, String protocol) {
        return protocol.equalsIgnoreCase(url.getProtocol());
    }

    public static Optional<SslResource> maybeCreateSslResource(URI uri, OAuthBearerJaasConfig jaasConfig) {
        if (schemeMatches(uri, "https")) {
            Map<String, ?> sslClientConfig = getSslClientConfig(jaasConfig);
            return Optional.of(SslResource.create(sslClientConfig));
        } else {
            return Optional.empty();
        }
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
    public static boolean validateUrlencodeHeader(OAuthBearerConfig oauthConfig) {
        Boolean urlencodeHeader = oauthConfig.validateBoolean(SASL_OAUTHBEARER_HEADER_URLENCODE, false);
        return Objects.requireNonNullElse(urlencodeHeader, DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE);
    }

    public static String configOrJaas(OAuthBearerConfig oauthConfig,
                                      OAuthBearerJaasConfig jaasConfig,
                                      String configName,
                                      String jaasName,
                                      boolean isRequired) {
        if (oauthConfig.get(configName) != null) {
            return oauthConfig.validateString(configName, isRequired);
        } else {
            return jaasConfig.validateString(jaasName, isRequired);
        }
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

    public static Set<String> validateScopes(String scopeClaimName, Collection<String> scopes) throws JwtValidatorException {
        if (scopes == null)
            throw new JwtValidatorException(String.format("%s value must be non-null", scopeClaimName));

        Set<String> copy = new HashSet<>();

        for (String scope : scopes) {
            scope = validateString(scopeClaimName, scope);

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

    public static long validateExpiration(String claimName, Long claimValue) throws JwtValidatorException {
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

    public static String validateSubject(String claimName, String claimValue) throws JwtValidatorException {
        return validateString(claimName, claimValue);
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

    public static Long validateIssuedAt(String claimName, Long claimValue) throws JwtValidatorException {
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
        return validateString(name, value);
    }

    private static String validateString(String name, String value) throws JwtValidatorException {
        if (Utils.isBlank(value))
            throw new JwtValidatorException(String.format("%s value must be non-null, non-empty, and non-whitespace", name));

        return value.trim();
    }
}
