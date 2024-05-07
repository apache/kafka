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
package org.apache.kafka.common.security.oauthbearer.internals;

import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.utils.Utils;

import javax.security.sasl.SaslException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OAuthBearerClientInitialResponse {
    static final String SEPARATOR = "\u0001";

    private static final String SASLNAME = "(?:[\\x01-\\x7F&&[^=,]]|=2C|=3D)+";
    private static final String KEY = "[A-Za-z]+";
    private static final String VALUE = "[\\x21-\\x7E \t\r\n]+";

    private static final String KVPAIRS = String.format("(%s=%s%s)*", KEY, VALUE, SEPARATOR);
    private static final Pattern AUTH_PATTERN = Pattern.compile("(?<scheme>[\\w]+)[ ]+(?<token>[-_~+/\\.a-zA-Z0-9]+([=]*))");
    private static final Pattern CLIENT_INITIAL_RESPONSE_PATTERN = Pattern.compile(
            String.format("n,(a=(?<authzid>%s))?,%s(?<kvpairs>%s)%s", SASLNAME, SEPARATOR, KVPAIRS, SEPARATOR));
    public static final String AUTH_KEY = "auth";

    private final String tokenValue;
    private final String authorizationId;
    private final SaslExtensions saslExtensions;

    public static final Pattern EXTENSION_KEY_PATTERN = Pattern.compile(KEY);
    public static final Pattern EXTENSION_VALUE_PATTERN = Pattern.compile(VALUE);

    public OAuthBearerClientInitialResponse(byte[] response) throws SaslException {
        String responseMsg = new String(response, StandardCharsets.UTF_8);
        Matcher matcher = CLIENT_INITIAL_RESPONSE_PATTERN.matcher(responseMsg);
        if (!matcher.matches())
            throw new SaslException("Invalid OAUTHBEARER client first message");
        String authzid = matcher.group("authzid");
        this.authorizationId = authzid == null ? "" : authzid;
        String kvPairs = matcher.group("kvpairs");
        Map<String, String> properties = Utils.parseMap(kvPairs, "=", SEPARATOR);
        String auth = properties.get(AUTH_KEY);
        if (auth == null)
            throw new SaslException("Invalid OAUTHBEARER client first message: 'auth' not specified");
        properties.remove(AUTH_KEY);
        SaslExtensions extensions = new SaslExtensions(properties);
        validateExtensions(extensions);
        this.saslExtensions = extensions;

        Matcher authMatcher = AUTH_PATTERN.matcher(auth);
        if (!authMatcher.matches())
            throw new SaslException("Invalid OAUTHBEARER client first message: invalid 'auth' format");
        if (!"bearer".equalsIgnoreCase(authMatcher.group("scheme"))) {
            String msg = String.format("Invalid scheme in OAUTHBEARER client first message: %s",
                    matcher.group("scheme"));
            throw new SaslException(msg);
        }
        this.tokenValue = authMatcher.group("token");
    }

    /**
     * Constructor
     * 
     * @param tokenValue
     *            the mandatory token value
     * @param extensions
     *            the optional extensions
     * @throws SaslException
     *             if any extension name or value fails to conform to the required
     *             regular expression as defined by the specification, or if the
     *             reserved {@code auth} appears as a key
     */
    public OAuthBearerClientInitialResponse(String tokenValue, SaslExtensions extensions) throws SaslException {
        this(tokenValue, "", extensions);
    }

    /**
     * Constructor
     * 
     * @param tokenValue
     *            the mandatory token value
     * @param authorizationId
     *            the optional authorization ID
     * @param extensions
     *            the optional extensions
     * @throws SaslException
     *             if any extension name or value fails to conform to the required
     *             regular expression as defined by the specification, or if the
     *             reserved {@code auth} appears as a key
     */
    public OAuthBearerClientInitialResponse(String tokenValue, String authorizationId, SaslExtensions extensions) throws SaslException {
        this.tokenValue = Objects.requireNonNull(tokenValue, "token value must not be null");
        this.authorizationId = authorizationId == null ? "" : authorizationId;
        validateExtensions(extensions);
        this.saslExtensions = extensions != null ? extensions : SaslExtensions.empty();
    }

    /**
     * Return the always non-null extensions
     * 
     * @return the always non-null extensions
     */
    public SaslExtensions extensions() {
        return saslExtensions;
    }

    public byte[] toBytes() {
        String authzid = authorizationId.isEmpty() ? "" : "a=" + authorizationId;
        String extensions = extensionsMessage();
        if (!extensions.isEmpty())
            extensions = SEPARATOR + extensions;

        String message = String.format("n,%s,%sauth=Bearer %s%s%s%s", authzid,
                SEPARATOR, tokenValue, extensions, SEPARATOR, SEPARATOR);

        return message.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Return the always non-null token value
     * 
     * @return the always non-null toklen value
     */
    public String tokenValue() {
        return tokenValue;
    }

    /**
     * Return the always non-null authorization ID
     * 
     * @return the always non-null authorization ID
     */
    public String authorizationId() {
        return authorizationId;
    }

    /**
     * Validates that the given extensions conform to the standard. They should also not contain the reserve key name {@link OAuthBearerClientInitialResponse#AUTH_KEY}
     *
     * @param extensions
     *            optional extensions to validate
     * @throws SaslException
     *             if any extension name or value fails to conform to the required
     *             regular expression as defined by the specification, or if the
     *             reserved {@code auth} appears as a key
     *
     * @see <a href="https://tools.ietf.org/html/rfc7628#section-3.1">RFC 7628,
     *  Section 3.1</a>
     */
    public static void validateExtensions(SaslExtensions extensions) throws SaslException {
        if (extensions == null)
            return;
        if (extensions.map().containsKey(OAuthBearerClientInitialResponse.AUTH_KEY))
            throw new SaslException("Extension name " + OAuthBearerClientInitialResponse.AUTH_KEY + " is invalid");

        for (Map.Entry<String, String> entry : extensions.map().entrySet()) {
            String extensionName = entry.getKey();
            String extensionValue = entry.getValue();

            if (!EXTENSION_KEY_PATTERN.matcher(extensionName).matches())
                throw new SaslException("Extension name " + extensionName + " is invalid");
            if (!EXTENSION_VALUE_PATTERN.matcher(extensionValue).matches())
                throw new SaslException("Extension value (" + extensionValue + ") for extension " + extensionName + " is invalid");
        }
    }

    /**
     * Converts the SASLExtensions to an OAuth protocol-friendly string
     */
    private String extensionsMessage() {
        return Utils.mkString(saslExtensions.map(), "", "", "=", SEPARATOR);
    }
}
