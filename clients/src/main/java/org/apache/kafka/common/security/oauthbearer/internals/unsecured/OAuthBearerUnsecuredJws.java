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
package org.apache.kafka.common.security.oauthbearer.internals.unsecured;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.NumericNode;

/**
 * A simple unsecured JWS implementation. The '{@code nbf}' claim is ignored if
 * it is given because the related logic is not required for Kafka testing and
 * development purposes.
 * 
 * @see <a href="https://tools.ietf.org/html/rfc7515">RFC 7515</a>
 */
public class OAuthBearerUnsecuredJws implements OAuthBearerToken {
    private final String compactSerialization;
    private final List<String> splits;
    private final Map<String, Object> header;
    private final String principalClaimName;
    private final String scopeClaimName;
    private final Map<String, Object> claims;
    private final Set<String> scope;
    private final long lifetime;
    private final String principalName;
    private final Long startTimeMs;

    /**
     * Constructor with the given principal and scope claim names
     * 
     * @param compactSerialization
     *            the compact serialization to parse as an unsecured JWS
     * @param principalClaimName
     *            the required principal claim name
     * @param scopeClaimName
     *            the required scope claim name
     * @throws OAuthBearerIllegalTokenException
     *             if the compact serialization is not a valid unsecured JWS
     *             (meaning it did not have 3 dot-separated Base64URL sections
     *             without an empty digital signature; or the header or claims
     *             either are not valid Base 64 URL encoded values or are not JSON
     *             after decoding; or the mandatory '{@code alg}' header value is
     *             not "{@code none}")
     */
    public OAuthBearerUnsecuredJws(String compactSerialization, String principalClaimName, String scopeClaimName)
            throws OAuthBearerIllegalTokenException {
        this.compactSerialization = Objects.requireNonNull(compactSerialization);
        if (compactSerialization.contains(".."))
            throw new OAuthBearerIllegalTokenException(
                    OAuthBearerValidationResult.newFailure("Malformed compact serialization contains '..'"));
        this.splits = extractCompactSerializationSplits();
        this.header = toMap(splits().get(0));
        String claimsSplit = splits.get(1);
        this.claims = toMap(claimsSplit);
        String alg = Objects.requireNonNull(header().get("alg"), "JWS header must have an Algorithm value").toString();
        if (!"none".equals(alg))
            throw new OAuthBearerIllegalTokenException(
                    OAuthBearerValidationResult.newFailure("Unsecured JWS must have 'none' for an algorithm"));
        String digitalSignatureSplit = splits.get(2);
        if (!digitalSignatureSplit.isEmpty())
            throw new OAuthBearerIllegalTokenException(
                    OAuthBearerValidationResult.newFailure("Unsecured JWS must not contain a digital signature"));
        this.principalClaimName = Objects.requireNonNull(principalClaimName).trim();
        if (this.principalClaimName.isEmpty())
            throw new IllegalArgumentException("Must specify a non-blank principal claim name");
        this.scopeClaimName = Objects.requireNonNull(scopeClaimName).trim();
        if (this.scopeClaimName.isEmpty())
            throw new IllegalArgumentException("Must specify a non-blank scope claim name");
        this.scope = calculateScope();
        Number expirationTimeSeconds = expirationTime();
        if (expirationTimeSeconds == null)
            throw new OAuthBearerIllegalTokenException(
                    OAuthBearerValidationResult.newFailure("No expiration time in JWT"));
        lifetime = convertClaimTimeInSecondsToMs(expirationTimeSeconds);
        String principalName = claim(this.principalClaimName, String.class);
        if (principalName == null || principalName.trim().isEmpty())
            throw new OAuthBearerIllegalTokenException(OAuthBearerValidationResult
                    .newFailure("No principal name in JWT claim: " + this.principalClaimName));
        this.principalName = principalName;
        this.startTimeMs = calculateStartTimeMs();
    }

    @Override
    public String value() {
        return compactSerialization;
    }

    /**
     * Return the 3 or 5 dot-separated sections of the JWT compact serialization
     * 
     * @return the 3 or 5 dot-separated sections of the JWT compact serialization
     */
    public List<String> splits() {
        return splits;
    }

    /**
     * Return the JOSE Header as a {@code Map}
     * 
     * @return the JOSE header
     */
    public Map<String, Object> header() {
        return header;
    }

    @Override
    public String principalName() {
        return principalName;
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs;
    }

    @Override
    public long lifetimeMs() {
        return lifetime;
    }

    @Override
    public Set<String> scope() throws OAuthBearerIllegalTokenException {
        return scope;
    }

    /**
     * Return the JWT Claim Set as a {@code Map}
     * 
     * @return the (always non-null but possibly empty) claims
     */
    public Map<String, Object> claims() {
        return claims;
    }

    /**
     * Return the (always non-null/non-empty) principal claim name
     * 
     * @return the (always non-null/non-empty) principal claim name
     */
    public String principalClaimName() {
        return principalClaimName;
    }

    /**
     * Return the (always non-null/non-empty) scope claim name
     * 
     * @return the (always non-null/non-empty) scope claim name
     */
    public String scopeClaimName() {
        return scopeClaimName;
    }

    /**
     * Indicate if the claim exists and is the given type
     * 
     * @param claimName
     *            the mandatory JWT claim name
     * @param type
     *            the mandatory type, which should either be String.class,
     *            Number.class, or List.class
     * @return true if the claim exists and is the given type, otherwise false
     */
    public boolean isClaimType(String claimName, Class<?> type) {
        Object value = rawClaim(claimName);
        Objects.requireNonNull(type);
        if (value == null)
            return false;
        if (type == String.class && value instanceof String)
            return true;
        if (type == Number.class && value instanceof Number)
            return true;
        return type == List.class && value instanceof List;
    }

    /**
     * Extract a claim of the given type
     * 
     * @param claimName
     *            the mandatory JWT claim name
     * @param type
     *            the mandatory type, which must either be String.class,
     *            Number.class, or List.class
     * @return the claim if it exists, otherwise null
     * @throws OAuthBearerIllegalTokenException
     *             if the claim exists but is not the given type
     */
    public <T> T claim(String claimName, Class<T> type) throws OAuthBearerIllegalTokenException {
        Object value = rawClaim(claimName);
        try {
            return Objects.requireNonNull(type).cast(value);
        } catch (ClassCastException e) {
            throw new OAuthBearerIllegalTokenException(
                    OAuthBearerValidationResult.newFailure(String.format("The '%s' claim was not of type %s: %s",
                            claimName, type.getSimpleName(), value.getClass().getSimpleName())));
        }
    }

    /**
     * Extract a claim in its raw form
     * 
     * @param claimName
     *            the mandatory JWT claim name
     * @return the raw claim value, if it exists, otherwise null
     */
    public Object rawClaim(String claimName) {
        return claims().get(Objects.requireNonNull(claimName));
    }

    /**
     * Return the
     * <a href="https://tools.ietf.org/html/rfc7519#section-4.1.4">Expiration
     * Time</a> claim
     * 
     * @return the <a href=
     *         "https://tools.ietf.org/html/rfc7519#section-4.1.4">Expiration
     *         Time</a> claim if available, otherwise null
     * @throws OAuthBearerIllegalTokenException
     *             if the claim value is the incorrect type
     */
    public Number expirationTime() throws OAuthBearerIllegalTokenException {
        return claim("exp", Number.class);
    }

    /**
     * Return the <a href="https://tools.ietf.org/html/rfc7519#section-4.1.6">Issued
     * At</a> claim
     * 
     * @return the
     *         <a href= "https://tools.ietf.org/html/rfc7519#section-4.1.6">Issued
     *         At</a> claim if available, otherwise null
     * @throws OAuthBearerIllegalTokenException
     *             if the claim value is the incorrect type
     */
    public Number issuedAt() throws OAuthBearerIllegalTokenException {
        return claim("iat", Number.class);
    }

    /**
     * Return the
     * <a href="https://tools.ietf.org/html/rfc7519#section-4.1.2">Subject</a> claim
     * 
     * @return the <a href=
     *         "https://tools.ietf.org/html/rfc7519#section-4.1.2">Subject</a> claim
     *         if available, otherwise null
     * @throws OAuthBearerIllegalTokenException
     *             if the claim value is the incorrect type
     */
    public String subject() throws OAuthBearerIllegalTokenException {
        return claim("sub", String.class);
    }

    /**
     * Decode the given Base64URL-encoded value, parse the resulting JSON as a JSON
     * object, and return the map of member names to their values (each value being
     * represented as either a String, a Number, or a List of Strings).
     * 
     * @param split
     *            the value to decode and parse
     * @return the map of JSON member names to their String, Number, or String List
     *         value
     * @throws OAuthBearerIllegalTokenException
     *             if the given Base64URL-encoded value cannot be decoded or parsed
     */
    public static Map<String, Object> toMap(String split) throws OAuthBearerIllegalTokenException {
        Map<String, Object> retval = new HashMap<>();
        try {
            byte[] decode = Base64.getDecoder().decode(split);
            JsonNode jsonNode = new ObjectMapper().readTree(decode);
            if (jsonNode == null)
                throw new OAuthBearerIllegalTokenException(OAuthBearerValidationResult.newFailure("malformed JSON"));
            for (Iterator<Entry<String, JsonNode>> iterator = jsonNode.fields(); iterator.hasNext();) {
                Entry<String, JsonNode> entry = iterator.next();
                retval.put(entry.getKey(), convert(entry.getValue()));
            }
            return Collections.unmodifiableMap(retval);
        } catch (IllegalArgumentException e) {
            // potentially thrown by java.util.Base64.Decoder implementations
            throw new OAuthBearerIllegalTokenException(
                    OAuthBearerValidationResult.newFailure("malformed Base64 URL encoded value"));
        } catch (IOException e) {
            throw new OAuthBearerIllegalTokenException(OAuthBearerValidationResult.newFailure("malformed JSON"));
        }
    }

    private List<String> extractCompactSerializationSplits() {
        List<String> tmpSplits = new ArrayList<>(Arrays.asList(compactSerialization.split("\\.")));
        if (compactSerialization.endsWith("."))
            tmpSplits.add("");
        if (tmpSplits.size() != 3)
            throw new OAuthBearerIllegalTokenException(OAuthBearerValidationResult.newFailure(
                    "Unsecured JWS compact serializations must have 3 dot-separated Base64URL-encoded values"));
        return Collections.unmodifiableList(tmpSplits);
    }

    private static Object convert(JsonNode value) {
        if (value.isArray()) {
            List<String> retvalList = new ArrayList<>();
            for (JsonNode arrayElement : value) {
                retvalList.add(arrayElement.asText());
            }
            return retvalList;
        }
        return value.getNodeType() == JsonNodeType.NUMBER ? ((NumericNode) value).numberValue() : value.asText();
    }

    private Long calculateStartTimeMs() throws OAuthBearerIllegalTokenException {
        Number issuedAtSeconds = claim("iat", Number.class);
        return issuedAtSeconds == null ? null : convertClaimTimeInSecondsToMs(issuedAtSeconds);
    }

    private static long convertClaimTimeInSecondsToMs(Number claimValue) {
        return Math.round(claimValue.doubleValue() * 1000);
    }

    private Set<String> calculateScope() {
        String scopeClaimName = scopeClaimName();
        if (isClaimType(scopeClaimName, String.class)) {
            String scopeClaimValue = claim(scopeClaimName, String.class);
            if (scopeClaimValue.trim().isEmpty())
                return Collections.emptySet();
            else {
                Set<String> retval = new HashSet<>();
                retval.add(scopeClaimValue.trim());
                return Collections.unmodifiableSet(retval);
            }
        }
        List<?> scopeClaimValue = claim(scopeClaimName, List.class);
        if (scopeClaimValue == null || scopeClaimValue.isEmpty())
            return Collections.emptySet();
        @SuppressWarnings("unchecked")
        List<String> stringList = (List<String>) scopeClaimValue;
        Set<String> retval = new HashSet<>();
        for (String scope : stringList) {
            if (scope != null && !scope.trim().isEmpty()) {
                retval.add(scope.trim());
            }
        }
        return Collections.unmodifiableSet(retval);
    }
}
