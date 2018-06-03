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
package org.apache.kafka.common.security.oauthbearer.internal.unsecured;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

public class OAuthBearerValidationUtils {
    /**
     * Validate the given claim for existence and type. It can be required to exist
     * in the given claims, and if it exists it must be one of the types indicated
     * 
     * @param jwt
     *            the mandatory JWT to which the validation will be applied
     * @param required
     *            true if the claim is required to exist
     * @param claimName
     *            the required claim name identifying the claim to be checked
     * @param allowedTypes
     *            one or more of {@code String.class}, {@code Number.class}, and
     *            {@code List.class} identifying the type(s) that the claim value is
     *            allowed to be if it exists
     * @return the result of the validation
     */
    public static OAuthBearerValidationResult validateClaimForExistenceAndType(OAuthBearerUnsecuredJws jwt,
            boolean required, String claimName, Class<?>... allowedTypes) {
        Object rawClaim = Objects.requireNonNull(jwt).rawClaim(Objects.requireNonNull(claimName));
        if (rawClaim == null)
            return required
                    ? OAuthBearerValidationResult.newFailure(String.format("Required claim missing: %s", claimName))
                    : OAuthBearerValidationResult.newSuccess();
        for (Class<?> allowedType : allowedTypes) {
            if (allowedType != null && allowedType.isAssignableFrom(rawClaim.getClass()))
                return OAuthBearerValidationResult.newSuccess();
        }
        return OAuthBearerValidationResult.newFailure(String.format("The %s claim had the incorrect type: %s",
                claimName, rawClaim.getClass().getSimpleName()));
    }

    /**
     * Validate the 'iat' (Issued At) claim. It can be required to exist in the
     * given claims, and if it exists it must be a (potentially fractional) number
     * of seconds since the epoch defining when the JWT was issued; it is a
     * validation error if the Issued At time is after the time at which the check
     * is being done (plus any allowable clock skew).
     * 
     * @param jwt
     *            the mandatory JWT to which the validation will be applied
     * @param required
     *            true if the claim is required to exist
     * @param whenCheckTimeMs
     *            the time relative to which the validation is to occur
     * @param allowableClockSkewMs
     *            non-negative number to take into account some potential clock skew
     * @return the result of the validation
     * @throws OAuthBearerConfigException
     *             if the given allowable clock skew is negative
     */
    public static OAuthBearerValidationResult validateIssuedAt(OAuthBearerUnsecuredJws jwt, boolean required,
            long whenCheckTimeMs, int allowableClockSkewMs) throws OAuthBearerConfigException {
        Number value;
        try {
            value = Objects.requireNonNull(jwt).issuedAt();
        } catch (OAuthBearerIllegalTokenException e) {
            return e.reason();
        }
        boolean exists = value != null;
        if (!exists)
            return doesNotExistResult(required, "iat");
        double doubleValue = value.doubleValue();
        return 1000 * doubleValue > whenCheckTimeMs + confirmNonNegative(allowableClockSkewMs)
                ? OAuthBearerValidationResult.newFailure(String.format(
                        "The Issued At value (%f seconds) was after the indicated time (%d ms) plus allowable clock skew (%d ms)",
                        doubleValue, whenCheckTimeMs, allowableClockSkewMs))
                : OAuthBearerValidationResult.newSuccess();
    }

    /**
     * Validate the 'exp' (Expiration Time) claim. It must exist and it must be a
     * (potentially fractional) number of seconds defining the point at which the
     * JWT expires. It is a validation error if the time at which the check is being
     * done (minus any allowable clock skew) is on or after the Expiration Time
     * time.
     * 
     * @param jwt
     *            the mandatory JWT to which the validation will be applied
     * @param whenCheckTimeMs
     *            the time relative to which the validation is to occur
     * @param allowableClockSkewMs
     *            non-negative number to take into account some potential clock skew
     * @return the result of the validation
     * @throws OAuthBearerConfigException
     *             if the given allowable clock skew is negative
     */
    public static OAuthBearerValidationResult validateExpirationTime(OAuthBearerUnsecuredJws jwt, long whenCheckTimeMs,
            int allowableClockSkewMs) throws OAuthBearerConfigException {
        Number value;
        try {
            value = Objects.requireNonNull(jwt).expirationTime();
        } catch (OAuthBearerIllegalTokenException e) {
            return e.reason();
        }
        boolean exists = value != null;
        if (!exists)
            return doesNotExistResult(true, "exp");
        double doubleValue = value.doubleValue();
        return whenCheckTimeMs - confirmNonNegative(allowableClockSkewMs) >= 1000 * doubleValue
                ? OAuthBearerValidationResult.newFailure(String.format(
                        "The indicated time (%d ms) minus allowable clock skew (%d ms) was on or after the Expiration Time value (%f seconds)",
                        whenCheckTimeMs, allowableClockSkewMs, doubleValue))
                : OAuthBearerValidationResult.newSuccess();
    }

    /**
     * Validate the 'iat' (Issued At) and 'exp' (Expiration Time) claims for
     * internal consistency. The following must be true if both claims exist:
     * 
     * <pre>
     * exp > iat
     * </pre>
     * 
     * @param jwt
     *            the mandatory JWT to which the validation will be applied
     * @return the result of the validation
     */
    public static OAuthBearerValidationResult validateTimeConsistency(OAuthBearerUnsecuredJws jwt) {
        Number issuedAt;
        Number expirationTime;
        try {
            issuedAt = Objects.requireNonNull(jwt).issuedAt();
            expirationTime = jwt.expirationTime();
        } catch (OAuthBearerIllegalTokenException e) {
            return e.reason();
        }
        if (expirationTime != null && issuedAt != null && expirationTime.doubleValue() <= issuedAt.doubleValue())
            return OAuthBearerValidationResult.newFailure(
                    String.format("The Expiration Time time (%f seconds) was not after the Issued At time (%f seconds)",
                            expirationTime.doubleValue(), issuedAt.doubleValue()));
        return OAuthBearerValidationResult.newSuccess();
    }

    /**
     * Validate the given token's scope against the required scope. Every required
     * scope element (if any) must exist in the provided token's scope for the
     * validation to succeed.
     * 
     * @param token
     *            the required token for which the scope will to validate
     * @param requiredScope
     *            the optional required scope against which the given token's scope
     *            will be validated
     * @return the result of the validation
     */
    public static OAuthBearerValidationResult validateScope(OAuthBearerToken token, List<String> requiredScope) {
        final Set<String> tokenScope = token.scope();
        if (requiredScope == null || requiredScope.isEmpty())
            return OAuthBearerValidationResult.newSuccess();
        for (String requiredScopeElement : requiredScope) {
            if (!tokenScope.contains(requiredScopeElement))
                return OAuthBearerValidationResult.newFailure(String.format(
                        "The provided scope (%s) was mising a required scope (%s).  All required scope elements: %s",
                        String.valueOf(tokenScope), requiredScopeElement, requiredScope.toString()),
                        requiredScope.toString(), null);
        }
        return OAuthBearerValidationResult.newSuccess();
    }

    private static int confirmNonNegative(int allowableClockSkewMs) throws OAuthBearerConfigException {
        if (allowableClockSkewMs < 0)
            throw new OAuthBearerConfigException(
                    String.format("Allowable clock skew must not be negative: %d", allowableClockSkewMs));
        return allowableClockSkewMs;
    }

    private static OAuthBearerValidationResult doesNotExistResult(boolean required, String claimName) {
        return required ? OAuthBearerValidationResult.newFailure(String.format("Required claim missing: %s", claimName))
                : OAuthBearerValidationResult.newSuccess();
    }

    private OAuthBearerValidationUtils() {
        // empty
    }
}
