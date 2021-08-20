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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class OAuthBearerValidationUtils {

    private static final SortedMap<Integer, String> JWT_SECTIONS;

    static {
        SortedMap<Integer, String> raw = new TreeMap<>();
        raw.put(0, "header");
        raw.put(1, "payload");
        raw.put(2, "signature");
        JWT_SECTIONS = Collections.unmodifiableSortedMap(raw);
    }

    public static String validateAccessToken(String token) throws ValidateException {
        token = validateString(token, "JWT access token");

        if (token.contains(".."))
            throw new ValidateException(String.format("Malformed JWT access token provided: '%s'", token));

        return token;
    }

    public static String[] validateAccessTokenSplits(String accessToken) {
        String[] splits = accessToken.split("\\.");

        if (splits.length != 3)
            throw new ValidateException(String.format("Malformed JWT provided; expected at least three sections (header, payload, and signature), but %s sections provided", splits.length));

        for (Entry<Integer, String> entry : JWT_SECTIONS.entrySet()) {
            int sectionIndex = entry.getKey();
            String sectionName = entry.getValue();
            String split = splits[sectionIndex].trim();

            if (split.trim().isEmpty())
                throw new ValidateException(String.format(
                    "Malformed JWT provided; expected at least three sections (header, payload, and signature), but %s section missing",
                    sectionName));
        }

        return splits;
    }

    public static Set<String> validateScopes(Collection<String> scopes) {
        if (scopes == null)
            throw new ValidateException("Scopes value must be non-null");

        Set<String> scopesCopy = new HashSet<>();

        for (String scope : scopes) {
            scope = validateString(scope, "Scope");

            if (scopesCopy.contains(scope))
                throw new ValidateException(String.format("Scopes must not contain duplicates - %s already present", scope));

            scopesCopy.add(scope);
        }

        return Collections.unmodifiableSet(scopesCopy);
    }

    public static long validateLifetimeMs(Long lifetimeMs) throws ValidateException {
        if (lifetimeMs == null || lifetimeMs < 0)
            throw new ValidateException(String.format("lifetimeMs value must be non-null and non-negative - was %s", lifetimeMs));

        return lifetimeMs;
    }

    public static String validatePrincipalName(String principalName) {
        return validateString(principalName, "principalName");
    }

    public static Long validateStartTimeMs(Long startTimeMs) throws ValidateException {
        if (startTimeMs != null && startTimeMs < 0)
            throw new ValidateException(String.format("startTimeMs value must be null or non-negative - was %s", startTimeMs));

        return startTimeMs;
    }

    public static String validateClaimName(String claimNameValue, String claimName) {
        return validateString(claimNameValue, claimName);
    }

    private static String validateString(String s, String name) throws ValidateException {
        if (s == null)
            throw new ValidateException(String.format("%s value must be non-null", name));

        if (s.isEmpty())
            throw new ValidateException(String.format("%s value must be non-empty", name));

        s = s.trim();

        if (s.isEmpty())
            throw new ValidateException(String.format("%s value must not contain only whitespace", name));

        return s;
    }

}
