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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple utility class to perform basic cleaning and validation on input values so that they're
 * performed consistently throughout the code base.
 */

public class ClaimValidationUtils {

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
     * @throws ValidateException Thrown if the value is <code>null</code>, contains duplicates, or
     *                           if any of the values in the set are <code>null</code>, empty,
     *                           or whitespace only
     */

    public static Set<String> validateScopes(String scopeClaimName, Collection<String> scopes) throws ValidateException {
        if (scopes == null)
            throw new ValidateException(String.format("%s value must be non-null", scopeClaimName));

        Set<String> copy = new HashSet<>();

        for (String scope : scopes) {
            scope = validateString(scopeClaimName, scope);

            if (copy.contains(scope))
                throw new ValidateException(String.format("%s value must not contain duplicates - %s already present", scopeClaimName, scope));

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
     * @throws ValidateException Thrown if the value is <code>null</code> or negative
     */

    public static long validateExpiration(String claimName, Long claimValue) throws ValidateException {
        if (claimValue == null)
            throw new ValidateException(String.format("%s value must be non-null", claimName));

        if (claimValue < 0)
            throw new ValidateException(String.format("%s value must be non-negative; value given was \"%s\"", claimName, claimValue));

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
     * @throws ValidateException Thrown if the value is <code>null</code>, empty, or whitespace only
     */

    public static String validateSubject(String claimName, String claimValue) throws ValidateException {
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
     * @throws ValidateException Thrown if the value is negative
     */

    public static Long validateIssuedAt(String claimName, Long claimValue) throws ValidateException {
        if (claimValue != null && claimValue < 0)
            throw new ValidateException(String.format("%s value must be null or non-negative; value given was \"%s\"", claimName, claimValue));

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
     * @throws ValidateException Thrown if the value is <code>null</code>, empty, or whitespace only
     */

    public static String validateClaimNameOverride(String name, String value) throws ValidateException {
        return validateString(name, value);
    }

    private static String validateString(String name, String value) throws ValidateException {
        if (value == null)
            throw new ValidateException(String.format("%s value must be non-null", name));

        if (value.isEmpty())
            throw new ValidateException(String.format("%s value must be non-empty", name));

        value = value.trim();

        if (value.isEmpty())
            throw new ValidateException(String.format("%s value must not contain only whitespace", name));

        return value;
    }

}
