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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class OAuthBearerValidationUtils {

    public static String validateToken(String token) throws ValidateException {
        token = validateString(token, "Token");

        if (token.contains(".."))
            throw new ValidateException("Malformed compact serialization contains '..'");

        return token;
    }

    public static Set<String> validateScopes(Set<String> scopes) {
        if (scopes == null)
            throw new ValidateException("Scopes value must be non-null");

        Set<String> scopesCopy = new HashSet<>();

        for (String scope : scopes) {
            scope = validateString(scope, "Scope");
            scopesCopy.add(scope);
        }

        return Collections.unmodifiableSet(scopesCopy);
    }

    public static String validateString(String s, String name) throws ValidateException {
        if (s == null)
            throw new ValidateException(String.format("%s value must be non-null", name));

        if (s.isEmpty())
            throw new ValidateException(String.format("%s value must be non-empty", s));

        s = s.trim();

        if (s.isEmpty())
            throw new ValidateException(String.format("%s value must not contain only whitespace", s));

        return s;
    }

}
