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
package org.apache.kafka.common.security.auth;

import org.apache.kafka.common.utils.SecurityUtils;

import java.security.Principal;

import static java.util.Objects.requireNonNull;

/**
 * <p>Principals in Kafka are defined by a type and a name. The principal type will always be <code>"User"</code>
 * for the simple authorizer that is enabled by default, but custom authorizers can leverage different
 * principal types (such as to enable group or role-based ACLs). The {@link KafkaPrincipalBuilder} interface
 * is used when you need to derive a different principal type from the authentication context, or when
 * you need to represent relations between different principals. For example, you could extend
 * {@link KafkaPrincipal} in order to link a user principal to one or more role principals.
 *
 * <p>For custom extensions of {@link KafkaPrincipal}, there two key points to keep in mind:
 * <ol>
 * <li>To be compatible with the ACL APIs provided by Kafka (including the command line tool), each ACL
 *    can only represent a permission granted to a single principal (consisting of a principal type and name).
 *    It is possible to use richer ACL semantics, but you must implement your own mechanisms for adding
 *    and removing ACLs.
 * <li>In general, {@link KafkaPrincipal} extensions are only useful when the corresponding Authorizer
 *    is also aware of the extension. If you have a {@link KafkaPrincipalBuilder} which derives user groups
 *    from the authentication context (e.g. from an SSL client certificate), then you need a custom
 *    authorizer which is capable of using the additional group information.
 * </ol>
 */
public class KafkaPrincipal implements Principal {
    public static final String USER_TYPE = "User";
    public final static KafkaPrincipal ANONYMOUS = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS");

    private final String principalType;
    private final String name;
    private volatile boolean tokenAuthenticated;

    public KafkaPrincipal(String principalType, String name) {
        this(principalType, name, false);
    }

    public KafkaPrincipal(String principalType, String name, boolean tokenAuthenticated) {
        this.principalType = requireNonNull(principalType, "Principal type cannot be null");
        this.name = requireNonNull(name, "Principal name cannot be null");
        this.tokenAuthenticated = tokenAuthenticated;
    }

    /**
     * Parse a {@link KafkaPrincipal} instance from a string. This method cannot be used for {@link KafkaPrincipal}
     * extensions.
     *
     * @param str The input string formatted as "{principalType}:{principalName}"
     * @return The parsed {@link KafkaPrincipal} instance
     * @deprecated As of 1.0.0. This method will be removed in a future major release.
     */
    @Deprecated
    public static KafkaPrincipal fromString(String str) {
        return SecurityUtils.parseKafkaPrincipal(str);
    }

    @Override
    public String toString() {
        return principalType + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (getClass() != o.getClass()) return false;

        KafkaPrincipal that = (KafkaPrincipal) o;
        return principalType.equals(that.principalType) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = principalType != null ? principalType.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getPrincipalType() {
        return principalType;
    }

    public void tokenAuthenticated(boolean tokenAuthenticated) {
        this.tokenAuthenticated = tokenAuthenticated;
    }

    public boolean tokenAuthenticated() {
        return tokenAuthenticated;
    }
}

