/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.auth;

import java.security.Principal;

/**
 * An implementation of `Principal` that contains a `name` and a `principalType`, which is used on the authorization
 * layer by the broker.
 *
 * @see SimplePrincipal
 */
public class KafkaPrincipal implements Principal {
    public static final String SEPARATOR = ":";
    public static final String USER_TYPE = "User";
    public final static KafkaPrincipal ANONYMOUS = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS");

    private final String principalType;
    private final String name;

    public KafkaPrincipal(String principalType, String name) {
        if (principalType == null || name == null) {
            throw new IllegalArgumentException("principalType and name can not be null");
        }
        this.principalType = principalType;
        this.name = name;
    }

    public static KafkaPrincipal fromString(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        String[] split = str.split(SEPARATOR, 2);

        if (split == null || split.length != 2) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        return new KafkaPrincipal(split[0], split[1]);
    }

    /**
     * Returns a KafkaPrincipal with `USER_TYPE` and with the same name as `principal`.
     */
    public static KafkaPrincipal fromPrincipal(Principal principal) {
        return new KafkaPrincipal(USER_TYPE, principal.getName());
    }

    @Override
    public String toString() {
        return principalType + SEPARATOR + name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KafkaPrincipal)) return false;

        KafkaPrincipal that = (KafkaPrincipal) o;

        if (!principalType.equals(that.principalType)) return false;
        return name.equals(that.name);

    }

    @Override
    public int hashCode() {
        int result = principalType.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getPrincipalType() {
        return principalType;
    }
}



