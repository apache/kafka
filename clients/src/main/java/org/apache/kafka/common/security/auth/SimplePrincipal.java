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
 * Simple implementation of `Principal` that contains a `name` and is used on the authentication layer by clients
 * and broker.
 *
 * @see KafkaPrincipal
 */
public class SimplePrincipal implements Principal {

    public final static Principal ANONYMOUS = new SimplePrincipal("ANONYMOUS");

    private final String name;

    public SimplePrincipal(String name) {
        if (name == null)
            throw new IllegalArgumentException("name can not be null");
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimplePrincipal)) return false;
        SimplePrincipal that = (SimplePrincipal) o;
        return name.equals(that.name);

    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String getName() {
        return name;
    }

}
