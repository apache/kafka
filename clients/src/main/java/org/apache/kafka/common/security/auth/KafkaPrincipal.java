/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.security.Principal;

public class KafkaPrincipal implements Principal {
    private final String name;

    public KafkaPrincipal(String name) {
        if (name == null)
            throw new IllegalArgumentException("name is null");
        this.name = name;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object)
            return true;

        if (object instanceof KafkaPrincipal) {
            return name.equals(((KafkaPrincipal) object).getName());
        }

        return false;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

}
