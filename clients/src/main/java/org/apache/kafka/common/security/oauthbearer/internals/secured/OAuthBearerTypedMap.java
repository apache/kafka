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


import java.util.List;
import java.util.Optional;

/**
 * <code>OAuthBearerTypedMap</code> is a utility class to retrieve key-based values using their given type.
 * This is to unify the manner in which configuration, JAAS options, JWT claims, etc. are handled within the
 * OAuth layer.
 */
public abstract class OAuthBearerTypedMap {

    public abstract String getString(String key);

    public Optional<String> maybeGetString(String key) {
        if (containsKey(key))
            return Optional.of(getString(key));

        return Optional.empty();
    }

    public abstract String getPassword(String key);

    public abstract boolean containsKey(String key);

    public abstract <T> T get(String key);

    public Short getShort(String key) {
        return (Short) get(key);
    }

    public Integer getInt(String key) {
        return (Integer) get(key);
    }

    public Optional<Integer> maybeGetInt(String key) {
        if (containsKey(key))
            return Optional.of(getInt(key));

        return Optional.empty();
    }

    public Number getNumber(String key) {
        return (Number) get(key);
    }

    public Optional<Number> maybeGetNumber(String key) {
        if (containsKey(key))
            return Optional.of(getNumber(key));

        return Optional.empty();
    }

    public Long getLong(String key) {
        return (Long) get(key);
    }

    public Double getDouble(String key) {
        return (Double) get(key);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        return (List<String>) get(key);
    }

    public Boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    public Optional<Boolean> maybeGetBoolean(String key) {
        if (containsKey(key))
            return Optional.of(getBoolean(key));

        return Optional.empty();
    }
}