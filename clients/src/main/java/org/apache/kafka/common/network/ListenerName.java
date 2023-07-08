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
package org.apache.kafka.common.network;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.util.Locale;
import java.util.Objects;

public final class ListenerName {

    private static final String CONFIG_STATIC_PREFIX = "listener.name";

    /**
     * Create an instance with the security protocol name as the value.
     */
    public static ListenerName forSecurityProtocol(SecurityProtocol securityProtocol) {
        return new ListenerName(securityProtocol.name);
    }

    /**
     * Create an instance with the provided value converted to uppercase.
     */
    public static ListenerName normalised(String value) {
        if (Utils.isBlank(value)) {
            throw new ConfigException("The provided listener name is null or empty string");
        }
        return new ListenerName(value.toUpperCase(Locale.ROOT));
    }

    private final String value;

    public ListenerName(String value) {
        Objects.requireNonNull(value, "value should not be null");
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ListenerName))
            return false;
        ListenerName that = (ListenerName) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "ListenerName(" + value + ")";
    }

    public String configPrefix() {
        return CONFIG_STATIC_PREFIX + "." + value.toLowerCase(Locale.ROOT) + ".";
    }

    public String saslMechanismConfigPrefix(String saslMechanism) {
        return configPrefix() + saslMechanismPrefix(saslMechanism);
    }

    public static String saslMechanismPrefix(String saslMechanism) {
        return saslMechanism.toLowerCase(Locale.ROOT) + ".";
    }
}
